//! TLS for dial-in hosts: each `ovrd` has a self-signed Ed25519 identity, and peers are trusted by pinning the
//! SHA-256 fingerprint of that certificate, never by a certificate authority.

use std::io;
use std::path::Path;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
    ClientConfig, DigitallySignedStruct, DistinguishedName, ServerConfig, SignatureScheme,
};

use crate::state;

/// The TLS server name; certificates are pinned, so it only has to be the same on both ends.
pub const SERVER_NAME: &str = "ovr";

/// This machine's long-lived TLS identity.
pub struct Identity {
    cert: CertificateDer<'static>,
    key: PrivatePkcs8KeyDer<'static>,
    pub fingerprint: String,
}

impl Identity {
    /// Loads the identity from `dir`, creating it on first use. An identity that exists but cannot be read is an
    /// error rather than a reason to replace it, which would silently invalidate every pairing.
    ///
    /// # Errors
    ///
    /// Returns an error when the identity cannot be read or written.
    pub fn load_or_create(dir: &Path) -> io::Result<Self> {
        let (cert_path, key_path) = (dir.join("identity.der"), dir.join("identity.key"));
        let (cert, key) = match (std::fs::read(&cert_path), std::fs::read(&key_path)) {
            (Ok(cert), Ok(key)) => (cert, key),
            (Err(cert), Err(key))
                if cert.kind() == io::ErrorKind::NotFound
                    && key.kind() == io::ErrorKind::NotFound =>
            {
                generate(&cert_path, &key_path)?
            }
            (Err(err), _) | (_, Err(err)) => {
                return Err(io::Error::new(
                    err.kind(),
                    format!("cannot load the identity in {}: {err}", dir.display()),
                ));
            }
        };
        Ok(Self {
            fingerprint: fingerprint(&cert),
            cert: cert.into(),
            key: key.into(),
        })
    }

    /// Server side: requires a client certificate, which the caller then checks against the pairing records.
    ///
    /// # Errors
    ///
    /// Returns an error when the key is unusable.
    pub fn server_config(&self) -> io::Result<Arc<ServerConfig>> {
        let provider = provider();
        let verifier = Arc::new(AnyCertificate(provider.signature_verification_algorithms));
        let config = ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(io::Error::other)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![self.cert.clone()], self.key())
            .map_err(io::Error::other)?;
        Ok(Arc::new(config))
    }

    /// Client side: presents this identity and accepts any server certificate, which the caller then pins.
    ///
    /// # Errors
    ///
    /// Returns an error when the key is unusable.
    pub fn client_config(&self) -> io::Result<Arc<ClientConfig>> {
        let provider = provider();
        let verifier = Arc::new(AnyCertificate(provider.signature_verification_algorithms));
        let config = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(io::Error::other)?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_client_auth_cert(vec![self.cert.clone()], self.key())
            .map_err(io::Error::other)?;
        Ok(Arc::new(config))
    }

    fn key(&self) -> PrivateKeyDer<'static> {
        PrivateKeyDer::Pkcs8(self.key.clone_key())
    }
}

fn generate(cert_path: &Path, key_path: &Path) -> io::Result<(Vec<u8>, Vec<u8>)> {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).map_err(io::Error::other)?;
    let cert = rcgen::CertificateParams::new(vec![SERVER_NAME.to_owned()])
        .and_then(|params| params.self_signed(&key))
        .map_err(io::Error::other)?;
    let (cert, key) = (cert.der().to_vec(), key.serialize_der());
    state::write_private(key_path, &key)?;
    state::write_private(cert_path, &cert)?;
    Ok((cert, key))
}

/// The fingerprint of the certificate the peer presented in a completed handshake.
#[must_use]
pub fn peer_fingerprint(certificates: Option<&[CertificateDer<'_>]>) -> Option<String> {
    certificates?.first().map(|cert| fingerprint(cert))
}

fn fingerprint(cert: &[u8]) -> String {
    hex(ring::digest::digest(&ring::digest::SHA256, cert).as_ref())
}

#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Accepts any certificate but still requires the peer to prove it holds the key: authentication happens by
/// fingerprint once the handshake completes.
#[derive(Debug)]
struct AnyCertificate(WebPkiSupportedAlgorithms);

impl AnyCertificate {
    fn verify(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0)
    }
}

impl ServerCertVerifier for AnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOffered,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.verify(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}

impl ClientCertVerifier for AnyCertificate {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOffered,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.verify(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}
