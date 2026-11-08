//! Just enough of JSON Web Tokens to check an identity provider's RS256-signed tokens against its published keys.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Deserialize;
use serde::de::DeserializeOwned;

/// How far clocks may disagree when checking a token's validity window.
const LEEWAY_SECS: u64 = 60;

/// A token split into its parts, its signature not yet checked.
pub struct Unverified<'a> {
    pub header: Header,
    signed: &'a [u8],
    payload: Vec<u8>,
    signature: Vec<u8>,
}

#[derive(Deserialize)]
pub struct Header {
    pub alg: String,
    pub kid: Option<String>,
}

/// A provider's signing keys, as served at its `jwks_uri`.
#[derive(Default, Deserialize)]
pub struct Jwks {
    pub keys: Vec<Jwk>,
}

#[derive(Deserialize)]
pub struct Jwk {
    pub kid: Option<String>,
    pub kty: String,
    pub n: Option<String>,
    pub e: Option<String>,
}

/// The registered claims every accepted token must satisfy.
#[derive(Deserialize)]
struct Registered {
    iss: String,
    aud: Audience,
    exp: u64,
    nbf: Option<u64>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Audience {
    One(String),
    Many(Vec<String>),
}

impl Audience {
    fn contains(&self, audience: &str) -> bool {
        match self {
            Self::One(one) => one == audience,
            Self::Many(many) => many.iter().any(|one| one == audience),
        }
    }
}

/// Splits `token` into header, payload, and signature.
///
/// # Errors
///
/// Returns an error when `token` is not a well-formed JWS in compact form.
pub fn decode(token: &str) -> Result<Unverified<'_>, String> {
    let malformed = || "the token is malformed".to_owned();
    let mut parts = token.split('.');
    let (Some(header), Some(payload), Some(signature), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(malformed());
    };
    let base64 = |part: &str| URL_SAFE_NO_PAD.decode(part).map_err(|_| malformed());
    Ok(Unverified {
        header: serde_json::from_slice(&base64(header)?).map_err(|_| malformed())?,
        signed: &token.as_bytes()[..header.len() + 1 + payload.len()],
        payload: base64(payload)?,
        signature: base64(signature)?,
    })
}

impl Unverified<'_> {
    /// Checks the RS256 signature with `key`, then the issuer, audience, and validity window at `now` (seconds since
    /// the Unix epoch), and returns the claims.
    ///
    /// # Errors
    ///
    /// Returns an error naming the first check that failed.
    pub fn verify<C: DeserializeOwned>(
        &self,
        key: &Jwk,
        issuer: &str,
        audience: &str,
        now: u64,
    ) -> Result<C, String> {
        if self.header.alg != "RS256" {
            return Err(format!(
                "the token is signed with {}, not RS256",
                self.header.alg
            ));
        }
        let (Some(n), Some(e)) = (&key.n, &key.e) else {
            return Err("the signing key is not an RSA key".to_owned());
        };
        if key.kty != "RSA" {
            return Err("the signing key is not an RSA key".to_owned());
        }
        let decode = |part: &str| {
            URL_SAFE_NO_PAD
                .decode(part)
                .map_err(|_| "the signing key is malformed".to_owned())
        };
        ring::signature::RsaPublicKeyComponents {
            n: decode(n)?,
            e: decode(e)?,
        }
        .verify(
            &ring::signature::RSA_PKCS1_2048_8192_SHA256,
            self.signed,
            &self.signature,
        )
        .map_err(|_| "the token's signature is invalid".to_owned())?;

        let registered: Registered = serde_json::from_slice(&self.payload)
            .map_err(|err| format!("the token lacks a registered claim: {err}"))?;
        if registered.iss != issuer {
            return Err(format!("the token was issued by {}", registered.iss));
        }
        if !registered.aud.contains(audience) {
            return Err(
                "the token was issued for a different initiator; check the fingerprint the host was given"
                    .to_owned(),
            );
        }
        if registered.exp + LEEWAY_SECS < now {
            return Err("the token has expired".to_owned());
        }
        if registered.nbf.is_some_and(|nbf| nbf > now + LEEWAY_SECS) {
            return Err("the token is not valid yet; check this machine's clock".to_owned());
        }
        serde_json::from_slice(&self.payload)
            .map_err(|err| format!("the token lacks a claim: {err}"))
    }
}
