//! The TLS listener dial-in hosts connect to, for pairing and then for serving runs.

use std::net::Ipv4Addr;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use ovr_proto::frame::{self, Framed};
use ovr_proto::msg::{Accepted, Banner, Failure, Federated, Pair, PairOk};
use ovr_proto::{Frame, FrameKind, PROTOCOL_VERSION};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

use super::{PORT, Server};
use crate::mux::{self, Mux};
use crate::state::PairedHost;
use crate::{Error, pairing, state, tls};

/// Bounds the handshake and first frame, so idle connections cannot pile up.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Accepts dial-in hosts until the server exits, retrying while the port is taken, e.g. by an exiting `ovrd`.
pub async fn listen(server: Arc<Server>) {
    let acceptor = match server.identity.server_config() {
        Ok(config) => TlsAcceptor::from(config),
        Err(err) => {
            tracing::error!(%err, "dial-in disabled: unusable identity");
            return;
        }
    };
    let mut warned = false;
    let listener = loop {
        match TcpListener::bind(("0.0.0.0", PORT)).await {
            Ok(listener) => break listener,
            Err(err) => {
                if !warned {
                    tracing::warn!(%err, "cannot listen on port {PORT}; retrying");
                    warned = true;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };
    tracing::info!("listening for dial-in hosts on port {PORT}");
    // Where the platform allows a more specific address beside the wildcard one (macOS), claim loopback too, so a
    // `devtunnel connect` on this machine cannot take it and loop dial-ins to this server back into the tunnel.
    if let Ok(loopback) = TcpListener::bind((Ipv4Addr::LOCALHOST, PORT)).await {
        tokio::spawn(accept(loopback, Arc::clone(&server), acceptor.clone()));
    }
    accept(listener, server, acceptor).await;
}

async fn accept(listener: TcpListener, server: Arc<Server>, acceptor: TlsAcceptor) {
    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                tracing::warn!(%err, "accept failed");
                continue;
            }
        };
        let (server, acceptor) = (Arc::clone(&server), acceptor.clone());
        tokio::spawn(async move {
            if let Err(err) = handle(&server, &acceptor, tcp).await {
                tracing::info!(%peer, %err, "dial-in connection ended");
            }
        });
    }
}

async fn handle(server: &Server, acceptor: &TlsAcceptor, tcp: TcpStream) -> Result<(), Error> {
    mux::tune(&tcp)?;
    let (mut tls, first) = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
        let mut tls = frame::framed(acceptor.accept(tcp).await?);
        let first = tls.next().await.ok_or(Error::Disconnected)??;
        Ok::<_, Error>((tls, first))
    })
    .await
    .map_err(|_| Error::Invalid("handshake timed out".to_owned()))??;
    let fingerprint = tls::peer_fingerprint(tls.get_ref().get_ref().1.peer_certificates())
        .ok_or_else(|| Error::Invalid("no client certificate".to_owned()))?;
    match first.kind {
        FrameKind::Pair => pair(server, &mut tls, &first, &fingerprint).await,
        FrameKind::Banner => attach(server, tls, &first, &fingerprint).await,
        FrameKind::Federated => federated(server, tls, &first, &fingerprint).await,
        kind => Err(ovr_proto::Error::Unexpected(kind).into()),
    }
}

/// Completes `ovrd connect ADDR CODE` when the host proves it holds a code issued here.
async fn pair<S>(
    server: &Server,
    stream: &mut Framed<S>,
    frame: &Frame,
    host: &str,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request: Pair = frame.parse()?;
    let initiator = &server.identity.fingerprint;
    let proof = pairing::Proof::host(initiator, host, &request.hostname);
    let code = server
        .codes
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .redeem(|code| proof.verify(code, &request.mac));
    let Some(code) = code else {
        let message = "the pairing code is wrong or has expired; run `ovrd pair` again".to_owned();
        stream
            .send(Frame::json(FrameKind::Error, &Failure { message })?)
            .await?;
        return Err(Error::Invalid(format!(
            "failed pairing attempt from {}",
            request.hostname
        )));
    };
    let name = valid_name(&request.hostname)?;
    state::add_host(
        &server.dir,
        PairedHost {
            name: name.clone(),
            fingerprint: host.to_owned(),
        },
    )?;
    let mac = pairing::Proof::initiator(initiator, host).sign(&code);
    stream
        .send(Frame::json(FrameKind::PairOk, &PairOk { mac })?)
        .await?;
    stream.close().await?;
    tracing::info!(name, "paired");
    Ok(())
}

/// Serves a paired host's connection until it drops.
async fn attach<S>(
    server: &Server,
    mut stream: Framed<S>,
    frame: &Frame,
    fingerprint: &str,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let banner: Banner = frame.parse()?;
    let paired = state::hosts(&server.dir)?
        .into_iter()
        .find(|host| host.fingerprint == fingerprint);
    let refusal = match &paired {
        None => Some(
            "this host is not paired with the initiator; pair again with `ovrd pair`".to_owned(),
        ),
        Some(_) if banner.protocol != PROTOCOL_VERSION => Some(format!(
            "ovrd {} on this host is incompatible with the initiator; install the initiator's version",
            banner.version
        )),
        Some(_) => None,
    };
    if let Some(message) = refusal {
        let failure = Failure {
            message: message.clone(),
        };
        stream
            .send(Frame::json(FrameKind::Error, &failure)?)
            .await?;
        return Err(Error::Invalid(message));
    }
    let Some(paired) = paired else { return Ok(()) };
    stream.send(Frame::empty(FrameKind::Accept)).await?;

    let mux = Mux::open_with_heartbeat(frame::resume(stream));
    let id = server
        .dialed
        .insert(&paired.name, fingerprint, mux.clone(), banner.platform);
    tracing::info!(
        name = paired.name,
        version = banner.version,
        "host connected"
    );
    mux.closed().await;
    server.dialed.remove(&paired.name, id);
    tracing::info!(name = paired.name, "host disconnected");
    Ok(())
}

/// Serves a host that is not paired but presents a token a trust rule admits, under a name taken from the workload
/// and the host's label, as in `nikhil-pandey/ovr/build`, with a number added while another host has it.
async fn federated<S>(
    server: &Server,
    mut stream: Framed<S>,
    frame: &Frame,
    fingerprint: &str,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let request: Federated = frame.parse()?;
    let admitted = if request.banner.protocol == PROTOCOL_VERSION {
        server
            .trust
            .admit(
                &request.provider,
                &request.token,
                &server.identity.fingerprint,
            )
            .await
            .and_then(|workload| federated_name(&workload, &request.label))
    } else {
        Err(format!(
            "ovrd {} on this host is incompatible with the initiator; install the initiator's version",
            request.banner.version
        ))
    };
    let base = match admitted {
        Ok(base) => base,
        Err(message) => {
            let failure = Failure {
                message: message.clone(),
            };
            stream
                .send(Frame::json(FrameKind::Error, &failure)?)
                .await?;
            return Err(Error::Invalid(message));
        }
    };
    // One admission at a time, so two hosts cannot both be told the same free name.
    let admitting = server.admitting.lock().await;
    let name = server.dialed.free_name(&base, fingerprint);
    stream
        .send(Frame::json(
            FrameKind::Accept,
            &Accepted { name: name.clone() },
        )?)
        .await?;
    let mux = Mux::open_with_heartbeat(frame::resume(stream));
    let id = server
        .dialed
        .insert(&name, fingerprint, mux.clone(), request.banner.platform);
    drop(admitting);
    tracing::info!(
        name,
        provider = request.provider,
        version = request.banner.version,
        "host connected"
    );
    mux.closed().await;
    server.dialed.remove(&name, id);
    tracing::info!(name, "host disconnected");
    Ok(())
}

/// `WORKLOAD/LABEL`, as in `nikhil-pandey/ovr/build`. Every part must survive [`name_part`], so the name always
/// contains `/`, which no paired host's name or SSH destination does.
fn federated_name(workload: &str, label: &str) -> Result<String, String> {
    let parts: Vec<String> = workload.split('/').chain([label]).map(name_part).collect();
    if parts.len() < 2 || parts.iter().any(String::is_empty) {
        return Err(format!(
            "cannot name a host after {workload:?} and {label:?}; the job needs an ID"
        ));
    }
    Ok(parts.join("/"))
}

/// Keeps the characters a host name may have, so that a workload's names cannot inject anything into one.
fn name_part(text: &str) -> String {
    text.chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        .take(64)
        .collect::<String>()
        .trim_start_matches(['-', '.'])
        .to_owned()
}

/// Host names become `ovr` arguments, so they must look like one.
fn valid_name(name: &str) -> Result<String, Error> {
    let valid = !name.is_empty()
        && !name.starts_with('-')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if valid {
        Ok(name.to_owned())
    } else {
        Err(Error::Invalid(format!("invalid host name {name:?}")))
    }
}
