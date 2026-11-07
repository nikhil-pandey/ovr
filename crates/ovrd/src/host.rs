//! `ovrd connect` on a dial-in host: pair once, then keep a connection to the laptop open and serve runs over it.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use ovr_proto::frame::{self, Framed, Resumed};
use ovr_proto::msg::{Failure, Pair, PairOk};
use ovr_proto::{Frame, FrameKind};
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::server::PORT;
use crate::state::{self, Laptop};
use crate::tls::{self, Identity};
use crate::{Error, agent, autostart, mux, pairing};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_BACKOFF: Duration = Duration::from_secs(10);

/// Pairs with the laptop at `addr` using `code`, then serves it from now on.
///
/// # Errors
///
/// Returns an error when the laptop is unreachable or rejects the code.
pub async fn pair(addr: &str, code: &str) -> Result<(), Error> {
    let code = pairing::normalize(code).ok_or_else(|| {
        Error::Invalid(format!(
            "{code:?} is not a pairing code; it looks like 7KQX-M4RT"
        ))
    })?;
    let addr = if addr.contains(':') {
        addr.to_owned()
    } else {
        format!("{addr}:{PORT}")
    };
    let dir = ovr_proto::paths::data_dir()?;
    let identity = Identity::load_or_create(&dir)?;
    let (mut stream, laptop) = dial(&identity, &addr).await?;

    let hostname = agent::hostname();
    let mac = pairing::Proof::host(&laptop, &identity.fingerprint, &hostname).sign(&code);
    let pair = Pair {
        hostname: hostname.clone(),
        mac,
    };
    stream.send(Frame::json(FrameKind::Pair, &pair)?).await?;
    let reply = stream.next().await.ok_or(Error::Disconnected)??;
    if reply.kind == FrameKind::Error {
        return Err(Error::Pairing(reply.parse::<Failure>()?.message));
    }
    let PairOk { mac } = reply.expect(FrameKind::PairOk)?;
    if !pairing::Proof::laptop(&laptop, &identity.fingerprint).verify(&code, &mac) {
        return Err(Error::Pairing(format!(
            "{addr} did not prove it issued the code; something may be intercepting the connection"
        )));
    }
    state::set_laptop(
        &dir,
        &Laptop {
            addr: addr.clone(),
            fingerprint: laptop,
        },
    )?;
    println!("paired with {addr}; serving as {hostname}");

    match autostart::install() {
        Ok(()) => {
            println!("ovrd connect now runs in the background and at every login");
            Ok(())
        }
        Err(err) => {
            eprintln!("cannot start ovrd connect at login ({err}); serving until interrupted");
            serve().await
        }
    }
}

/// Serves the paired laptop forever, reconnecting with backoff.
///
/// # Errors
///
/// Returns an error when this host is not paired or another `ovrd connect` is already serving.
pub async fn serve() -> Result<(), Error> {
    let dir = ovr_proto::paths::data_dir()?;
    let lock = std::fs::File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join("connect.lock"))?;
    if lock.try_lock().is_err() {
        return Err(Error::Invalid("ovrd connect is already running".to_owned()));
    }
    let laptop = state::laptop(&dir)?.ok_or_else(|| {
        Error::Invalid(
            "not paired; run the `ovrd connect ADDR CODE` command `ovrd pair` prints".to_owned(),
        )
    })?;
    let identity = Identity::load_or_create(&dir)?;
    let config = Arc::new(agent::Config::detect()?);
    tracing::info!(addr = laptop.addr, "serving");

    let mut backoff = Duration::from_secs(1);
    loop {
        match attach(&identity, &laptop).await {
            Ok(stream) => {
                tracing::info!("connected");
                backoff = Duration::from_secs(1);
                agent::serve(stream, Arc::clone(&config)).await;
                tracing::info!("disconnected");
            }
            Err(err) => {
                tracing::info!(%err, "cannot reach the laptop; retrying in {}s", backoff.as_secs());
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

/// Connects to the pinned laptop and announces this host.
async fn attach(
    identity: &Identity,
    laptop: &Laptop,
) -> Result<Resumed<TlsStream<TcpStream>>, Error> {
    let (mut stream, fingerprint) = dial(identity, &laptop.addr).await?;
    if fingerprint != laptop.fingerprint {
        return Err(Error::Pairing(format!(
            "{} presented a different identity than when paired; pair again if the laptop was reset",
            laptop.addr
        )));
    }
    stream
        .send(Frame::json(FrameKind::Banner, &agent::banner())?)
        .await?;
    let reply = tokio::time::timeout(CONNECT_TIMEOUT, stream.next())
        .await
        .map_err(|_| Error::Disconnected)?
        .ok_or(Error::Disconnected)??;
    match reply.kind {
        FrameKind::Accept => Ok(frame::resume(stream)),
        FrameKind::Error => Err(Error::Pairing(reply.parse::<Failure>()?.message)),
        kind => Err(ovr_proto::Error::Unexpected(kind).into()),
    }
}

/// Opens TLS to `addr`, returning the stream and the laptop's fingerprint for the caller to check.
async fn dial(
    identity: &Identity,
    addr: &str,
) -> Result<(Framed<TlsStream<TcpStream>>, String), Error> {
    let connector = TlsConnector::from(identity.client_config()?);
    let name =
        ServerName::try_from(tls::SERVER_NAME).map_err(|err| Error::Invalid(err.to_string()))?;
    let stream = tokio::time::timeout(CONNECT_TIMEOUT, async {
        let tcp = TcpStream::connect(addr).await?;
        mux::tune(&tcp)?;
        connector.connect(name, tcp).await
    })
    .await
    .map_err(|_| Error::Host(format!("timed out connecting to {addr}")))?
    .map_err(|err| Error::Host(format!("cannot connect to {addr}: {err}")))?;
    let fingerprint = tls::peer_fingerprint(stream.get_ref().1.peer_certificates())
        .ok_or_else(|| Error::Invalid("the laptop presented no certificate".to_owned()))?;
    Ok((frame::framed(stream), fingerprint))
}
