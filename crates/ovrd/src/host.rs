//! `ovrd connect` on a dial-in host: pair once, then keep a connection to the initiator open and serve runs over it.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use ovr_proto::frame::{self, Framed, Resumed};
use ovr_proto::lock::Lock;
use ovr_proto::msg::{Accepted, Failure, Federated, Pair, PairOk};
use ovr_proto::{Frame, FrameKind};
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::mux::Incoming;
use crate::state::{self, Initiator};
use crate::tls::{self, Identity};
use crate::trust::{Federation, Provider};
use crate::tunnel::{self, Addr, Running};
use crate::{Error, agent, autostart, mux, pairing};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Longest wait after a failed attempt to reach the initiator directly; not a recovery deadline.
const MAX_BACKOFF: Duration = Duration::from_secs(2);
/// Longest wait between attempts through a tunnel, each of which restarts the provider's local forwarder.
const MAX_TUNNEL_BACKOFF: Duration = Duration::from_secs(10);

/// How long a tunnel may take to carry connections once its forwarder is up: a new quick tunnel's hostname, for
/// one, can take a minute or more to resolve at the provider's edge.
const TUNNEL_WAIT: Duration = Duration::from_mins(3);
const TUNNEL_RETRY: Duration = Duration::from_secs(2);

type Stream = Framed<TlsStream<TcpStream>>;

/// `ovrd connect`: pair with the initiator at `addr` using `code`, or move to `addr` without one, then keep serving
/// it, in the foreground if asked to or if it cannot be started at login. Without `addr`, serve the paired initiator.
/// A code of the form `PROVIDER:FINGERPRINT` serves as a workload that provider vouches for instead; see
/// [`federated`].
///
/// # Errors
///
/// Returns an error when the initiator is unreachable, rejects the code, or is not the one this host paired with.
pub async fn connect(
    addr: Option<Addr>,
    code: Option<&str>,
    foreground: bool,
) -> Result<(), Error> {
    match (addr, code) {
        (Some(addr), Some(code)) if code.contains(':') => {
            let federation = code.parse().map_err(Error::Invalid)?;
            return federated(addr, federation, foreground).await;
        }
        (Some(addr), Some(code)) => pair(&addr, code).await?,
        (Some(addr), None) => move_to(&addr).await?,
        (None, _) => return serve().await,
    }
    if foreground {
        return serve().await;
    }
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

/// Serves the initiator at `addr` as a workload `federation`'s provider vouches for, such as a GitHub Actions job,
/// until this process ends. Nothing is installed or recorded, and the identity lives only in memory. Unless
/// `foreground`, it starts itself again in the background, logging to a file, and returns at once, so that a CI
/// step never waits for an initiator that is away.
async fn federated(addr: Addr, federation: Federation, foreground: bool) -> Result<(), Error> {
    federation
        .provider
        .check_environment()
        .map_err(Error::Invalid)?;
    if !foreground {
        let log_path =
            ovr_proto::paths::log_dir()?.join(format!("connect-{}.log", federation.provider));
        let log = std::fs::File::options()
            .create(true)
            .append(true)
            .open(&log_path)?;
        let args = [
            "connect".into(),
            addr.to_string().into(),
            federation.to_string().into(),
            "--foreground".into(),
        ];
        ovr_proto::ipc::spawn_detached(&std::env::current_exe()?, &args, log)?;
        println!(
            "serving {addr} in the background until this job ends; log: {}",
            log_path.display()
        );
        return Ok(());
    }
    let identity = Identity::ephemeral()?;
    let config = Arc::new(agent::Config::detect()?);
    tracing::info!(%addr, provider = %federation.provider, "serving");
    keep_serving(
        &identity,
        &addr,
        &federation.initiator,
        Some(federation.provider),
        &config,
    )
    .await
}

/// Pairs with the initiator at `addr` using `code`.
async fn pair(addr: &Addr, code: &str) -> Result<(), Error> {
    let code = pairing::normalize(code).ok_or_else(|| {
        Error::Invalid(format!(
            "{code:?} is not a pairing code; it looks like 7KQX-M4RT"
        ))
    })?;
    let dir = ovr_proto::paths::data_dir()?;
    let identity = Identity::load_or_create(&dir)?;
    let (_forwarder, mut stream, initiator) = dial(&identity, addr).await?;

    let hostname = agent::hostname();
    let mac = pairing::Proof::host(&initiator, &identity.fingerprint, &hostname).sign(&code);
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
    if !pairing::Proof::initiator(&initiator, &identity.fingerprint).verify(&code, &mac) {
        return Err(Error::Pairing(format!(
            "{addr} did not prove it issued the code; something may be intercepting the connection"
        )));
    }
    state::set_initiator(
        &dir,
        &Initiator {
            addr: addr.to_string(),
            fingerprint: initiator,
        },
    )?;
    println!("paired with {addr}; serving as {hostname}");
    Ok(())
}

/// Points this host at `addr` once the initiator there proves it is the one this host paired with.
async fn move_to(addr: &Addr) -> Result<(), Error> {
    let dir = ovr_proto::paths::data_dir()?;
    let mut initiator = paired(&dir)?;
    let identity = Identity::load_or_create(&dir)?;
    let (_forwarder, _stream, fingerprint) = dial(&identity, addr).await?;
    if fingerprint != initiator.fingerprint {
        return Err(Error::Pairing(format!(
            "{addr} is a different initiator than the one this host paired with; pair with it using a code"
        )));
    }
    initiator.addr = addr.to_string();
    state::set_initiator(&dir, &initiator)?;
    println!("moved to {addr}; serving as {}", agent::hostname());
    Ok(())
}

fn paired(dir: &std::path::Path) -> Result<Initiator, Error> {
    state::initiator(dir)?.ok_or_else(|| {
        Error::Invalid(
            "not paired; run the `ovrd connect ADDR CODE` command `ovrd pair` prints".to_owned(),
        )
    })
}

/// Serves the paired initiator forever, reconnecting with backoff.
///
/// # Errors
///
/// Returns an error when this host is not paired or another `ovrd connect` is already serving.
pub async fn serve() -> Result<(), Error> {
    let dir = ovr_proto::paths::data_dir()?;
    let Some(_lock) = Lock::try_exclusive(&dir.join("connect.lock"))? else {
        return Err(Error::Invalid("ovrd connect is already running".to_owned()));
    };
    let initiator = paired(&dir)?;
    let addr: Addr = initiator.addr.parse().map_err(Error::Invalid)?;
    let identity = Identity::load_or_create(&dir)?;
    let config = Arc::new(agent::Config::detect()?);
    tracing::info!(%addr, "serving");
    keep_serving(&identity, &addr, &initiator.fingerprint, None, &config).await
}

/// Serves the initiator at `addr` with fingerprint `pinned` forever, reconnecting with backoff. With `provider`,
/// each connection presents a fresh token from it instead of relying on a pairing.
async fn keep_serving(
    identity: &Identity,
    addr: &Addr,
    pinned: &str,
    provider: Option<Provider>,
    config: &Arc<agent::Config>,
) -> Result<(), Error> {
    let max_backoff = match addr {
        Addr::Direct(_) => MAX_BACKOFF,
        Addr::Tunnel { .. } => MAX_TUNNEL_BACKOFF,
    };
    let mut backoff = Duration::from_secs(1);
    // Retries come every few seconds while the initiator is away, so only a new reason is logged.
    let mut reported = None;
    loop {
        match attach(identity, addr, pinned, provider).await {
            Ok((forwarder, stream, name)) => {
                if let Some(name) = name {
                    tracing::info!(%name, "connected; run `ovr {name} -- CMD` on the initiator");
                } else {
                    tracing::info!("connected");
                }
                backoff = Duration::from_secs(1);
                reported = None;
                agent::serve(Incoming::accept_with_heartbeat(stream), Arc::clone(config)).await;
                drop(forwarder);
                tracing::info!("disconnected");
            }
            Err(err) => {
                let reason = err.to_string();
                if reported.as_ref() == Some(&reason) {
                    tracing::debug!(%reason, "still cannot reach the initiator");
                } else {
                    tracing::info!(%reason, "cannot reach the initiator; retrying until it answers");
                    reported = Some(reason);
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }
}

/// Connects to the pinned initiator and announces this host, with a token from `provider` if given. Returns the
/// forwarder, if any, which must outlive the stream, and the name the initiator gave a federated host.
async fn attach(
    identity: &Identity,
    addr: &Addr,
    pinned: &str,
    provider: Option<Provider>,
) -> Result<
    (
        Option<Running>,
        Resumed<TlsStream<TcpStream>>,
        Option<String>,
    ),
    Error,
> {
    let (forwarder, mut stream, fingerprint) = dial(identity, addr).await?;
    if fingerprint != pinned {
        return Err(Error::Pairing(match provider {
            Some(_) => format!(
                "{addr} presented a different identity than the fingerprint this host was given; check it with \
                 `ovrd trust` on the initiator"
            ),
            None => format!(
                "{addr} presented a different identity than when paired; pair again if the initiator was reset"
            ),
        }));
    }
    let banner = agent::banner();
    let hello = match provider {
        // A fresh token for every connection: they live for minutes, and this one only goes to the pinned initiator.
        Some(provider) => Frame::json(
            FrameKind::Federated,
            &Federated {
                provider: provider.name().to_owned(),
                token: provider.token(pinned).await.map_err(Error::Host)?,
                label: provider.label(),
                banner,
            },
        )?,
        None => Frame::json(FrameKind::Banner, &banner)?,
    };
    stream.send(hello).await?;
    let reply = tokio::time::timeout(CONNECT_TIMEOUT, stream.next())
        .await
        .map_err(|_| Error::Disconnected)?
        .ok_or(Error::Disconnected)??;
    match reply.kind {
        FrameKind::Accept => {
            let name = match provider {
                Some(_) => Some(reply.parse::<Accepted>()?.name),
                None => None,
            };
            Ok((forwarder, frame::resume(stream), name))
        }
        FrameKind::Error => Err(Error::Pairing(reply.parse::<Failure>()?.message)),
        kind => Err(ovr_proto::Error::Unexpected(kind).into()),
    }
}

/// Opens TLS to `addr`, through a new forwarder for a tunnel, returning the forwarder, the stream, and the
/// initiator's fingerprint for the caller to check. A tunnel gets a while to start carrying connections, with a
/// fresh forwarder for every attempt.
async fn dial(
    identity: &Identity,
    addr: &Addr,
) -> Result<(Option<Running>, Stream, String), Error> {
    let deadline = Instant::now()
        + match addr {
            Addr::Direct(_) => Duration::ZERO,
            Addr::Tunnel { .. } => TUNNEL_WAIT,
        };
    loop {
        let (forwarder, target) = match addr {
            Addr::Direct(target) => (None, target.clone()),
            Addr::Tunnel { provider, name } => {
                let (forwarder, local) = tunnel::forward(*provider, name).await?;
                (Some(forwarder), local.to_string())
            }
        };
        match handshake(identity, &target, addr).await {
            Ok((stream, fingerprint)) => return Ok((forwarder, stream, fingerprint)),
            Err(err) if Instant::now() < deadline => {
                drop(forwarder);
                tracing::debug!(%err, "the tunnel is not carrying connections yet");
                tokio::time::sleep(TUNNEL_RETRY).await;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn handshake(
    identity: &Identity,
    target: &str,
    addr: &Addr,
) -> Result<(Stream, String), Error> {
    let connector = TlsConnector::from(identity.client_config()?);
    let name =
        ServerName::try_from(tls::SERVER_NAME).map_err(|err| Error::Invalid(err.to_string()))?;
    let stream = tokio::time::timeout(CONNECT_TIMEOUT, async {
        let tcp = TcpStream::connect(target).await?;
        mux::tune(&tcp)?;
        connector.connect(name, tcp).await
    })
    .await
    .map_err(|_| Error::Host(format!("timed out connecting to {addr}")))?
    .map_err(|err| Error::Host(format!("cannot connect to {addr}: {err}")))?;
    let fingerprint = tls::peer_fingerprint(stream.get_ref().1.peer_certificates())
        .ok_or_else(|| Error::Invalid("the initiator presented no certificate".to_owned()))?;
    Ok((frame::framed(stream), fingerprint))
}
