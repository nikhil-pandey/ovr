//! `ovrd connect` on a dial-in host: pair once, then keep a connection to the initiator open and serve runs over it.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use ovr_proto::frame::{self, Framed, Resumed};
use ovr_proto::lock::Lock;
use ovr_proto::msg::{Accepted, Failure, Federated, Pair, PairOk};
use ovr_proto::{Frame, FrameKind};
use rustls::pki_types::ServerName;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::agent::runs::now;
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

/// `ovrd connect`: pair with the initiator at `addr` using `token`, `FINGERPRINT-CODE` as `ovr pair` prints it, or
/// move to `addr` without one, then keep serving it, in the foreground if asked to or if it cannot be started at
/// login. Without `addr`, serve the paired initiator. A token of the form `PROVIDER:FINGERPRINT` serves as a workload
/// that provider vouches for instead, telling it apart from others of the same workload by `name` if given; see
/// [`federated`].
///
/// # Errors
///
/// Returns an error when the initiator is unreachable, rejects the code, or is not the one expected.
pub async fn connect(
    addr: Option<Addr>,
    token: Option<&str>,
    name: Option<&str>,
    foreground: bool,
) -> Result<(), Error> {
    match (&addr, token) {
        (Some(addr), Some(token)) if token.contains(':') => {
            let federation = token.parse().map_err(Error::Invalid)?;
            return federated(addr.clone(), federation, name, foreground).await;
        }
        _ if name.is_some() => {
            return Err(Error::Invalid(
                "--name names a host serving as a workload, with PROVIDER:FINGERPRINT".to_owned(),
            ));
        }
        _ => {}
    }
    let dir = ovr_proto::paths::data_dir()?;
    // Serving without an address needs a pairing, whoever holds the lock.
    if addr.is_none() {
        paired(&dir)?;
    }
    // Only a login service may already be serving when this pairs or moves, as installing restarts it: anything else
    // holding the lock fails here, before the pairing changes under it.
    let lock = Lock::try_exclusive(&dir.join("connect.lock"))?;
    if lock.is_none()
        && (foreground || addr.is_none() || !matches!(autostart::installed(), Ok(Some(_))))
    {
        return Err(already_serving(&dir)?);
    }
    match (addr, token) {
        (Some(addr), Some(token)) => pair(&dir, &addr, token).await?,
        (Some(addr), None) => move_to(&dir, &addr).await?,
        (None, _) => return serve(&dir, lock).await,
    }
    if foreground {
        return serve(&dir, lock).await;
    }
    // The service takes the lock over.
    drop(lock);
    match autostart::install() {
        Ok(()) => {
            println!(
                "ovrd connect now runs in the background and at every login, serving as {}; log: {}",
                paired(&dir)?.name,
                log_path()?.display()
            );
            Ok(())
        }
        Err(err) => {
            eprintln!("cannot start ovrd connect at login ({err}); serving until interrupted");
            serve(&dir, None).await
        }
    }
}

/// `ovrd disconnect`: stops and removes the login service and forgets the paired initiator. Run keepers, checkouts,
/// and this host's identity stay, and so do runs still running.
///
/// # Errors
///
/// Returns an error when the service manager rejects the removal or the pairing cannot be removed.
pub fn disconnect() -> Result<(), Error> {
    let dir = ovr_proto::paths::data_dir()?;
    let service = autostart::uninstall()?;
    let initiator = state::forget_initiator(&dir)?;
    let mut done = vec![service.map_or_else(
        || "no login service was installed".to_owned(),
        |service| format!("removed {service}"),
    )];
    done.push(initiator.as_ref().map_or_else(
        || "not paired".to_owned(),
        |initiator| {
            format!(
                "forgot initiator {} ({})",
                initiator.addr, initiator.fingerprint
            )
        },
    ));
    println!("{}", done.join("; "));
    if let Some(Initiator { name, .. }) = &initiator {
        println!("the initiator lists this host as {name} until `ovr unpair {name}` runs there");
    }
    // Removing the service stopped its `ovrd connect`, which may take a moment to let go of the lock; one started
    // by hand keeps holding it.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while Lock::try_exclusive(&dir.join("connect.lock"))?.is_none() {
        if std::time::Instant::now() >= deadline {
            println!(
                "an ovrd connect started in a terminal is still serving; stop it to disconnect now"
            );
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Ok(())
}

fn log_path() -> io::Result<PathBuf> {
    Ok(ovr_proto::paths::log_dir()?.join("connect.log"))
}

/// The error for a second `ovrd connect` while one is serving.
fn already_serving(dir: &Path) -> Result<Error, Error> {
    let addr = state::initiator(dir)?.map_or_else(|| "the paired initiator".to_owned(), |i| i.addr);
    Ok(Error::Invalid(format!(
        "ovrd connect is already serving {addr}; ovrd status shows whether it is connected, and connect.log at {} \
         has details",
        log_path()?.display()
    )))
}

/// Serves the initiator at `addr` as a workload `federation`'s provider vouches for, such as a GitHub Actions job,
/// until this process ends; `name` tells it apart from others of that workload, such as a matrix's jobs. Nothing is
/// installed or recorded, and the identity lives only in memory. Unless `foreground`, it starts itself again in the
/// background, logging to a file, and returns at once, so that a CI step never waits for an initiator that is away.
async fn federated(
    addr: Addr,
    federation: Federation,
    name: Option<&str>,
    foreground: bool,
) -> Result<(), Error> {
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
        let mut args: Vec<std::ffi::OsString> = vec![
            "connect".into(),
            addr.to_string().into(),
            federation.to_string().into(),
            "--foreground".into(),
        ];
        if let Some(name) = name {
            args.extend(["--name".into(), name.into()]);
        }
        ovr_proto::ipc::spawn_detached(&std::env::current_exe()?, &args, log)?;
        println!(
            "serving {addr} in the background until this job ends; log: {}",
            log_path.display()
        );
        return Ok(());
    }
    let identity = Identity::ephemeral()?;
    let provider = federation.provider;
    let mut config = agent::Config::detect()?;
    if let Some(workspace) = provider.workspace() {
        config.base = workspace;
    }
    let config = Arc::new(config);
    tracing::info!(%addr, %provider, base = %config.base.display(), "serving");
    let workload = Workload {
        provider,
        label: match name {
            Some(name) => format!("{}/{name}", provider.label()),
            None => provider.label(),
        },
    };
    keep_serving(
        &identity,
        &addr,
        &federation.initiator,
        Some(&workload),
        &config,
    )
    .await
}

/// Pairs with the initiator at `addr` using `token`, sending its code only once the initiator there presents the
/// fingerprint the token starts with.
async fn pair(dir: &Path, addr: &Addr, token: &str) -> Result<(), Error> {
    // Not echoed: it may hold a live pairing code, and this error can end up in a CI log.
    let (initiator, code) = pairing::parse(token).ok_or_else(|| {
        Error::Invalid(
            "that is not what `ovr pair` prints; it looks like FINGERPRINT-7KQX-M4RT".to_owned(),
        )
    })?;
    let previous = state::initiator(dir)?;
    let identity = Identity::load_or_create(dir)?;
    let (_forwarder, mut stream) = dial(
        &identity,
        addr,
        &initiator,
        &format!(
            "{addr} presented a different identity than the one `ovr pair` printed; something may be intercepting \
             the connection, or it is another initiator"
        ),
    )
    .await?;

    let pair = Pair {
        hostname: agent::hostname(),
        code,
    };
    stream.send(Frame::json(FrameKind::Pair, &pair)?).await?;
    let reply = stream.next().await.ok_or(Error::Disconnected)??;
    if reply.kind == FrameKind::Error {
        return Err(Error::Pairing(reply.parse::<Failure>()?.message));
    }
    let PairOk { name } = reply.expect(FrameKind::PairOk)?;
    if let Some(previous) = previous.filter(|previous| previous.fingerprint != initiator) {
        eprintln!(
            "replacing the pairing with {} ({}): a host serves one initiator; that initiator will see {name} \
             offline until it runs ovr unpair {name}",
            previous.addr,
            previous.fingerprint,
            name = previous.name
        );
    }
    state::set_initiator(
        dir,
        &Initiator {
            addr: addr.to_string(),
            fingerprint: initiator,
            name: name.clone(),
        },
    )?;
    println!("paired with {addr}; serving as {name}");
    Ok(())
}

/// Points this host at `addr` once the initiator there proves it is the one this host paired with.
async fn move_to(dir: &Path, addr: &Addr) -> Result<(), Error> {
    let mut initiator = paired(dir)?;
    let identity = Identity::load_or_create(dir)?;
    dial(
        &identity,
        addr,
        &initiator.fingerprint,
        &format!(
            "{addr} is a different initiator than the one this host paired with; pair with it using `ovr pair`"
        ),
    )
    .await?;
    initiator.addr = addr.to_string();
    state::set_initiator(dir, &initiator)?;
    println!("moved to {addr}; serving as {}", initiator.name);
    Ok(())
}

fn paired(dir: &Path) -> Result<Initiator, Error> {
    state::initiator(dir)?.ok_or_else(|| {
        Error::Invalid(
            "not paired; run the `ovrd connect ADDR FINGERPRINT-CODE` command `ovr pair` prints"
                .to_owned(),
        )
    })
}

/// Serves the paired initiator forever, reconnecting with backoff, holding `connect.lock` if given or taking it.
async fn serve(dir: &Path, lock: Option<Lock>) -> Result<(), Error> {
    let _lock = match lock {
        Some(lock) => lock,
        None => match Lock::try_exclusive(&dir.join("connect.lock"))? {
            Some(lock) => lock,
            None => return Err(already_serving(dir)?),
        },
    };
    let initiator = paired(dir)?;
    let addr: Addr = initiator.addr.parse().map_err(Error::Invalid)?;
    let identity = Identity::load_or_create(dir)?;
    let config = Arc::new(agent::Config::detect()?);
    tracing::info!(%addr, "serving");
    keep_serving(&identity, &addr, &initiator.fingerprint, None, &config).await
}

/// Whether `ovrd connect` is connected to the paired initiator, as it records in `connect.json` for `ovrd status`.
/// `since` is in seconds since the Unix epoch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Connection {
    Connected {
        since: u64,
    },
    /// Not connected since serving started or the last connection ended; `error` is why the last attempt failed,
    /// if one has.
    Retrying {
        since: u64,
        error: Option<String>,
    },
}

/// `connect.json` in the state directory, beside `connect.log`.
fn connection_path() -> io::Result<PathBuf> {
    Ok(ovr_proto::paths::log_dir()?.join("connect.json"))
}

/// What the last `ovrd connect` recorded, which is stale unless one still holds `connect.lock`.
///
/// # Errors
///
/// Returns an error when the record exists but cannot be read.
pub fn connection() -> io::Result<Option<Connection>> {
    state::read(&connection_path()?)
}

/// Records [`Connection`] changes while serving the paired initiator, and removes the record when serving ends.
/// Workloads record nothing: several may serve at once, and none holds `connect.lock`.
struct Report(Option<PathBuf>);

impl Report {
    fn write(&self, connection: &Connection) {
        if let Some(path) = &self.0
            && let Err(err) = state::write(path, connection)
        {
            tracing::warn!(%err, path = %path.display(), "cannot record the connection");
        }
    }
}

impl Drop for Report {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// A workload a host serves as, and the label that tells it apart from others of that workload.
struct Workload {
    provider: Provider,
    label: String,
}

/// Serves the initiator at `addr` with fingerprint `pinned` forever, reconnecting with backoff. As a `workload`,
/// each connection presents a fresh token from its provider instead of relying on a pairing.
async fn keep_serving(
    identity: &Identity,
    addr: &Addr,
    pinned: &str,
    workload: Option<&Workload>,
    config: &Arc<agent::Config>,
) -> Result<(), Error> {
    let max_backoff = match addr {
        Addr::Direct(_) => MAX_BACKOFF,
        Addr::Tunnel { .. } => MAX_TUNNEL_BACKOFF,
    };
    let report = Report(match workload {
        Some(_) => None,
        None => Some(connection_path()?),
    });
    let mut backoff = Duration::from_secs(1);
    // Retries come every few seconds while the initiator is away, so only a new reason is logged and recorded.
    let mut reported = None;
    let mut since = now();
    report.write(&Connection::Retrying { since, error: None });
    loop {
        match attach(identity, addr, pinned, workload).await {
            Ok((forwarder, stream, name)) => {
                if let Some(name) = name {
                    tracing::info!(%name, "connected; run `ovr {name} -- CMD` on the initiator");
                } else {
                    tracing::info!("connected");
                }
                report.write(&Connection::Connected { since: now() });
                backoff = Duration::from_secs(1);
                reported = None;
                agent::serve(Incoming::accept_with_heartbeat(stream), Arc::clone(config)).await;
                drop(forwarder);
                tracing::info!("disconnected");
                since = now();
                report.write(&Connection::Retrying { since, error: None });
            }
            Err(err) => {
                let reason = err.to_string();
                if reported.as_ref() == Some(&reason) {
                    tracing::debug!(%reason, "still cannot reach the initiator");
                } else {
                    tracing::info!(%reason, "cannot reach the initiator; retrying until it answers");
                    report.write(&Connection::Retrying {
                        since,
                        error: Some(reason.clone()),
                    });
                    reported = Some(reason);
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }
}

/// Connects to the pinned initiator and announces this host, as `workload` with a token from its provider if given. Returns the
/// forwarder, if any, which must outlive the stream, and the name the initiator gave a federated host.
async fn attach(
    identity: &Identity,
    addr: &Addr,
    pinned: &str,
    workload: Option<&Workload>,
) -> Result<
    (
        Option<Running>,
        Resumed<TlsStream<TcpStream>>,
        Option<String>,
    ),
    Error,
> {
    let mismatch = match workload {
        Some(_) => format!(
            "{addr} presented a different identity than the fingerprint this host was given; check it with \
             `ovr trust` on the initiator"
        ),
        None => format!(
            "{addr} presented a different identity than when paired; pair again if the initiator was reset"
        ),
    };
    let (forwarder, mut stream) = dial(identity, addr, pinned, &mismatch).await?;
    let banner = agent::banner();
    let hello = match workload {
        // A fresh token for every connection: they live for minutes, and this one only goes to the pinned initiator.
        Some(workload) => Frame::json(
            FrameKind::Federated,
            &Federated {
                provider: workload.provider.name().to_owned(),
                token: workload.provider.token(pinned).await.map_err(Error::Host)?,
                label: workload.label.clone(),
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
            let name = match workload {
                Some(_) => Some(reply.parse::<Accepted>()?.name),
                None => None,
            };
            Ok((forwarder, frame::resume(stream), name))
        }
        FrameKind::Error => Err(Error::Pairing(reply.parse::<Failure>()?.message)),
        kind => Err(ovr_proto::Error::Unexpected(kind).into()),
    }
}

/// Opens TLS to `addr`, through a new forwarder for a tunnel, returning the forwarder and the stream once the
/// initiator presents the fingerprint `pinned`, and failing with `mismatch` at once, before anything is sent, when it
/// presents another. A tunnel gets a while to start carrying connections, with a fresh forwarder for every attempt.
async fn dial(
    identity: &Identity,
    addr: &Addr,
    pinned: &str,
    mismatch: &str,
) -> Result<(Option<Running>, Stream), Error> {
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
            Ok((stream, fingerprint)) if fingerprint == pinned => return Ok((forwarder, stream)),
            Ok(_) => return Err(Error::Pairing(mismatch.to_owned())),
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
