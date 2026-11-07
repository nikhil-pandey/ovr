//! `ovrd` on the laptop: answers `ovr` over the local socket and owns the connections to hosts.

mod dialin;
mod ssh;

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use ovr_proto::msg::{HostStatus, Op, Platform, Request, Response};
use ovr_proto::{Frame, FrameKind, VERSION, frame, ipc};
use tokio::sync::Notify;
use tokio::task::JoinSet;

use crate::mux::{Channel, Mux};
use crate::pairing::Codes;
use crate::tls::Identity;
use crate::{Error, state};

/// TCP port dial-in hosts connect to.
pub const PORT: u16 = 7420;

/// How long a run waits for a paired host that is offline, e.g. reconnecting after the laptop woke up.
const ONLINE_WAIT: Duration = Duration::from_secs(15);

/// How long a second `ovrd` waits for the first to exit before giving up.
const LOCK_WAIT: Duration = Duration::from_secs(2);

/// Serves until a client of a different version asks it to make way, then finishes in-flight runs and exits.
///
/// # Errors
///
/// Returns an error when another `ovrd` holds the lock or the socket cannot be bound.
pub async fn run() -> Result<(), Error> {
    let dir = ovr_proto::paths::data_dir()?;
    let lock = acquire(dir.join("ovrd.lock")).await?;
    let mut listener = ipc::Listener::bind()?;
    let server = Arc::new(Server {
        identity: Identity::load_or_create(&dir)?,
        dir,
        dialed: Registry::default(),
        ssh: ssh::Pool::default(),
        codes: Mutex::default(),
    });
    tracing::info!(version = VERSION, "serving");
    tokio::spawn(dialin::listen(Arc::clone(&server)));

    let restart = Arc::new(Notify::new());
    let mut runs = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let stream = accepted?;
                runs.spawn(handle(Arc::clone(&server), stream, Arc::clone(&restart)));
            }
            Some(_) = runs.join_next(), if !runs.is_empty() => {}
            () = restart.notified() => break,
        }
    }
    tracing::info!("a different version was started; exiting after in-flight runs");
    drop(listener);
    drop(lock);
    while runs.join_next().await.is_some() {}
    Ok(())
}

async fn acquire(path: PathBuf) -> Result<std::fs::File, Error> {
    let file = std::fs::File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)?;
    let deadline = Instant::now() + LOCK_WAIT;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::WouldBlock) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(Error::Invalid("another ovrd is already running".to_owned()));
            }
            Err(std::fs::TryLockError::Error(err)) => return Err(err.into()),
        }
    }
}

pub(crate) struct Server {
    identity: Identity,
    dir: PathBuf,
    dialed: Registry,
    ssh: ssh::Pool,
    codes: Mutex<Codes>,
}

async fn handle(server: Arc<Server>, stream: ipc::ServerStream, restart: Arc<Notify>) {
    let mut stream = frame::framed(stream);
    let request = match stream.next().await {
        Some(Ok(frame)) => frame.expect::<Request>(FrameKind::Request),
        None => return,
        Some(Err(err)) => Err(err),
    };
    let request = match request {
        Ok(request) => request,
        Err(err) => {
            tracing::warn!(%err, "bad request");
            return;
        }
    };
    if request.version != VERSION {
        let _ = respond(&mut stream, &Response::Restart).await;
        restart.notify_one();
        return;
    }
    let response = match request.op {
        Op::Open { host } => match server.open(&host).await {
            Ok((mut channel, lease)) => {
                if respond(&mut stream, &Response::Opened).await.is_ok() {
                    // From here on the socket carries the agent session verbatim.
                    let mut stream = frame::resume(stream);
                    let _ = tokio::io::copy_bidirectional(&mut stream, &mut channel).await;
                }
                drop(lease);
                return;
            }
            Err(err) => {
                tracing::warn!(host, %err, "cannot open a run");
                Response::Failed {
                    message: err.to_string(),
                }
            }
        },
        Op::Hosts => server.hosts().unwrap_or_else(|err| Response::Failed {
            message: err.to_string(),
        }),
        Op::Pair => server.pair().unwrap_or_else(|err| Response::Failed {
            message: err.to_string(),
        }),
    };
    if respond(&mut stream, &response).await.is_ok() {
        let _ = stream.close().await;
    }
}

async fn respond(
    stream: &mut frame::Framed<ipc::ServerStream>,
    response: &Response,
) -> Result<(), ovr_proto::Error> {
    stream
        .send(Frame::json(FrameKind::Response, response)?)
        .await
}

impl Server {
    /// Opens a channel to `host`: a paired host by name, otherwise an SSH destination.
    async fn open(&self, host: &str) -> Result<(Channel, Option<ssh::Lease>), Error> {
        let paired = state::hosts(&self.dir)?
            .into_iter()
            .any(|paired| paired.name == host);
        if paired {
            let mux = self
                .dialed
                .wait_for(host, ONLINE_WAIT)
                .await
                .ok_or_else(|| {
                    Error::Host(format!(
                        "{host} is paired but has not connected; is `ovrd connect` running there?"
                    ))
                })?;
            return Ok((mux.open().await?, None));
        }
        let (channel, lease) = self.ssh.open(host).await?;
        Ok((channel, Some(lease)))
    }

    fn hosts(&self) -> io::Result<Response> {
        let hosts = state::hosts(&self.dir)?
            .into_iter()
            .map(|host| HostStatus {
                platform: self.dialed.platform(&host.name),
                name: host.name,
            })
            .collect();
        Ok(Response::Hosts { hosts })
    }

    fn pair(&self) -> Result<Response, Error> {
        let code = self
            .codes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .issue()
            .map_err(|_| Error::Invalid("the system random number generator failed".to_owned()))?;
        Ok(Response::Pair {
            addr: format!("{}:{PORT}", local_ip()),
            code,
        })
    }
}

/// The address hosts should dial: the one this machine routes from, which is its Tailscale address when the
/// tailnet is up. No packets are sent.
fn local_ip() -> std::net::IpAddr {
    ["100.100.100.100:53", "8.8.8.8:53"]
        .into_iter()
        .find_map(|target| {
            let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
            socket.connect(target).ok()?;
            socket.local_addr().ok().map(|addr| addr.ip())
        })
        .unwrap_or(std::net::Ipv4Addr::LOCALHOST.into())
}

/// Dialed-in hosts that are currently connected, by name.
#[derive(Default)]
struct Registry {
    hosts: Mutex<HashMap<String, Dialed>>,
    changed: Notify,
    next_id: AtomicU64,
}

struct Dialed {
    id: u64,
    mux: Mux,
    platform: Platform,
}

impl Registry {
    /// Registers a connection, replacing any earlier one from the same host. Returns an id for [`Registry::remove`].
    fn insert(&self, name: &str, mux: Mux, platform: Platform) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let previous = self
            .hosts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name.to_owned(), Dialed { id, mux, platform });
        if let Some(previous) = previous {
            previous.mux.close();
        }
        self.changed.notify_waiters();
        id
    }

    fn remove(&self, name: &str, id: u64) {
        let mut hosts = self.hosts.lock().unwrap_or_else(PoisonError::into_inner);
        if hosts.get(name).is_some_and(|dialed| dialed.id == id) {
            hosts.remove(name);
        }
    }

    fn get(&self, name: &str) -> Option<Mux> {
        let hosts = self.hosts.lock().unwrap_or_else(PoisonError::into_inner);
        hosts
            .get(name)
            .map(|dialed| dialed.mux.clone())
            .filter(|mux| !mux.is_closed())
    }

    fn platform(&self, name: &str) -> Option<Platform> {
        let hosts = self.hosts.lock().unwrap_or_else(PoisonError::into_inner);
        hosts
            .get(name)
            .filter(|dialed| !dialed.mux.is_closed())
            .map(|dialed| dialed.platform.clone())
    }

    async fn wait_for(&self, name: &str, timeout: Duration) -> Option<Mux> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(mux) = self.get(name) {
                return Some(mux);
            }
            tokio::time::timeout_at(deadline, changed).await.ok()?;
        }
    }
}
