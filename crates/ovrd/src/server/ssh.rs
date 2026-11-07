//! SSH hosts: one persistent `ovrd agent` per host, multiplexed, installed over SFTP when missing.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use futures::StreamExt;
use ovr_proto::msg::Banner;
use ovr_proto::{FrameKind, PROTOCOL_VERSION, VERSION, frame};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};

use crate::mux::{Channel, Mux};
use crate::{Error, target};

/// An agent connection is closed after this long without streams; runs carry on without it.
const IDLE: Duration = Duration::from_mins(10);

/// Covers slow logins, e.g. through a jump host or with a second factor already cached.
const BANNER_TIMEOUT: Duration = Duration::from_secs(30);

/// Never prompt, since nobody is there to answer; Git packs are already compressed. Probe the server so that a
/// vanished host ends the connection within a minute.
const OPTIONS: [&str; 9] = [
    "-o",
    "BatchMode=yes",
    "-o",
    "Compression=no",
    "-o",
    "ServerAliveInterval=15",
    "-o",
    "ServerAliveCountMax=3",
    "-T",
];

/// Exit status of a shell that cannot find the command.
const NOT_FOUND: i32 = 127;

/// Agent connections by SSH destination, with one connection attempt at a time per destination.
#[derive(Default)]
pub struct Pool {
    hosts: Mutex<HashMap<String, Arc<tokio::sync::Mutex<Option<Connection>>>>>,
}

#[derive(Clone)]
struct Connection {
    mux: Mux,
    usage: Arc<Usage>,
}

struct Usage {
    active: AtomicUsize,
    idle_since: Mutex<Instant>,
}

/// Keeps a connection from being reaped while a run uses it.
pub struct Lease(Arc<Usage>);

impl Drop for Lease {
    fn drop(&mut self) {
        *self
            .0
            .idle_since
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Instant::now();
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Pool {
    /// Opens a channel to the agent on `host`, connecting and installing it first if needed.
    ///
    /// # Errors
    ///
    /// Returns an error when SSH fails or the agent cannot be installed.
    pub async fn open(&self, host: &str) -> Result<(Channel, Lease), Error> {
        if host.starts_with('-') {
            return Err(Error::Invalid(format!("invalid host {host:?}")));
        }
        let slot = Arc::clone(
            self.hosts
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(host.to_owned())
                .or_default(),
        );
        let mut slot = slot.lock().await;
        let connection = match slot
            .as_ref()
            .filter(|connection| !connection.mux.is_closed())
        {
            Some(connection) => connection.clone(),
            None => slot.insert(connect(host).await?).clone(),
        };
        drop(slot);
        connection.usage.active.fetch_add(1, Ordering::SeqCst);
        let lease = Lease(Arc::clone(&connection.usage));
        Ok((connection.mux.open().await?, lease))
    }
}

impl Pool {
    /// Destinations with an open connection, in order.
    pub fn connected(&self) -> Vec<String> {
        let hosts = self.hosts.lock().unwrap_or_else(PoisonError::into_inner);
        let mut names: Vec<String> = hosts
            .iter()
            .filter(|(_, slot)| {
                // A slot being connected right now is not open yet.
                slot.try_lock()
                    .is_ok_and(|slot| slot.as_ref().is_some_and(|c| !c.mux.is_closed()))
            })
            .map(|(name, _)| name.clone())
            .collect();
        names.sort();
        names
    }
}

async fn connect(host: &str) -> Result<Connection, Error> {
    if let Some(connection) = start(host).await? {
        return Ok(connection);
    }
    tracing::info!(host, "installing ovrd {VERSION}");
    install(host).await?;
    start(host)
        .await?
        .ok_or_else(|| Error::Host(format!("{host}: ovrd did not start after installing it")))
}

fn remote_path() -> String {
    format!("{}/ovrd-{VERSION}", ovr_proto::paths::SSH_AGENTS)
}

/// Starts the agent; `None` when it is not installed.
async fn start(host: &str) -> Result<Option<Connection>, Error> {
    let mut child = Command::new("ssh")
        .args(OPTIONS)
        .arg("--")
        .arg(host)
        // Relative to the home directory, where SSH starts; a path with a slash bypasses PATH lookup.
        .arg(format!("{} agent", remote_path()))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|err| Error::Host(format!("cannot run ssh: {err}")))?;
    let (Some(stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
        return Err(Error::Host("ssh has no stdio".to_owned()));
    };
    let stderr = capture_stderr(&mut child, host);

    let mut agent = frame::framed(tokio::io::join(stdout, stdin));
    let banner = tokio::time::timeout(BANNER_TIMEOUT, agent.next()).await;
    let banner = match banner {
        Err(_) => {
            return Err(Error::Host(format!(
                "{host}: timed out waiting for the agent to start"
            )));
        }
        Ok(Some(Ok(frame))) if frame.kind == FrameKind::Banner => frame.parse::<Banner>()?,
        Ok(None) => {
            let status = child.wait().await?;
            if status.code() == Some(NOT_FOUND) {
                return Ok(None);
            }
            let stderr = stderr
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            return Err(Error::Host(format!(
                "ssh {host} failed ({status}): {}",
                stderr.trim()
            )));
        }
        Ok(_) => {
            return Err(Error::Host(format!(
                "{host} printed unexpected output before the agent started; \
                 make sure shell startup files print nothing for non-interactive sessions"
            )));
        }
    };
    if banner.version != VERSION || banner.protocol != PROTOCOL_VERSION {
        return Err(Error::Host(format!(
            "{host} runs ovrd {} but this is {VERSION}",
            banner.version
        )));
    }
    tracing::info!(host, platform = ?banner.platform, "connected over ssh");

    let mux = Mux::open_on(frame::resume(agent));
    let usage = Arc::new(Usage {
        active: AtomicUsize::new(0),
        idle_since: Mutex::new(Instant::now()),
    });
    tokio::spawn(supervise(
        child,
        mux.clone(),
        Arc::clone(&usage),
        host.to_owned(),
    ));
    Ok(Some(Connection { mux, usage }))
}

/// Ends the connection when it has been idle too long, and the mux when SSH exits.
async fn supervise(mut child: Child, mux: Mux, usage: Arc<Usage>, host: String) {
    let mut ticks = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            status = child.wait() => {
                tracing::info!(host, ?status, "ssh exited");
                mux.close();
                return;
            }
            () = mux.closed() => break,
            _ = ticks.tick() => {
                let idle = usage.idle_since.lock().unwrap_or_else(PoisonError::into_inner).elapsed();
                if usage.active.load(Ordering::SeqCst) == 0 && idle >= IDLE {
                    tracing::info!(host, "closing idle connection");
                    mux.close();
                    break;
                }
            }
        }
    }
    let _ = child.kill().await;
}

/// Logs SSH's stderr and keeps its tail for error messages.
fn capture_stderr(child: &mut Child, host: &str) -> Arc<Mutex<String>> {
    let tail = Arc::new(Mutex::new(String::new()));
    if let Some(stderr) = child.stderr.take() {
        let (tail, host) = (Arc::clone(&tail), host.to_owned());
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::debug!(host, "{line}");
                let mut tail = tail.lock().unwrap_or_else(PoisonError::into_inner);
                if tail.len() < 4096 {
                    tail.push_str(&line);
                    tail.push('\n');
                }
            }
        });
    }
    tail
}

/// Uploads the `ovrd` build matching this one for the host's platform.
async fn install(host: &str) -> Result<(), Error> {
    let uname = ssh_output(host, "uname -sm").await?;
    let target = target::from_uname(&uname).ok_or_else(|| {
        Error::Host(format!(
            "{host} runs {}, which ovr cannot reach over SSH; dial in instead with `ovrd pair`",
            uname.trim()
        ))
    })?;
    let binary = binary_for(target).await?;
    upload(host, &binary).await
}

async fn ssh_output(host: &str, command: &str) -> Result<String, Error> {
    let output = Command::new("ssh")
        .args(OPTIONS)
        .arg("--")
        .arg(host)
        .arg(command)
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        Err(Error::Host(format!(
            "ssh {host} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

/// Removes agents downloaded for versions other than this one, returning how many and the bytes they held.
///
/// # Errors
///
/// Returns an error when the cache cannot be read.
pub fn clean_downloads() -> Result<(u64, u64), Error> {
    let dir = ovr_proto::paths::cache_dir()?.join("bin");
    let versions = match std::fs::read_dir(&dir) {
        Ok(entries) => entries.filter_map(Result::ok).collect(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(err) => return Err(err.into()),
    };
    let (mut agents, mut bytes) = (0, 0);
    for version in versions {
        if version.file_name() == VERSION {
            continue;
        }
        agents += std::fs::read_dir(version.path()).map_or(0, |targets| targets.count() as u64);
        bytes += crate::agent::clean::remove(&version.path());
    }
    Ok((agents, bytes))
}

/// This binary when the host matches, otherwise the release build for `target`, downloaded once and cached.
async fn binary_for(target: &str) -> Result<PathBuf, Error> {
    if target::current() == Some(target) {
        return Ok(std::env::current_exe()?);
    }
    let dir = ovr_proto::paths::cache_dir()?
        .join("bin")
        .join(VERSION)
        .join(target);
    let binary = dir.join("ovrd");
    if binary.is_file() {
        return Ok(binary);
    }
    tokio::fs::create_dir_all(&dir).await?;
    let staging = tempfile::tempdir_in(&dir)?;
    download(&target::release_url(VERSION, target), staging.path()).await?;
    tokio::fs::rename(staging.path().join("ovrd"), &binary).await?;
    Ok(binary)
}

/// `curl URL | tar -xz ovrd`, with the tools every supported initiator ships.
async fn download(url: &str, dir: &Path) -> Result<(), Error> {
    let failed = |what: &str| Error::Host(format!("cannot download {url}: {what}"));
    let mut curl = Command::new("curl")
        .args(["-fsSL", "--", url])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let archive: Stdio = curl
        .stdout
        .take()
        .ok_or_else(|| failed("no curl output"))?
        .try_into()?;
    let tar = Command::new("tar")
        .args(["-xzf", "-", "-C"])
        .arg(dir)
        .arg("ovrd")
        .stdin(archive)
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .output();
    let (curl, tar) = tokio::join!(curl.wait_with_output(), tar);
    let curl = curl?;
    if !curl.status.success() {
        return Err(failed(String::from_utf8_lossy(&curl.stderr).trim()));
    }
    let tar = tar?;
    if !tar.status.success() {
        return Err(failed(String::from_utf8_lossy(&tar.stderr).trim()));
    }
    Ok(())
}

/// Copies `binary` into place with SFTP, so that no remote shell parses any of it.
async fn upload(host: &str, binary: &Path) -> Result<(), Error> {
    let remote = remote_path();
    let local = binary.to_string_lossy().replace('\\', "/");
    let batch = format!(
        "-mkdir .cache\n-mkdir .cache/ovr\n-mkdir .cache/ovr/bin\n\
         put \"{local}\" {remote}.part\nchmod 755 {remote}.part\n-rm {remote}\nrename {remote}.part {remote}\n"
    );
    let mut sftp = Command::new("sftp")
        .args(["-q", "-b", "-", "-o", "BatchMode=yes", "--", host])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    if let Some(mut stdin) = sftp.stdin.take() {
        stdin.write_all(batch.as_bytes()).await?;
    }
    let output = sftp.wait_with_output().await?;
    if output.status.success() {
        Ok(())
    } else {
        Err(Error::Host(format!(
            "cannot upload ovrd to {host}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}
