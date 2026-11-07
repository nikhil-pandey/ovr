//! The local endpoint between `ovr` and `ovrd`: a Unix socket in the state directory, or a per-user named pipe on
//! Windows.

use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use crate::state_dir;

/// How long a client waits for a freshly spawned `ovrd` to start listening.
const SPAWN_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(unix)]
pub type ClientStream = tokio::net::UnixStream;
#[cfg(unix)]
pub type ServerStream = tokio::net::UnixStream;
#[cfg(windows)]
pub type ClientStream = tokio::net::windows::named_pipe::NamedPipeClient;
#[cfg(windows)]
pub type ServerStream = tokio::net::windows::named_pipe::NamedPipeServer;

#[cfg(unix)]
fn endpoint() -> io::Result<PathBuf> {
    Ok(state_dir()?.join("ovrd.sock"))
}

#[cfg(windows)]
#[allow(clippy::unnecessary_wraps)] // same signature as on Unix
fn endpoint() -> io::Result<PathBuf> {
    let user = std::env::var("USERNAME").unwrap_or_default();
    Ok(PathBuf::from(format!(r"\\.\pipe\ovrd-{user}")))
}

/// Connects to a running `ovrd`.
///
/// # Errors
///
/// Returns an error when no `ovrd` is listening.
pub async fn connect() -> io::Result<ClientStream> {
    let endpoint = endpoint()?;
    #[cfg(unix)]
    {
        tokio::net::UnixStream::connect(endpoint).await
    }
    #[cfg(windows)]
    {
        /// All pipe instances are taken; the server creates the next one right after each accept.
        const ERROR_PIPE_BUSY: i32 = 231;
        let mut attempts = 0;
        loop {
            match tokio::net::windows::named_pipe::ClientOptions::new().open(&endpoint) {
                Err(err) if err.raw_os_error() == Some(ERROR_PIPE_BUSY) && attempts < 50 => {
                    attempts += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                result => return result,
            }
        }
    }
}

/// Connects to `ovrd`, starting `program` in the background first if it is not running.
///
/// # Errors
///
/// Returns an error when `program` cannot be started or does not start listening in time.
pub async fn connect_or_spawn(program: &Path) -> io::Result<ClientStream> {
    if let Ok(stream) = connect().await {
        return Ok(stream);
    }
    spawn(program)?;
    let deadline = Instant::now() + SPAWN_TIMEOUT;
    loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        match connect().await {
            Ok(stream) => return Ok(stream),
            Err(_) if Instant::now() < deadline => {}
            Err(err) => return Err(err),
        }
    }
}

/// Starts `program` detached from this process, its terminal, and its signals, logging to `ovrd.log`.
fn spawn(program: &Path) -> io::Result<()> {
    let log = std::fs::File::create(state_dir()?.join("ovrd.log"))?;
    let mut command = std::process::Command::new(program);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
    }
    command.spawn().map(drop)
}

/// The server side of the endpoint. Only one `ovrd` per user may hold it; callers serialise with a lock file.
pub struct Listener {
    #[cfg(unix)]
    inner: tokio::net::UnixListener,
    #[cfg(unix)]
    path: PathBuf,
    #[cfg(windows)]
    next: tokio::net::windows::named_pipe::NamedPipeServer,
    #[cfg(windows)]
    path: PathBuf,
}

impl Listener {
    /// Binds the endpoint, replacing a stale socket left by an `ovrd` that did not exit cleanly.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint cannot be created.
    pub fn bind() -> io::Result<Self> {
        let path = endpoint()?;
        #[cfg(unix)]
        {
            match std::fs::remove_file(&path) {
                Err(err) if err.kind() != io::ErrorKind::NotFound => return Err(err),
                _ => {}
            }
            let inner = tokio::net::UnixListener::bind(&path)?;
            Ok(Self { inner, path })
        }
        #[cfg(windows)]
        {
            let next = tokio::net::windows::named_pipe::ServerOptions::new()
                .first_pipe_instance(true)
                .create(&path)?;
            Ok(Self { next, path })
        }
    }

    /// Waits for the next client.
    ///
    /// # Errors
    ///
    /// Returns an error when accepting fails.
    pub async fn accept(&mut self) -> io::Result<ServerStream> {
        #[cfg(unix)]
        {
            let (stream, _) = self.inner.accept().await?;
            Ok(stream)
        }
        #[cfg(windows)]
        {
            self.next.connect().await?;
            let fresh = tokio::net::windows::named_pipe::ServerOptions::new().create(&self.path)?;
            Ok(std::mem::replace(&mut self.next, fresh))
        }
    }
}

#[cfg(unix)]
impl Drop for Listener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
