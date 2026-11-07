//! The agent: `ovrd` on a host, serving runs from the initiator over a multiplexed connection.

pub mod checkout;
pub mod keeper;
mod log;
pub mod process;
pub mod runs;
pub mod session;

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use futures::SinkExt;
use ovr_proto::msg::{Banner, Platform};
use ovr_proto::{Frame, FrameKind, PROTOCOL_VERSION, VERSION, frame};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::JoinSet;
use tokio_util::codec::FramedWrite;

use crate::mux::Incoming;
pub use process::Shell;

/// Where checkouts and runs live and how commands start.
#[derive(Debug, Clone)]
pub struct Config {
    pub root: PathBuf,
    pub runs: PathBuf,
    pub shell: Shell,
    /// The `ovrd` that keeps runs, as `ovrd keep`; `None` keeps them in this process, which only tests want, since
    /// runs then end with it.
    pub keeper: Option<PathBuf>,
}

impl Config {
    /// `~/ovr` on Unix or `C:\ovr` on Windows, the runs directory, the user's shell, and this `ovrd` as keeper.
    ///
    /// # Errors
    ///
    /// Returns an error when the home directory is unknown.
    pub fn detect() -> io::Result<Self> {
        #[cfg(unix)]
        let root = ovr_proto::paths::home_dir()?.join("ovr");
        #[cfg(windows)]
        let root = {
            let drive = std::env::var("SystemDrive").unwrap_or_else(|_| "C:".to_owned());
            PathBuf::from(format!(r"{drive}\ovr"))
        };
        Ok(Self {
            root,
            runs: ovr_proto::paths::runs_dir()?,
            shell: Shell::detect(),
            keeper: Some(std::env::current_exe()?),
        })
    }
}

/// What this host tells the initiator when it connects.
#[must_use]
pub fn banner() -> Banner {
    Banner {
        version: VERSION.to_owned(),
        protocol: PROTOCOL_VERSION,
        hostname: hostname(),
        platform: Platform::current(),
    }
}

#[must_use]
pub fn hostname() -> String {
    let name = gethostname::gethostname().to_string_lossy().into_owned();
    // Hosts are addressed by their short name, like SSH aliases usually are.
    name.split('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
}

/// Serves streams over `stream` until the initiator closes it. Runs carry on with their keepers.
pub async fn serve<S>(stream: S, config: Arc<Config>)
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let mut incoming = Incoming::accept_on(stream);
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            channel = incoming.accept() => {
                let Some(channel) = channel else { break };
                let config = Arc::clone(&config);
                sessions.spawn(async move { session::serve(channel, &config).await });
            }
            Some(_) = sessions.join_next() => {}
        }
    }
    sessions.shutdown().await;
}

/// `ovrd agent`: announce this host on stdout, then serve over stdin and stdout until SSH closes them.
///
/// # Errors
///
/// Returns an error when stdout is closed before the banner is written.
pub async fn serve_stdio() -> Result<(), ovr_proto::Error> {
    let config = Arc::new(Config::detect()?);
    let mut stdout = FramedWrite::new(tokio::io::stdout(), frame::Codec::default());
    stdout
        .send(Frame::json(FrameKind::Banner, &banner())?)
        .await?;
    serve(
        tokio::io::join(tokio::io::stdin(), stdout.into_inner()),
        config,
    )
    .await;
    Ok(())
}
