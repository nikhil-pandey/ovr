//! The agent: `ovrd` on a host, serving runs from the initiator over a multiplexed connection.

pub mod checkout;
pub mod clean;
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
use tokio::task::JoinSet;
use tokio_util::codec::FramedWrite;

use crate::mux::Incoming;
pub use process::Shell;

/// Where checkouts and runs live and how commands start.
#[derive(Debug, Clone)]
pub struct Config {
    pub root: PathBuf,
    pub runs: PathBuf,
    /// Where agents uploaded over SSH live, so that cleaning removes other versions.
    pub agents: PathBuf,
    /// Where runs without a checkout start: the user's home, or a CI job's workspace.
    pub base: PathBuf,
    pub shell: Shell,
    /// The `ovrd` that keeps runs, as `ovrd keep`; `None` keeps them in this process, which only tests want, since
    /// runs then end with it.
    pub keeper: Option<PathBuf>,
}

impl Config {
    /// The checkouts, runs, and uploaded agents directories, the home directory as base, the user's shell, and this
    /// `ovrd` as keeper.
    ///
    /// # Errors
    ///
    /// Returns an error when the home directory is unknown or a directory cannot be created.
    pub fn detect() -> io::Result<Self> {
        let home = ovr_proto::paths::home_dir()?;
        Ok(Self {
            root: ovr_proto::paths::checkouts_dir()?,
            runs: ovr_proto::paths::runs_dir()?,
            agents: home.join(ovr_proto::paths::SSH_AGENTS),
            base: home,
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

/// Serves the initiator's streams until it closes the connection. Runs carry on with their keepers.
pub(crate) async fn serve(mut incoming: Incoming, config: Arc<Config>) {
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
    // Every stream has failed with the connection, so each session ends at its next read or write. Letting them get
    // there, rather than aborting them, never kills Git mid-update and leaves its lock files behind.
    while sessions.join_next().await.is_some() {}
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
    let stdio = tokio::io::join(tokio::io::stdin(), stdout.into_inner());
    serve(Incoming::accept_on(stdio), config).await;
    Ok(())
}
