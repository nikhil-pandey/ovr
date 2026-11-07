//! Wire protocol and local IPC shared by `ovr` and `ovrd`.
//!
//! A stream is a sequence of frames: a five-byte header (`kind:u8, len:u32le`) followed by `len` payload bytes.
//! Control frames carry JSON; data frames carry raw bytes.

pub mod frame;
pub mod ipc;
pub mod msg;

use std::io;
use std::path::PathBuf;

pub use frame::{Frame, FrameKind};

/// Package version of this build. `ovr`, `ovrd`, and every SSH agent run the same version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Agent protocol version. Dial-in hosts may run an older `ovrd`; they are accepted while this matches.
pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unknown frame kind {0}")]
    UnknownFrame(u8),
    #[error("frame of {0} bytes exceeds the {max}-byte limit", max = frame::MAX_LEN)]
    TooLarge(u32),
    #[error("unexpected {0:?} frame")]
    Unexpected(FrameKind),
    #[error("malformed control frame")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// The per-user directory holding `ovrd`'s socket, log, identity, and pairing records, created private on first
/// use: `~/.local/state/ovr` on Unix, `%LOCALAPPDATA%\ovr` on Windows.
///
/// # Errors
///
/// Returns an error when the home directory is unknown or the directory cannot be created.
pub fn state_dir() -> io::Result<PathBuf> {
    #[cfg(unix)]
    let dir = home_dir()?.join(".local/state/ovr");
    #[cfg(windows)]
    let dir = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "LOCALAPPDATA is not set"))?
        .join("ovr");

    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(&dir)?;
    Ok(dir)
}

/// The current user's home directory.
///
/// # Errors
///
/// Returns an error when it cannot be determined.
pub fn home_dir() -> io::Result<PathBuf> {
    std::env::home_dir()
        .filter(|home| !home.as_os_str().is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "cannot determine the home directory",
            )
        })
}
