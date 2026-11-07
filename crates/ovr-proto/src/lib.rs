//! Wire protocol and local IPC shared by `ovr` and `ovrd`.
//!
//! A stream is a sequence of frames: `len:u32le` covering the rest, `kind:u8`, then the payload. Control frames
//! carry JSON; data frames carry raw bytes.

pub mod frame;
pub mod git;
pub mod ipc;
pub mod msg;
pub mod paths;

use std::io;

pub use frame::{Frame, FrameKind};

/// Package version of this build. `ovr`, `ovrd`, and every SSH agent run the same version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Agent protocol version. Dial-in hosts may run an older `ovrd`; they are accepted while this matches.
pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unknown frame kind {0}")]
    UnknownFrame(u8),
    #[error("unexpected {0:?} frame")]
    Unexpected(FrameKind),
    #[error("malformed control frame")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}
