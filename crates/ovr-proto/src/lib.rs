//! Wire protocol shared by `ovr` and `ovrd`.
//!
//! A stream is a sequence of frames: a five-byte [`Header`] followed by `len` payload bytes. Control frames carry
//! JSON; data frames carry raw bytes.

use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 1;

/// Largest payload a data frame may carry.
pub const MAX_DATA_LEN: u32 = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    Hello = 1,
    Exec = 2,
    Resize = 3,
    Exit = 4,
    Pack = 16,
    Out = 17,
    In = 18,
}

impl TryFrom<u8> for FrameKind {
    type Error = Error;

    fn try_from(byte: u8) -> Result<Self, Error> {
        Ok(match byte {
            1 => Self::Hello,
            2 => Self::Exec,
            3 => Self::Resize,
            4 => Self::Exit,
            16 => Self::Pack,
            17 => Self::Out,
            18 => Self::In,
            other => return Err(Error::UnknownFrame(other)),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub kind: FrameKind,
    pub len: u32,
}

impl Header {
    pub const SIZE: usize = 5;

    #[must_use]
    pub fn encode(self) -> [u8; Self::SIZE] {
        let [a, b, c, d] = self.len.to_le_bytes();
        [self.kind as u8, a, b, c, d]
    }

    /// # Errors
    ///
    /// Returns an error for an unknown frame kind or a payload longer than [`MAX_DATA_LEN`].
    pub fn decode(bytes: [u8; Self::SIZE]) -> Result<Self, Error> {
        let [kind, a, b, c, d] = bytes;
        let len = u32::from_le_bytes([a, b, c, d]);
        if len > MAX_DATA_LEN {
            return Err(Error::TooLarge(len));
        }
        Ok(Self {
            kind: FrameKind::try_from(kind)?,
            len,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub version: u32,
    pub repo: String,
    pub base: Option<String>,
    pub snapshot: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HelloReply {
    Ok,
    NeedFull,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Exec {
    pub argv: Vec<String>,
    pub cwd: String,
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Resize {
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Exit {
    pub code: i32,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unknown frame kind {0}")]
    UnknownFrame(u8),
    #[error("frame of {0} bytes exceeds the {MAX_DATA_LEN}-byte limit")]
    TooLarge(u32),
    #[error("malformed control frame")]
    Json(#[from] serde_json::Error),
}
