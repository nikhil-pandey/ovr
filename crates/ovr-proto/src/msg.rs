//! Control messages. Each is the JSON payload of one [`FrameKind`](crate::FrameKind).

use serde::{Deserialize, Serialize};

/// `HELLO`: first frame of a run. `base` is the snapshot the client believes the host already has.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub repo: String,
    pub base: Option<String>,
    pub snapshot: String,
}

/// `WELCOME`: the agent's answer to [`Hello`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Welcome {
    /// The base is present; the speculative thin pack will be applied.
    Ok,
    /// The base is missing. The agent discards everything up to the first `EXEC`; the client follows it with a
    /// full pack and a second `EXEC`.
    NeedFull,
}

/// `EXEC`: what to run once the snapshot is checked out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Exec {
    /// Argument vector; empty opens a login shell.
    pub argv: Vec<String>,
    /// Directory relative to the checkout root, `/`-separated.
    pub cwd: String,
    /// Run under a pseudo-terminal; otherwise stdout and stderr are separate pipes.
    pub tty: bool,
    pub cols: u16,
    pub rows: u16,
}

/// `RESIZE`: the client's terminal changed size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resize {
    pub cols: u16,
    pub rows: u16,
}

/// `CONSUMED`: how much input, in total, the command has consumed or the agent has discarded, so the client may send more.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Consumed {
    pub bytes: u64,
}

/// `EXIT`: the command finished; always the last frame of a successful run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Exit {
    pub code: i32,
}

/// `ERROR`: the run failed before or around the command; the stream closes after it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Failure {
    pub message: String,
}

/// `REQUEST`: first frame from `ovr` to the local `ovrd`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    /// The client's package version; `ovrd` restarts itself when it differs.
    pub version: String,
    pub op: Op,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    /// Open a run on `host`. After [`Response::Opened`] the socket carries the agent session verbatim.
    Open { host: String },
    /// List paired hosts.
    Hosts,
    /// Issue a pairing code.
    Pair,
}

/// `RESPONSE`: the local `ovrd`'s answer to a [`Request`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Response {
    Opened,
    Hosts {
        hosts: Vec<HostStatus>,
    },
    Pair {
        addr: String,
        code: String,
    },
    /// `ovrd` is a different version and is exiting; reconnect to start the matching one.
    Restart,
    Failed {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostStatus {
    pub name: String,
    /// Present while the host is connected.
    pub platform: Option<Platform>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Platform {
    pub os: String,
    pub arch: String,
}

impl Platform {
    #[must_use]
    pub fn current() -> Self {
        Self {
            os: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
        }
    }
}

/// `BANNER`: first frame from a host agent, over SSH or after a dial-in TLS handshake.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Banner {
    pub version: String,
    pub protocol: u32,
    pub hostname: String,
    pub platform: Platform,
}

/// `PAIR`: a host redeeming a pairing code. `mac` is hex HMAC-SHA256 keyed by the code; see `ovrd`'s pairing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pair {
    pub hostname: String,
    pub mac: String,
}

/// `PAIR_OK`: the laptop proves it knows the code too.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairOk {
    pub mac: String,
}
