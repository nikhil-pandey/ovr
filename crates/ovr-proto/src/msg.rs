//! Control messages. Each is the JSON payload of one [`FrameKind`](crate::FrameKind).

use serde::{Deserialize, Serialize};

/// `HELLO`: first frame of a new run. `base` is the snapshot the client believes the host already has.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    /// The host's checkout for this initiator and local repository: `<repo>-<key>`.
    pub checkout: String,
    /// Orders the snapshots of one local repository; a checkout never goes back to a lower one.
    pub seq: u64,
    pub base: Option<String>,
    pub snapshot: String,
    /// The initiator starting the run, whose runs `ovr ps` lists.
    pub initiator: String,
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

/// `ATTACH`: first frame of a stream that resumes run `run`, with output from the given offsets onwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attach {
    pub run: u64,
    pub out: u64,
    pub err: u64,
}

/// `ATTACHED`: the run's output follows from these offsets, which are later than requested when the log no longer
/// holds the requested ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attached {
    pub run: u64,
    pub tty: bool,
    pub out: u64,
    pub err: u64,
}

/// `RUNS`: first frame of a stream that lists `initiator`'s runs; answered by [`RunList`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Runs {
    pub initiator: String,
}

/// `RUN_LIST`: runs on a host, oldest first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunList {
    pub runs: Vec<RunInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunInfo {
    pub run: u64,
    pub checkout: String,
    pub argv: Vec<String>,
    /// Seconds since the Unix epoch.
    pub started: u64,
    pub state: RunState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Running,
    Exited {
        code: i32,
    },
    /// The command could not be started.
    Failed {
        message: String,
    },
    /// The keeper died without recording how the command ended.
    Lost,
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
    /// Open a stream to `host`'s agent. After [`Response::Opened`] the socket carries it verbatim.
    Open { host: String },
    /// List paired hosts.
    Hosts,
    /// List hosts with an open connection: dialed-in hosts that are online and SSH hosts in use.
    Connected,
    /// Issue a pairing code, for an address through a tunnel from `tunnel` if given, which is opened first.
    Pair { tunnel: Option<Provider> },
    /// List open tunnels.
    Tunnels,
    /// Close the tunnel from `provider` and delete what `ovrd` created there.
    CloseTunnel { provider: Provider },
}

/// `RESPONSE`: the local `ovrd`'s answer to a [`Request`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Response {
    /// `initiator` identifies this initiator to hosts.
    Opened {
        initiator: String,
    },
    Hosts {
        hosts: Vec<HostStatus>,
    },
    Connected {
        hosts: Vec<String>,
    },
    Pair {
        addr: String,
        code: String,
    },
    Tunnels {
        tunnels: Vec<TunnelStatus>,
    },
    TunnelClosed,
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

/// A tunnel provider, whose CLI `ovrd` runs to reach the initiator from networks that cannot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    /// Cloudflare quick tunnels, through `cloudflared`.
    Cloudflare,
    /// Microsoft Dev Tunnels, through `devtunnel`.
    Devtunnel,
}

impl Provider {
    pub const ALL: [Self; 2] = [Self::Cloudflare, Self::Devtunnel];

    /// The name used in commands and addresses, as in `cloudflare:HOSTNAME`.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Cloudflare => "cloudflare",
            Self::Devtunnel => "devtunnel",
        }
    }
}

impl std::fmt::Display for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl std::str::FromStr for Provider {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|provider| provider.name() == name)
            .ok_or_else(|| format!("unknown tunnel provider {name:?}; use cloudflare or devtunnel"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TunnelStatus {
    pub provider: Provider,
    /// The address hosts dial, as in `cloudflare:HOSTNAME`; absent while the tunnel is starting or restarting.
    pub addr: Option<String>,
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

/// `PAIR_OK`: the initiator proves it knows the code too.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairOk {
    pub mac: String,
}
