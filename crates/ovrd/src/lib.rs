//! `ovrd`: the initiator server and the host agent for `ovr`.

pub mod agent;
mod autostart;
mod host;
#[cfg(windows)]
mod job;
mod mux;
mod pairing;
mod server;
mod state;
mod target;
mod tls;
mod trust;
mod tunnel;

use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Parser, Subcommand};
use futures::{SinkExt, StreamExt};
use ovr_proto::msg::{Op, Provider, Request, Response};
use ovr_proto::{Frame, FrameKind, VERSION, frame, ipc};

/// Server and host agent for `ovr`.
#[derive(Debug, Parser)]
#[command(name = "ovrd", version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Print a one-shot command for a host that must dial in, through a tunnel from PROVIDER if given.
    Pair {
        /// `cloudflare` or `devtunnel`: open a tunnel with that provider's CLI unless one is open.
        provider: Option<Provider>,
    },
    /// List open tunnels and the command that points a paired host at each.
    Tunnel {
        #[command(subcommand)]
        command: Option<TunnelCommand>,
    },
    /// Trust workloads matching RULE, such as GitHub Actions jobs of a repository, to dial in without pairing.
    /// Without RULE, list the rules. Either way, print what a trusted workload needs to dial in.
    Trust {
        /// `github:OWNER/REPO`, or `github:OWNER` for every repository of OWNER.
        rule: Option<String>,
    },
    /// Stop trusting workloads matching RULE.
    Untrust {
        /// A rule as `ovrd trust` lists it.
        rule: String,
    },
    /// On a host: pair with the initiator at ADDR, then keep serving it. Without a code, move a paired host to ADDR.
    /// Without arguments, serve the paired initiator.
    Connect {
        /// `host:port`, `cloudflare:HOSTNAME`, or `devtunnel:ID`, as printed by `ovrd pair` or `ovrd tunnel`.
        addr: Option<tunnel::Addr>,
        /// Pairing code printed by `ovrd pair`, or `github:FINGERPRINT` printed by `ovrd trust` to serve as this
        /// GitHub Actions job, in the background until the job ends.
        #[arg(requires = "addr")]
        code: Option<String>,
        /// With `github:FINGERPRINT`, tell this job's host apart from others of the same job, such as a matrix's:
        /// it is named `OWNER/REPO/JOB/NAME`.
        #[arg(long, requires = "code")]
        name: Option<String>,
        /// Serve in the foreground and install nothing to start at login, as on a CI runner.
        #[arg(long, conflicts_with = "service")]
        foreground: bool,
        /// Started at login: log to a file and detach from the console.
        #[arg(long, hide = true, conflicts_with = "addr")]
        service: bool,
    },
    /// Serve runs over stdin and stdout; started by `ovrd` over SSH.
    #[command(hide = true)]
    Agent,
    /// Keep the run in DIR: own its command and log, and serve attached clients; started by the agent.
    #[command(hide = true)]
    Keep { dir: std::path::PathBuf },
}

#[derive(Debug, Subcommand)]
pub enum TunnelCommand {
    /// Close the tunnel `ovrd pair PROVIDER` opened, and delete what was created for it at the provider.
    Close { provider: Provider },
}

#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum Error {
    #[error("{0}")]
    #[diagnostic(code(ovrd::host))]
    Host(String),

    #[error("pairing failed: {0}")]
    #[diagnostic(code(ovrd::pairing))]
    Pairing(String),

    #[error(transparent)]
    #[diagnostic(code(ovrd::git))]
    Git(#[from] ovr_proto::git::Error),

    #[error(
        "{error}\nthe checkout's index stayed locked: wait for the Git command using it to finish, or, once no Git \
         process is running in the checkout, remove {}",
        lock.display()
    )]
    #[diagnostic(code(ovrd::index_locked))]
    IndexLocked {
        error: ovr_proto::git::Error,
        lock: PathBuf,
    },

    #[error("{0}")]
    #[diagnostic(code(ovrd::invalid))]
    Invalid(String),

    #[error("the connection was closed")]
    #[diagnostic(code(ovrd::disconnected))]
    Disconnected,

    #[error(transparent)]
    #[diagnostic(code(ovrd::protocol))]
    Protocol(#[from] ovr_proto::Error),

    #[error(transparent)]
    #[diagnostic(code(ovrd::io))]
    Io(#[from] io::Error),
}

/// Runs the parsed command until it finishes.
///
/// # Errors
///
/// Returns an error when the server or agent fails.
pub async fn run(cli: Cli) -> Result<(), Error> {
    match cli.command {
        None => until_stopped(server::run()).await,
        Some(Command::Pair { provider }) => pair(provider).await,
        Some(Command::Trust { rule }) => trust(Op::Trust { rule }).await,
        Some(Command::Untrust { rule }) => trust(Op::Untrust { rule }).await,
        Some(Command::Tunnel { command: None }) => tunnels().await,
        Some(Command::Tunnel {
            command: Some(TunnelCommand::Close { provider }),
        }) => close_tunnel(provider).await,
        Some(Command::Connect {
            addr,
            code,
            name,
            foreground,
            ..
        }) => {
            until_stopped(host::connect(
                addr,
                code.as_deref(),
                name.as_deref(),
                foreground,
            ))
            .await
        }
        Some(Command::Agent) => Ok(agent::serve_stdio().await?),
        Some(Command::Keep { dir }) => agent::keeper::main(dir).await,
    }
}

/// Runs `work` until it ends or `ovrd` is told to stop, by Ctrl-C or, on Unix, `SIGTERM` or `SIGHUP`. Stopping
/// returns rather than letting the signal kill the process, so the tunnel CLIs `ovrd` started are killed with it.
async fn until_stopped(work: impl Future<Output = Result<(), Error>>) -> Result<(), Error> {
    tokio::select! {
        result = work => result,
        () = stopped() => {
            tracing::info!("stopping");
            Ok(())
        }
    }
}

async fn stopped() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let (Ok(mut interrupt), Ok(mut terminate), Ok(mut hangup)) = (
            signal(SignalKind::interrupt()),
            signal(SignalKind::terminate()),
            signal(SignalKind::hangup()),
        ) else {
            return std::future::pending().await;
        };
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
            _ = hangup.recv() => {}
        }
    }
    #[cfg(not(unix))]
    if tokio::signal::ctrl_c().await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// Asks the running server, started if needed, for a pairing code.
async fn pair(tunnel: Option<Provider>) -> Result<(), Error> {
    if let Some(provider) = tunnel {
        eprintln!("opening a {provider} tunnel unless one is open...");
    }
    match request(Op::Pair { tunnel }).await? {
        Response::Pair { addr, code } => {
            println!("run this on the host within 10 minutes:\n  ovrd connect {addr} {code}");
            Ok(())
        }
        other => Err(unexpected(other)),
    }
}

/// `ovrd trust` and `ovrd untrust`: changes the rules, if asked to, then lists them and how a workload dials in.
async fn trust(op: Op) -> Result<(), Error> {
    let changed = match &op {
        Op::Trust { rule: Some(rule) } => Some(format!("trusting {rule}")),
        Op::Untrust { rule } => Some(format!("no longer trusting {rule}")),
        _ => None,
    };
    match request(op).await? {
        Response::Trust {
            rules,
            initiator,
            addr,
        } => {
            if let Some(changed) = changed {
                println!("{changed}");
            }
            if rules.is_empty() {
                println!(
                    "no workload is trusted; `ovrd trust github:OWNER/REPO` trusts a repository's jobs"
                );
                return Ok(());
            }
            println!("trusted:");
            for rule in &rules {
                println!("  {rule}");
            }
            let repo = rules
                .iter()
                .find_map(|rule| {
                    rule.strip_prefix("github:")
                        .filter(|spec| spec.contains('/'))
                })
                .unwrap_or("OWNER/REPO");
            println!(
                "\nstore where this machine is as repository variables (or organization variables, with --org):\n  \
                 gh variable set OVR_ADDR --repo {repo} --body {addr}\n  \
                 gh variable set OVR_INITIATOR --repo {repo} --body {initiator}\n\n\
                 then add a step to a job with `permissions: id-token: write`:\n  \
                 - if: failure() || runner.debug == '1'\n    \
                 uses: nikhil-pandey/ovr@v{VERSION}\n    \
                 with:\n      \
                 addr: ${{{{ vars.OVR_ADDR }}}}\n      \
                 initiator: ${{{{ vars.OVR_INITIATOR }}}}\n      \
                 hold: 30m\n\n\
                 or run `ovrd connect {addr} github:{initiator}` in any step"
            );
            if !addr.starts_with(Provider::Devtunnel.name()) {
                println!(
                    "\nGitHub-hosted runners reach {addr} only through a tunnel or your tailnet; \
                     `ovrd pair devtunnel` opens a tunnel whose address stays the same"
                );
            }
            Ok(())
        }
        other => Err(unexpected(other)),
    }
}

/// `ovrd tunnel`: lists open tunnels.
async fn tunnels() -> Result<(), Error> {
    match request(Op::Tunnels).await? {
        Response::Tunnels { tunnels } if tunnels.is_empty() => {
            println!("no tunnels are open; `ovrd pair PROVIDER` opens one");
            Ok(())
        }
        Response::Tunnels { tunnels } => {
            for tunnel in tunnels {
                match tunnel.addr {
                    Some(addr) => println!("{:<10}  ovrd connect {addr}", tunnel.provider),
                    None => println!("{:<10}  (restarting)", tunnel.provider),
                }
            }
            Ok(())
        }
        other => Err(unexpected(other)),
    }
}

async fn close_tunnel(provider: Provider) -> Result<(), Error> {
    match request(Op::CloseTunnel { provider }).await? {
        Response::TunnelClosed => {
            println!("closed the {provider} tunnel");
            Ok(())
        }
        other => Err(unexpected(other)),
    }
}

/// Sends `op` to the running server, started if needed, and returns its response. A server of another version is
/// asked to make way first, as `ovr` does.
async fn request(op: Op) -> Result<Response, Error> {
    let program = std::env::current_exe()?;
    let request = Frame::json(
        FrameKind::Request,
        &Request {
            version: VERSION.to_owned(),
            op,
        },
    )?;
    // Servers before protocol 3 read the whole request before its version, and close on an operation they do not
    // know without answering; one they all know gets them to restart.
    let known = Frame::json(
        FrameKind::Request,
        &Request {
            version: VERSION.to_owned(),
            op: Op::Hosts,
        },
    )?;
    let mut last = Error::Disconnected;
    let mut probed = false;
    for _ in 0..3 {
        match exchange(&program, request.clone()).await {
            Ok(Some(Response::Restart)) => {}
            Ok(Some(response)) => return Ok(response),
            Ok(None) if !probed => {
                probed = true;
                let probe = exchange(&program, known.clone()).await;
                if !matches!(probe, Ok(Some(Response::Restart))) {
                    return Err(Error::Disconnected);
                }
            }
            Ok(None) => last = Error::Disconnected,
            Err(err) => last = err,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(last)
}

/// Sends one request on a new connection, returning the response, if any came.
async fn exchange(program: &Path, request: Frame) -> Result<Option<Response>, Error> {
    let mut stream = frame::framed(ipc::connect_or_spawn(program).await?);
    stream.send(request).await?;
    match stream.next().await {
        Some(frame) => Ok(Some(frame?.expect(FrameKind::Response)?)),
        None => Ok(None),
    }
}

fn unexpected(response: Response) -> Error {
    match response {
        Response::Failed { message } => Error::Host(message),
        Response::Restart => Error::Host("ovrd was restarting; try again".to_owned()),
        other => Error::Host(format!("unexpected response: {other:?}")),
    }
}
