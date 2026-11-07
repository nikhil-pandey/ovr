//! `ovrd`: the initiator server and the host agent for `ovr`.

pub mod agent;
mod autostart;
mod host;
mod mux;
mod pairing;
mod server;
mod state;
mod target;
mod tls;
mod tunnel;

use std::io;

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
    /// On a host: pair with the initiator at ADDR, then keep serving it. Without a code, move a paired host to ADDR.
    /// Without arguments, serve the paired initiator.
    Connect {
        /// `host:port`, `cloudflare:HOSTNAME`, or `devtunnel:ID`, as printed by `ovrd pair` or `ovrd tunnel`.
        addr: Option<tunnel::Addr>,
        /// Pairing code printed by `ovrd pair`.
        #[arg(requires = "addr")]
        code: Option<String>,
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
        Some(Command::Tunnel { command: None }) => tunnels().await,
        Some(Command::Tunnel {
            command: Some(TunnelCommand::Close { provider }),
        }) => close_tunnel(provider).await,
        Some(Command::Connect {
            addr,
            code,
            foreground,
            ..
        }) => until_stopped(host::connect(addr, code.as_deref(), foreground)).await,
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

/// Sends `op` to the running server, started if needed, and returns its response.
async fn request(op: Op) -> Result<Response, Error> {
    let stream = ipc::connect_or_spawn(&std::env::current_exe()?).await?;
    let mut stream = frame::framed(stream);
    let request = Request {
        version: VERSION.to_owned(),
        op,
    };
    stream
        .send(Frame::json(FrameKind::Request, &request)?)
        .await?;
    Ok(stream
        .next()
        .await
        .ok_or(Error::Disconnected)??
        .expect(FrameKind::Response)?)
}

fn unexpected(response: Response) -> Error {
    match response {
        Response::Failed { message } => Error::Host(message),
        Response::Restart => Error::Host("ovrd was restarting; try again".to_owned()),
        other => Error::Host(format!("unexpected response: {other:?}")),
    }
}
