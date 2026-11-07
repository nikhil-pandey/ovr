//! `ovrd`: the laptop server and the host agent for `ovr`.

pub mod agent;
mod autostart;
mod host;
pub mod mux;
mod pairing;
pub mod server;
mod state;
mod target;
mod tls;

use std::io;

use clap::{Parser, Subcommand};
use ovr_proto::msg::{Op, Request, Response};
use ovr_proto::{FrameKind, VERSION, frame, ipc};

/// Server and host agent for `ovr`.
#[derive(Debug, Parser)]
#[command(name = "ovrd", version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Print a one-shot command for a host that must dial in.
    Pair,
    /// On a host: pair with the laptop at ADDR, then keep serving it. Without arguments, serve a paired laptop.
    Connect {
        /// Address printed by `ovrd pair`.
        #[arg(requires = "code")]
        addr: Option<String>,
        /// Pairing code printed by `ovrd pair`.
        #[arg(requires = "addr")]
        code: Option<String>,
        /// Started at login: log to a file and detach from the console.
        #[arg(long, hide = true, conflicts_with = "addr")]
        service: bool,
    },
    /// Serve runs over stdin and stdout; started by `ovrd` over SSH.
    #[command(hide = true)]
    Agent,
}

#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum Error {
    #[error("{0}")]
    #[diagnostic(code(ovrd::host))]
    Host(String),

    #[error("pairing failed: {0}")]
    #[diagnostic(code(ovrd::pairing))]
    Pairing(String),

    #[error("git {command} failed: {stderr}")]
    #[diagnostic(code(ovrd::git))]
    Git { command: String, stderr: String },

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
        None => server::run().await,
        Some(Command::Pair) => pair().await,
        Some(Command::Connect {
            addr: Some(addr),
            code: Some(code),
            ..
        }) => host::pair(&addr, &code).await,
        Some(Command::Connect { .. }) => host::serve().await,
        Some(Command::Agent) => Ok(agent::serve_stdio().await?),
    }
}

/// Asks the running server, started if needed, for a pairing code.
async fn pair() -> Result<(), Error> {
    let mut stream = ipc::connect_or_spawn(&std::env::current_exe()?).await?;
    let request = Request {
        version: VERSION.to_owned(),
        op: Op::Pair,
    };
    frame::send(&mut stream, FrameKind::Request, &request).await?;
    let response: Response = frame::read(&mut stream)
        .await?
        .ok_or(Error::Disconnected)?
        .expect(FrameKind::Response)?;
    match response {
        Response::Pair { addr, code } => {
            println!("On the host, within 10 minutes, run:\n\n    ovrd connect {addr} {code}\n");
            Ok(())
        }
        Response::Failed { message } => Err(Error::Host(message)),
        Response::Restart => Err(Error::Host("ovrd was restarting; try again".to_owned())),
        other => Err(Error::Host(format!("unexpected response: {other:?}"))),
    }
}
