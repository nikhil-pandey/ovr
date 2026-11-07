//! `ovr`: run a command on another machine against the local working tree.

pub mod daemon;
pub mod session;
pub mod snapshot;
pub mod terminal;

use std::io;

use clap::{Args, Parser, Subcommand};
use ovr_proto::msg::{Exec, Op, Response};

use crate::snapshot::Snapshot;

/// Exit code for failures in `ovr` itself, following `docker run`.
pub const FAILURE: i32 = 125;

/// Run a command on another machine against your local working tree.
#[derive(Debug, Parser)]
#[command(
    name = "ovr",
    version,
    args_conflicts_with_subcommands = true,
    subcommand_negates_reqs = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    #[command(flatten)]
    pub run: RunArgs,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// List dialed-in hosts.
    Hosts,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// SSH destination or dialed-in host name.
    #[arg(required = true)]
    pub host: Option<String>,

    /// Command to run; opens a shell when omitted.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub cmd: Vec<String>,
}

#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum Error {
    #[error("not inside a Git working tree")]
    #[diagnostic(
        code(ovr::not_a_repository),
        help("ovr syncs the repository around the current directory")
    )]
    NotARepository,

    #[error("git {command} failed: {stderr}")]
    #[diagnostic(code(ovr::git))]
    Git { command: String, stderr: String },

    #[error("cannot reach ovrd")]
    #[diagnostic(
        code(ovr::daemon),
        help("is ovrd installed next to ovr? ovrd.log in ~/.local/state/ovr has details")
    )]
    Daemon(#[source] io::Error),

    #[error("{0}")]
    #[diagnostic(code(ovr::host))]
    Host(String),

    #[error("remote: {0}")]
    #[diagnostic(code(ovr::remote))]
    Remote(String),

    #[error("connection to the host was lost")]
    #[diagnostic(code(ovr::disconnected))]
    Disconnected,

    #[error(transparent)]
    #[diagnostic(code(ovr::protocol))]
    Protocol(#[from] ovr_proto::Error),

    #[error(transparent)]
    #[diagnostic(code(ovr::io))]
    Io(#[from] io::Error),
}

/// Runs the parsed command and returns the exit code to report.
///
/// # Errors
///
/// Returns an error when `ovr` itself fails, as opposed to the remote command.
pub async fn run(cli: Cli) -> Result<i32, Error> {
    match (cli.command, cli.run.host) {
        (Some(Command::Hosts), _) => hosts().await,
        (None, Some(host)) => run_on(host, cli.run.cmd).await,
        (None, None) => Err(Error::Host("no host given".to_owned())),
    }
}

async fn run_on(host: String, argv: Vec<String>) -> Result<i32, Error> {
    let cwd = std::env::current_dir()?;
    let (opened, snapshot) = tokio::join!(
        daemon::request(Op::Open { host: host.clone() }),
        Snapshot::take(&cwd, &host)
    );
    let snapshot = snapshot?;
    let stream = match opened? {
        (stream, Response::Opened) => stream,
        (_, Response::Failed { message }) => return Err(Error::Host(message)),
        (_, other) => {
            return Err(Error::Host(format!(
                "unexpected response from ovrd: {other:?}"
            )));
        }
    };

    let tty = terminal::is_interactive();
    let (cols, rows) = terminal::size();
    let exec = Exec {
        argv,
        cwd: snapshot.prefix.clone(),
        tty,
        cols,
        rows,
    };
    let (_terminal, input) = terminal::start(tty)?;
    let io = session::Io {
        input,
        stdout: tokio::io::stdout(),
        stderr: tokio::io::stderr(),
    };
    session::run(stream, &snapshot, &exec, io).await
}

async fn hosts() -> Result<i32, Error> {
    let hosts = match daemon::request(Op::Hosts).await? {
        (_, Response::Hosts { hosts }) => hosts,
        (_, Response::Failed { message }) => return Err(Error::Host(message)),
        (_, other) => {
            return Err(Error::Host(format!(
                "unexpected response from ovrd: {other:?}"
            )));
        }
    };
    if hosts.is_empty() {
        eprintln!("no dialed-in hosts; run `ovrd pair` to add one");
    }
    let width = hosts.iter().map(|host| host.name.len()).max().unwrap_or(0);
    for host in hosts {
        match host.platform {
            Some(platform) => println!(
                "{:width$}  online   {}/{}",
                host.name, platform.os, platform.arch
            ),
            None => println!("{:width$}  offline", host.name),
        }
    }
    Ok(0)
}
