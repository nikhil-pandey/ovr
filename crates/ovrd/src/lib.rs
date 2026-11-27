//! `ovrd`: the host side of `ovr`, and the initiator server `ovr` starts.

pub mod agent;
mod autostart;
mod host;
#[cfg(windows)]
mod job;
mod mux;
mod pairing;
mod server;
mod state;
mod status;
mod target;
mod tls;
mod trust;
mod tunnel;

use std::io;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

const EXAMPLES: &str = "\
Examples:
  ovrd connect 100.64.0.10:7420 7KQX-M4RT      pair with the initiator, then serve it, at login too
  ovrd connect devtunnel:quiet-river-x1y2z3    move this paired host to a new address
  ovrd status                                  whether this host is paired and connected, and its runs
  ovrd clean                                   remove checkouts and finished runs not in use

On your own machine, use `ovr`: it starts the initiator server, and `ovr pair` prints the connect command.";

const CONNECT_HELP: &str = "\
Examples:
  ovrd connect 100.64.0.10:7420 7KQX-M4RT                pair, then serve, at login too
  ovrd connect devtunnel:quiet-river-x1y2z3              move to a new address of the same initiator
  ovrd connect                                           serve the paired initiator
  ovrd connect cloudflare:NAME 7KQX-M4RT --foreground    serve until stopped, installing nothing
  ovrd connect ADDR github:FINGERPRINT --name windows    in a GitHub Actions job, in the background

ADDR and CODE come from `ovr pair` on the initiator, `github:FINGERPRINT` from `ovr trust`.";

/// Serve `ovr` runs on this host, and the initiator server `ovr` starts.
#[derive(Debug, Parser)]
#[command(name = "ovrd", version, arg_required_else_help = true, after_help = EXAMPLES)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Pair with an initiator and serve it, at every login too.
    ///
    /// With a pairing code, pair with the initiator at ADDR, then keep serving it, at every login too. Without a
    /// code, move a paired host to ADDR, a new address of the same initiator. Without arguments, serve the paired
    /// initiator.
    #[command(after_help = CONNECT_HELP)]
    Connect {
        /// `host:port`, `cloudflare:HOSTNAME`, `devtunnel:ID`, or `tailcat:ADDRESS`, as printed by `ovr pair` or `ovr tunnel`.
        addr: Option<tunnel::Addr>,
        /// Pairing code printed by `ovr pair`, or `github:FINGERPRINT` printed by `ovr trust` to serve as this
        /// GitHub Actions job, in the background until the job ends.
        #[arg(requires = "addr")]
        code: Option<String>,
        /// With `github:FINGERPRINT`, tell this job's host apart from others of the same job, such as a matrix's:
        /// it is named `OWNER/REPO/WORKFLOW/RUN/JOB/NAME`.
        #[arg(long, requires = "code")]
        name: Option<String>,
        /// Serve in the foreground and install nothing to start at login, as on a CI runner.
        #[arg(long, conflicts_with = "service")]
        foreground: bool,
        /// Started at login: log to a file and detach from the console.
        #[arg(long, hide = true, conflicts_with = "addr")]
        service: bool,
    },
    /// Show this host's state: its initiator, connection, login service, and runs.
    ///
    /// Reads this host's own files only: it contacts no initiator and starts nothing.
    #[command(after_help = "Example:\n  ovrd status    on a host, before looking in connect.log")]
    Status,
    /// Remove every initiator's checkouts and finished runs on this machine, keeping those in use.
    ///
    /// Also removes agents of other versions uploaded over SSH. Checkouts being synced, and runs still running or
    /// followed by a client, stay with their checkouts.
    #[command(after_help = "Example:\n  ovrd clean    on a host, to free its disk")]
    Clean,
    /// Run the initiator server; started by `ovr`.
    #[command(hide = true)]
    Serve,
    /// Serve runs over stdin and stdout; started by the initiator server over SSH.
    #[command(hide = true)]
    Agent,
    /// Keep the run in DIR: own its command and log, and serve attached clients; started by the agent.
    #[command(hide = true)]
    Keep { dir: std::path::PathBuf },
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
        Command::Serve => until_stopped(server::run()).await,
        Command::Connect {
            addr,
            code,
            name,
            foreground,
            ..
        } => {
            until_stopped(host::connect(
                addr,
                code.as_deref(),
                name.as_deref(),
                foreground,
            ))
            .await
        }
        Command::Status => status::status(),
        Command::Clean => clean().await,
        Command::Agent => Ok(agent::serve_stdio().await?),
        Command::Keep { dir } => agent::keeper::main(dir).await,
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

/// `ovrd clean`: removes every initiator's checkouts and finished runs here, without the initiator server.
async fn clean() -> Result<(), Error> {
    let config = agent::Config::detect()?;
    let cleaned = tokio::task::spawn_blocking(move || agent::clean::clean(&config, None))
        .await
        .map_err(io::Error::from)??;
    println!("{cleaned}");
    for checkout in &cleaned.kept {
        println!("kept {checkout}: in use");
    }
    Ok(())
}
