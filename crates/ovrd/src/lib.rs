use clap::{Parser, Subcommand};

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
    /// Pair with the laptop at ADDR and keep serving it.
    Connect {
        /// Address printed by `ovrd pair`.
        addr: String,
        /// Pairing code printed by `ovrd pair`.
        code: String,
    },
    /// Serve one run over stdin and stdout; started by `ovrd` over SSH.
    #[command(hide = true)]
    Agent,
}

#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum Error {
    #[error("`{0}` is not implemented yet")]
    #[diagnostic(code(ovrd::not_implemented))]
    NotImplemented(&'static str),
}

/// Runs the parsed command until it finishes.
///
/// # Errors
///
/// Returns an error when the server or agent fails.
#[allow(clippy::unused_async)]
pub async fn run(cli: Cli) -> Result<(), Error> {
    match cli.command {
        None => Err(Error::NotImplemented("ovrd")),
        Some(Command::Pair) => Err(Error::NotImplemented("ovrd pair")),
        Some(Command::Connect { .. }) => Err(Error::NotImplemented("ovrd connect")),
        Some(Command::Agent) => Err(Error::NotImplemented("ovrd agent")),
    }
}
