use std::ffi::OsString;

use clap::{Args, Parser, Subcommand};

/// Exit code for failures in `ovr` itself, following `docker run`.
pub const FAILURE: u8 = 125;

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
    pub cmd: Vec<OsString>,
}

#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum Error {
    #[error("`{0}` is not implemented yet")]
    #[diagnostic(code(ovr::not_implemented))]
    NotImplemented(&'static str),
}

/// Runs the parsed command and returns the exit code to report.
///
/// # Errors
///
/// Returns an error when `ovr` itself fails, as opposed to the remote command.
pub fn run(cli: &Cli) -> Result<u8, Error> {
    match cli.command {
        Some(Command::Hosts) => Err(Error::NotImplemented("ovr hosts")),
        None => Err(Error::NotImplemented("ovr HOST")),
    }
}
