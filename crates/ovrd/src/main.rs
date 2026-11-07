use std::process::ExitCode;

use clap::Parser;
use ovrd::{Cli, Command};

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let service = matches!(cli.command, Some(Command::Connect { service: true, .. }));
    if let Err(err) = logging(service) {
        eprintln!("cannot set up logging: {err}");
        return ExitCode::FAILURE;
    }
    match ovrd::run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(%err);
            eprintln!("{:?}", miette::Report::new(err));
            ExitCode::FAILURE
        }
    }
}

/// Logs go to stderr, never stdout, which carries the protocol in agent mode. A service started at login logs to
/// `connect.log` in the log directory instead and, on Windows, gives up its console window.
fn logging(service: bool) -> std::io::Result<()> {
    let builder = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO);
    if service {
        let log = std::fs::File::options()
            .create(true)
            .append(true)
            .open(ovr_proto::paths::log_dir()?.join("connect.log"))?;
        builder
            .with_ansi(false)
            .with_writer(std::sync::Mutex::new(log))
            .init();
        #[cfg(windows)]
        console::detach();
    } else {
        builder.with_writer(std::io::stderr).init();
    }
    Ok(())
}

#[cfg(windows)]
#[allow(unsafe_code)]
mod console {
    pub fn detach() {
        // SAFETY: FreeConsole has no preconditions; failure only means there was no console.
        unsafe { windows_sys::Win32::System::Console::FreeConsole() };
    }
}
