use std::process::ExitCode;

use clap::Parser;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = ovrd::Cli::parse();

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    match ovrd::run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{:?}", miette::Report::new(err));
            ExitCode::FAILURE
        }
    }
}
