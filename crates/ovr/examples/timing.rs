//! `ovr` with its timing events on stderr, to see where a run's startup goes:
//!
//! ```sh
//! cargo run --release -p ovr --example timing -- HOST -- CMD... 2> timings.log
//! ```
//!
//! It finds `ovrd` on `PATH` rather than beside itself, so put one of the same version there first.

use clap::Parser;

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    let cli = ovr::Cli::parse();
    let code = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime.block_on(ovr::run(cli)).unwrap_or_else(|err| {
            eprintln!("{:?}", miette::Report::new(err));
            ovr::FAILURE
        }),
        Err(err) => {
            eprintln!("ovr: {err}");
            ovr::FAILURE
        }
    };
    std::process::exit(code);
}
