use clap::Parser;

fn main() {
    let cli = match ovr::Cli::try_parse() {
        Ok(cli) => cli,
        Err(err) => {
            let code = if err.use_stderr() { ovr::FAILURE } else { 0 };
            let _ = err.print();
            std::process::exit(code);
        }
    };

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
    // Exit without waiting for the stdin thread, which may be blocked in a read.
    std::process::exit(code);
}
