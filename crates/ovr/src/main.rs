use std::process::ExitCode;

use clap::Parser;

fn main() -> ExitCode {
    let cli = match ovr::Cli::try_parse() {
        Ok(cli) => cli,
        Err(err) => {
            let code = if err.use_stderr() { ovr::FAILURE } else { 0 };
            let _ = err.print();
            return ExitCode::from(code);
        }
    };

    match ovr::run(&cli) {
        Ok(code) => ExitCode::from(code),
        Err(err) => {
            eprintln!("{:?}", miette::Report::new(err));
            ExitCode::from(ovr::FAILURE)
        }
    }
}
