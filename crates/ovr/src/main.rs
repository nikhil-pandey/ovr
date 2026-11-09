use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{CommandFactory, Parser};

fn main() {
    let cli = match ovr::Cli::try_parse() {
        Ok(cli) => cli,
        Err(mut err) => {
            let code = if err.use_stderr() { ovr::FAILURE } else { 0 };
            let first = std::env::args().nth(1);
            let subcommand = first
                .as_deref()
                .is_some_and(|arg| ovr::Cli::command().find_subcommand(arg).is_some());
            let suggestion = match err.kind() {
                ErrorKind::UnknownArgument if !subcommand => {
                    Some("put `--` before the remote command: `ovr HOST -- CMD...`")
                }
                ErrorKind::MissingRequiredArgument if first.as_deref() == Some("get") => Some(
                    "the last path is where files go on this machine: `ovr get HOST SRC... DEST`, \
                     as in `ovr get box target/x.log .`",
                ),
                _ => None,
            };
            if let Some(suggestion) = suggestion {
                err.insert(
                    ContextKind::Suggested,
                    ContextValue::StyledStrs(vec![suggestion.into()]),
                );
            }
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
