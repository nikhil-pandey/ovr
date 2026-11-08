//! `ovr`: run a command on another machine against the local working tree.

mod daemon;
mod session;
mod snapshot;
mod terminal;

use std::io;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Args, Parser, Subcommand};
use ovr_proto::frame::Resumed;
use ovr_proto::ipc::ClientStream;
use ovr_proto::msg::{Attach, Cleaned, Exec, Op, Resize, Response, RunInfo, RunState};
use ovr_proto::{Frame, FrameKind};

use crate::session::{End, Io, Mode, Position, Signal, Stream};
use crate::snapshot::Snapshot;

/// Exit code for failures in `ovr` itself, following `docker run`.
pub const FAILURE: i32 = 125;

/// Longest wait between attempts to reach a host again.
const MAX_BACKOFF: Duration = Duration::from_secs(10);

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
    /// List this initiator's runs: running, and finished in the last 7 days.
    Ps {
        /// Only this host; otherwise every host with an open connection.
        host: Option<String>,
    },
    /// Reconnect this terminal to a run, as if it had been started here.
    Attach { host: String, run: u64 },
    /// Print a run's output, following it until the run ends.
    Logs { host: String, run: u64 },
    /// Kill a run's whole process tree.
    Kill { host: String, run: u64 },
    /// Remove checkouts and finished runs, keeping those in use, and agents of other versions.
    Clean {
        /// Remove this initiator's from these hosts; otherwise everyone's from this machine.
        hosts: Vec<String>,
        /// This machine, and this initiator's from every host with an open connection.
        #[arg(long, conflicts_with = "hosts")]
        all: bool,
    },
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Start the command in the background and print its run number.
    #[arg(short = 'd')]
    pub detach: bool,

    /// SSH destination or dialed-in host name.
    #[arg(required = true)]
    pub host: Option<String>,

    /// Command to run, after `--`; opens a shell when omitted.
    #[arg(last = true)]
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

    #[error(transparent)]
    #[diagnostic(code(ovr::git))]
    Git(#[from] ovr_proto::git::Error),

    #[error("cannot reach ovrd")]
    #[diagnostic(code(ovr::daemon))]
    Daemon {
        #[source]
        source: io::Error,
        #[help]
        help: String,
    },

    #[error("{0}")]
    #[diagnostic(code(ovr::host))]
    Host(String),

    #[error("remote: {0}")]
    #[diagnostic(code(ovr::remote))]
    Remote(String),

    #[error("connection to the host was lost")]
    #[diagnostic(code(ovr::disconnected))]
    Disconnected,

    #[error("left run {run} running on {host}")]
    #[diagnostic(code(ovr::detached), help("resume it with `ovr attach {host} {run}`"))]
    Detached { host: String, run: u64 },

    #[error("cannot write the output; the run was killed")]
    #[diagnostic(code(ovr::output))]
    Output(#[source] io::Error),

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
        (Some(Command::Ps { host }), _) => ps(host).await,
        (Some(Command::Attach { host, run }), _) => attach(host, run, Mode::Drive).await,
        (Some(Command::Logs { host, run }), _) => attach(host, run, Mode::Watch).await,
        (Some(Command::Kill { host, run }), _) => {
            let (stream, _) = daemon::open(&host).await?;
            session::kill(stream, run).await?;
            Ok(0)
        }
        (Some(Command::Clean { hosts, all }), _) => clean(hosts, all).await,
        (None, Some(host)) => run_on(host, cli.run.cmd, cli.run.detach).await,
        (None, None) => Err(Error::Host("no host given".to_owned())),
    }
}

#[tracing::instrument(level = "debug", skip_all, fields(%host))]
async fn run_on(host: String, argv: Vec<String>, detach: bool) -> Result<i32, Error> {
    let started = Instant::now();
    let cwd = std::env::current_dir()?;
    let open = async {
        let opened = daemon::open(&host).await;
        if opened.is_ok() {
            tracing::debug!(elapsed = ?started.elapsed(), "opened the host stream");
        }
        opened
    };
    let (opened, snapshot) = tokio::join!(open, Snapshot::take(&cwd));
    let snapshot = snapshot?;
    let (stream, initiator) = opened?;
    let delivery = snapshot.deliver_to(&host, &initiator).await?;

    let tty = terminal::is_interactive();
    let (cols, rows) = terminal::size();
    let exec = Exec {
        argv,
        cwd: snapshot.prefix.clone(),
        tty,
        cols,
        rows,
    };
    let (mut stream, position) =
        session::start(stream, &delivery, &exec, &mut tokio::io::stderr()).await?;
    tracing::debug!(
        elapsed = ?started.elapsed(),
        %host,
        checkout = %delivery.checkout,
        run = position.run,
        "attached"
    );
    if detach {
        // Nothing will ever feed a background command's pipe; a terminal waits for whoever attaches.
        if !tty {
            stream.send(Frame::empty(FrameKind::In)).await?;
        }
        println!("{}", position.run);
        return Ok(0);
    }
    let (_terminal, stdin, signals) = terminal::start(tty, true)?;
    let io = Io {
        stdin,
        signals,
        stdout: tokio::io::stdout(),
        stderr: tokio::io::stderr(),
    };
    follow(&host, stream, position, io, Mode::Drive).await
}

async fn attach(host: String, run: u64, mode: Mode) -> Result<i32, Error> {
    let (stream, _) = daemon::open(&host).await?;
    let from_start = Attach {
        run,
        out: 0,
        err: 0,
    };
    let (mut stream, attached) = session::attach(stream, from_start).await?;
    let position = Position::new(attached);
    let drive = mode == Mode::Drive;
    let tty = position.tty && drive && terminal::is_interactive();
    let (_terminal, stdin, signals) = terminal::start(tty, drive)?;
    if tty {
        send_size(&mut stream).await?;
    }
    let io = Io {
        stdin,
        signals,
        stdout: tokio::io::stdout(),
        stderr: tokio::io::stderr(),
    };
    follow(&host, stream, position, io, mode).await
}

type HostStream = Stream<Resumed<ClientStream>>;

/// Relays the run until it ends, resuming it on a new stream whenever one breaks.
async fn follow<O, E>(
    host: &str,
    mut stream: HostStream,
    mut position: Position,
    mut io: Io<O, E>,
    mode: Mode,
) -> Result<i32, Error>
where
    O: tokio::io::AsyncWrite + Unpin,
    E: tokio::io::AsyncWrite + Unpin,
{
    loop {
        match session::relay(&mut stream, &mut io, &mut position, mode).await {
            Ok(End::Exit(code)) => return Ok(code),
            Ok(End::Detached) => return Err(detached(host, &position)),
            Err(Error::Disconnected) => {}
            Err(err) => return Err(err),
        }
        // Raw mode needs the carriage return.
        eprint!(
            "\r\novr: lost the connection to {host}; reconnecting (Ctrl-C leaves run {} running)\r\n",
            position.run
        );
        stream = reconnect(host, &mut position, &mut io, mode).await?;
        eprint!("ovr: reconnected\r\n");
    }
}

/// Attaches again where `position` left off, retrying with backoff until the host answers. Input typed meanwhile is
/// dropped; Ctrl-C leaves the run running.
async fn reconnect<O, E>(
    host: &str,
    position: &mut Position,
    io: &mut Io<O, E>,
    mode: Mode,
) -> Result<HostStream, Error> {
    let mut backoff = Duration::from_secs(1);
    loop {
        let resume = position.resume();
        let attempt = async {
            let (stream, _) = daemon::open(host).await?;
            session::attach(stream, resume).await
        };
        tokio::pin!(attempt);
        let result = loop {
            tokio::select! {
                result = &mut attempt => break result,
                leave = waiting(io) => if leave {
                    return Err(detached(host, position));
                },
            }
        };
        match result {
            Ok((mut stream, attached)) => {
                let dropped = position.resumed(attached);
                if dropped > 0 {
                    eprint!("ovr: {dropped} bytes of output were dropped meanwhile\r\n");
                }
                if position.tty && mode == Mode::Drive && terminal::is_interactive() {
                    send_size(&mut stream).await?;
                }
                return Ok(stream);
            }
            Err(err @ Error::Remote(_)) => return Err(err),
            Err(_) => {}
        }
        let pause = tokio::time::sleep(backoff);
        tokio::pin!(pause);
        loop {
            tokio::select! {
                () = &mut pause => break,
                leave = waiting(io) => if leave {
                    return Err(detached(host, position));
                },
            }
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Discards the next input while disconnected; `true` when the user wants to leave instead.
async fn waiting<O, E>(io: &mut Io<O, E>) -> bool {
    tokio::select! {
        Some(signal) = io.signals.recv() => matches!(signal, Signal::Interrupt | Signal::Detach),
        Some(_) = io.stdin.recv() => false,
        else => std::future::pending().await,
    }
}

async fn send_size(stream: &mut HostStream) -> Result<(), Error> {
    let (cols, rows) = terminal::size();
    stream
        .send(Frame::json(FrameKind::Resize, &Resize { cols, rows })?)
        .await
}

fn detached(host: &str, position: &Position) -> Error {
    Error::Detached {
        host: host.to_owned(),
        run: position.run,
    }
}

async fn ps(host: Option<String>) -> Result<i32, Error> {
    let explicit = host.is_some();
    let hosts = match host {
        Some(host) => vec![host],
        None => daemon::connected().await?,
    };
    if hosts.is_empty() {
        eprintln!("no host is connected; name one with `ovr ps HOST`");
        return Ok(0);
    }
    let mut rows = Vec::new();
    let mut code = 0;
    for host in hosts {
        let listed = async {
            let (stream, initiator) = daemon::open(&host).await?;
            session::list(stream, &initiator).await
        };
        match listed.await {
            Ok(runs) => rows.extend(runs.into_iter().map(|run| (host.clone(), run))),
            Err(err) if explicit => return Err(err),
            Err(err) => {
                eprintln!("ovr: {host}: {err}");
                code = FAILURE;
            }
        }
    }
    print_runs(&rows);
    Ok(code)
}

fn print_runs(rows: &[(String, RunInfo)]) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |time| time.as_secs());
    let table: Vec<[String; 6]> = rows
        .iter()
        .map(|(host, run)| {
            let state = match &run.state {
                RunState::Running => "running".to_owned(),
                RunState::Exited { code } => format!("exited {code}"),
                RunState::Failed { .. } => "failed".to_owned(),
                RunState::Lost => "lost".to_owned(),
            };
            let command = if run.argv.is_empty() {
                "(shell)".to_owned()
            } else {
                run.argv.join(" ")
            };
            [
                host.clone(),
                run.run.to_string(),
                state,
                age(now.saturating_sub(run.started)),
                run.checkout.clone(),
                command,
            ]
        })
        .collect();
    let header = ["HOST", "RUN", "STATE", "AGE", "CHECKOUT", "COMMAND"].map(str::to_owned);
    let mut widths = header.clone().map(|cell| cell.len());
    for row in &table {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.len());
        }
    }
    for row in std::iter::once(&header).chain(&table) {
        let line: Vec<String> = row
            .iter()
            .zip(widths)
            .map(|(cell, width)| format!("{cell:width$}"))
            .collect();
        println!("{}", line.join("  ").trim_end());
    }
}

fn age(seconds: u64) -> String {
    match seconds {
        0..60 => format!("{seconds}s"),
        60..3600 => format!("{}m", seconds / 60),
        3600..86400 => format!("{}h", seconds / 3600),
        _ => format!("{}d", seconds / 86400),
    }
}

async fn clean(hosts: Vec<String>, all: bool) -> Result<i32, Error> {
    let mut code = 0;
    if all || hosts.is_empty() {
        match daemon::request(Op::Clean).await? {
            (_, Response::Cleaned(cleaned)) => report("this machine", &cleaned),
            (_, other) => {
                eprintln!("ovr: this machine: {}", daemon::unexpected(other));
                code = FAILURE;
            }
        }
    }
    let hosts = if all {
        daemon::connected().await?
    } else {
        hosts
    };
    let cwd = std::env::current_dir()?;
    for host in hosts {
        let cleaned = async {
            let (stream, initiator) = daemon::open(&host).await?;
            session::clean(stream, &initiator).await
        };
        match cleaned.await {
            Ok(cleaned) => {
                snapshot::forget_host(&cwd, &host).await;
                report(&host, &cleaned);
            }
            Err(err) => {
                eprintln!("ovr: {host}: {err}");
                code = FAILURE;
            }
        }
    }
    Ok(code)
}

fn report(target: &str, cleaned: &Cleaned) {
    let count = |n: u64, what: &str| format!("{n} {what}{}", if n == 1 { "" } else { "s" });
    println!(
        "{target}: removed {}, {}, {}; freed {}",
        count(cleaned.checkouts, "checkout"),
        count(cleaned.runs, "run"),
        count(cleaned.agents, "agent"),
        bytes(cleaned.bytes)
    );
    for checkout in &cleaned.kept {
        println!("{target}: kept {checkout}: in use");
    }
}

#[allow(clippy::cast_precision_loss)]
fn bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

async fn hosts() -> Result<i32, Error> {
    let hosts = match daemon::request(Op::Hosts).await? {
        (_, Response::Hosts { hosts }) => hosts,
        (_, other) => return Err(daemon::unexpected(other)),
    };
    if hosts.is_empty() {
        eprintln!("no dialed-in hosts; run `ovrd pair` to add one");
    }
    let width = hosts.iter().map(|host| host.name.len()).max().unwrap_or(0);
    for host in hosts {
        match (host.platform, host.about) {
            (Some(platform), about) => println!(
                "{:width$}  online   {}/{}{}",
                host.name,
                platform.os,
                platform.arch,
                about.map(|about| format!("  {about}")).unwrap_or_default()
            ),
            (None, _) => println!("{:width$}  offline", host.name),
        }
    }
    Ok(0)
}
