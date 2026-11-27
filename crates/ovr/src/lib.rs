//! `ovr`: run a command on another machine against the local working tree.

mod daemon;
mod fetch;
mod manage;
mod session;
mod snapshot;
mod terminal;

use std::io;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Args, Parser, Subcommand};
use ovr_proto::frame::Resumed;
use ovr_proto::ipc::ClientStream;
use ovr_proto::msg::{Attach, Enter, Exec, Op, Provider, Resize, Response, RunInfo};
use ovr_proto::{Frame, FrameKind, print_field};

use crate::session::{End, Io, Mode, Position, Signal, Stream};
use crate::snapshot::{Repository, Snapshot};

/// Exit code for failures in `ovr` itself, following `docker run`.
pub const FAILURE: i32 = 125;

const GET_HELP: &str = "\
Examples:
  ovr get bigbox target/nextest/ci/junit.xml .       one file into the current directory
  ovr get bigbox target/doc out/                      a tree: out/doc/...
  ovr get bigbox target/release/app ./app-linux       one file, renamed
  ovr get bigbox 'bazel-testlogs/**/test.xml' logs/   every match, keeping its path below bazel-testlogs/
  ovr get bigbox '~/.cargo/config.toml' .             ~ is the host user's home; quote it, and globs
  ovr get winbox 'C:\\ProgramData\\Foo\\crash.dmp' .     anywhere the host user can read

Each SRC lands as DEST/<name of SRC>, so the same command always lands it in the same place. A single
file can be renamed instead: with one SRC that is a file, and a DEST that is not a directory and does
not end in /, SRC is copied as DEST. Relative SRCs start from this repository's checkout on HOST,
in the same directory as here; outside a repository, or before the first sync, from the host's base
directory: its home, or a trusted CI job's workspace. get never syncs: run what writes the files first.

Files whose size and modification time match the copy in DEST are skipped, so the same command again
fetches only what changed, and resumes one that was cut short. .gitignore does not apply.

Exits 0 when everything was fetched, 1 when something could not be, and 125 when ovr itself fails.";

const EXAMPLES: &str = "\
Examples:
  ovr bigbox -- cargo test                  sync this working tree to bigbox, run there, exit with its code
  ovr bigbox                                a login shell in the synced checkout
  ovr --detach bigbox -- cargo build        in the background; prints the run number for logs, attach, and kill
  ovr --no-sync bigbox -- cat Cargo.lock    without syncing: in the checkout as bigbox has it
  ovr get bigbox target/doc out/            copy files back: out/doc/..., skipping what is unchanged
  ovr ps                                    runs on every connected host

Commands run in the same directory relative to the repository root as here. Outside a Git repository
nothing is synced, and commands run in the host's base directory: its home, or a trusted CI job's workspace.
HOST is an SSH destination, or a host that dialed in: see `ovr pair` and `ovr trust`.";

const HOSTS_HELP: &str = "\
Example:
  ovr hosts    NAME, STATE, and PLATFORM of each paired host, and of trusted hosts while connected";

const PS_HELP: &str = "\
Examples:
  ovr ps           runs on every host with an open connection
  ovr ps bigbox    runs on bigbox, connecting to it if needed";

const ATTACH_HELP: &str = "\
Example:
  ovr attach bigbox 12    take run 12's terminal back, as if it had been started here";

const LOGS_HELP: &str = "\
Example:
  ovr logs bigbox 12 && deploy    follow run 12's output, and exit with its code";

const KILL_HELP: &str = "\
Example:
  ovr kill bigbox 12    kill run 12 and every process it started";

const CLEAN_HELP: &str = "\
Examples:
  ovr clean bigbox winbox    this initiator's checkouts and finished runs on bigbox and winbox
  ovr clean                  agents downloaded here for other versions
  ovr clean --all            both, on every host with an open connection

On a host, `ovrd clean` removes every initiator's checkouts and finished runs.";

const STATUS_HELP: &str = "\
Example:
  ovr status    this initiator's fingerprint, whether hosts can dial in, and where its files are

On a host, `ovrd status` shows whether it is paired and connected, and the runs there.";

const PAIR_HELP: &str = "\
Examples:
  ovr pair               prints `ovrd connect ADDR CODE` to run on the host within 10 minutes
  ovr pair cloudflare    the same, through a Cloudflare quick tunnel; or devtunnel, or tailcat";

const UNPAIR_HELP: &str = "\
Example:
  ovr unpair winbox    winbox is refused from now on; pair it again with `ovr pair`";

const TRUST_HELP: &str = "\
Examples:
  ovr trust github:nikhil-pandey/ovr    trust the jobs of one repository
  ovr trust github:nikhil-pandey       trust the jobs of every repository of an owner
  ovr trust                            list the rules, and how a job dials in";

const TUNNEL_HELP: &str = "\
Examples:
  ovr tunnel                     open tunnels, and the command that moves a paired host to each
  ovr tunnel close cloudflare    close a tunnel and delete what was created for it";

/// Longest wait between attempts to reach a host again.
const MAX_BACKOFF: Duration = Duration::from_secs(10);

/// Run a command on another machine against your local working tree.
#[derive(Debug, Parser)]
#[command(
    name = "ovr",
    version,
    arg_required_else_help = true,
    args_conflicts_with_subcommands = true,
    subcommand_negates_reqs = true,
    after_help = EXAMPLES
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    #[command(flatten)]
    pub run: RunArgs,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// List paired hosts, and trusted hosts while connected.
    #[command(after_help = HOSTS_HELP)]
    Hosts,
    /// List this initiator's runs: running, and finished in the last 7 days.
    #[command(after_help = PS_HELP)]
    Ps {
        /// SSH destination or dialed-in host name; otherwise every host with an open connection.
        host: Option<String>,
    },
    /// Reconnect this terminal to a run, as if it had been started here.
    #[command(after_help = ATTACH_HELP)]
    Attach {
        /// SSH destination or dialed-in host name.
        host: String,
        /// Run number, as `ovr -d` prints and `ovr ps` lists.
        run: u64,
    },
    /// Print a run's output, following it until the run ends.
    #[command(after_help = LOGS_HELP)]
    Logs {
        /// SSH destination or dialed-in host name.
        host: String,
        /// Run number, as `ovr -d` prints and `ovr ps` lists.
        run: u64,
    },
    /// Kill a run's whole process tree.
    #[command(after_help = KILL_HELP)]
    Kill {
        /// SSH destination or dialed-in host name.
        host: String,
        /// Run number, as `ovr -d` prints and `ovr ps` lists.
        run: u64,
    },
    /// Copy files and trees from HOST to DEST on this machine, skipping files already there.
    #[command(after_help = GET_HELP)]
    Get {
        /// SSH destination or dialed-in host name.
        host: String,
        /// Files, trees, or quoted globs on HOST. Relative ones start from this repository's checkout there.
        #[arg(required = true, value_name = "SRC")]
        sources: Vec<String>,
        /// Where they go on this machine.
        dest: String,
    },
    /// Remove this initiator's checkouts and finished runs from hosts, keeping those in use.
    ///
    /// Without HOST, remove what this machine keeps as an initiator: agents downloaded for other versions.
    #[command(after_help = CLEAN_HELP)]
    Clean {
        /// SSH destination or dialed-in host name.
        #[arg(value_name = "HOST")]
        hosts: Vec<String>,
        /// Clean this machine, and yours on every host with an open connection.
        #[arg(long, conflicts_with = "hosts")]
        all: bool,
    },
    /// Print a one-shot command for a host to dial in and pair.
    ///
    /// The host runs the printed `ovrd connect ADDR CODE` within 10 minutes. With PROVIDER, ADDR is a tunnel from
    /// that provider, opened unless one is open; it stays open until `ovr tunnel close PROVIDER`.
    #[command(after_help = PAIR_HELP)]
    Pair {
        /// `cloudflare`, `devtunnel` or `tailcat`: open a tunnel with that provider's CLI unless one is open.
        provider: Option<Provider>,
    },
    /// Forget a paired host and close its connection; runs already there keep going.
    #[command(after_help = UNPAIR_HELP)]
    Unpair {
        /// Paired host name, as `ovr hosts` lists it.
        host: String,
    },
    /// Trust workloads matching RULE to dial in without pairing; without RULE, list the rules.
    ///
    /// Trusted workloads, such as the GitHub Actions jobs of a repository, dial in with a token from their identity
    /// provider instead of a pairing code. Either way, print what a trusted workload needs to dial in.
    #[command(after_help = TRUST_HELP)]
    Trust {
        /// `github:OWNER/REPO`, or `github:OWNER` for every repository of OWNER.
        rule: Option<String>,
    },
    /// Stop trusting workloads matching RULE.
    Untrust {
        /// A rule as `ovr trust` lists it.
        rule: String,
    },
    /// List open tunnels and the command that points a paired host at each.
    #[command(after_help = TUNNEL_HELP)]
    Tunnel {
        #[command(subcommand)]
        command: Option<TunnelCommand>,
    },
    /// Show this initiator's state: identity, dial-in address, tunnels, hosts, and files.
    #[command(after_help = STATUS_HELP)]
    Status,
}

#[derive(Debug, Subcommand)]
pub enum TunnelCommand {
    /// Close the tunnel `ovr pair PROVIDER` opened, and delete what was created for it at the provider.
    Close {
        /// `cloudflare`, `devtunnel` or `tailcat`.
        provider: Provider,
    },
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Start the command in the background and print its run number.
    #[arg(short = 'd', long)]
    pub detach: bool,

    /// Do not sync: run in the checkout as the host has it, or in the host's base directory when it has none yet.
    #[arg(short = 'n', long)]
    pub no_sync: bool,

    /// SSH destination or dialed-in host name.
    #[arg(required = true)]
    pub host: Option<String>,

    /// Command to run, after `--`; opens a shell when omitted.
    #[arg(last = true)]
    pub cmd: Vec<String>,
}

#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum Error {
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

    #[error("{message}")]
    #[diagnostic(code(ovr::usage), help("{help}"))]
    Usage { message: String, help: String },

    #[error("remote: {0}")]
    #[diagnostic(code(ovr::remote))]
    Remote(String),

    #[error("connection to the host was lost")]
    #[diagnostic(code(ovr::disconnected))]
    Disconnected,

    #[error("the connection to the host was lost")]
    #[diagnostic(
        code(ovr::disconnected),
        help(
            "run the same command again: the files already here are complete, and only the rest is fetched"
        )
    )]
    FetchCut,

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
pub async fn run(mut cli: Cli) -> Result<i32, Error> {
    match (cli.command, cli.run.host.take()) {
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
        (Some(Command::Pair { provider }), _) => manage::pair(provider).await,
        (Some(Command::Unpair { host }), _) => manage::unpair(host).await,
        (Some(Command::Trust { rule }), _) => manage::trust(Op::Trust { rule }).await,
        (Some(Command::Untrust { rule }), _) => manage::trust(Op::Untrust { rule }).await,
        (Some(Command::Tunnel { command: None }), _) => manage::tunnels().await,
        (Some(Command::Status), _) => status().await,
        (
            Some(Command::Tunnel {
                command: Some(TunnelCommand::Close { provider }),
            }),
            _,
        ) => manage::close_tunnel(provider).await,
        (
            Some(Command::Get {
                host,
                sources,
                dest,
            }),
            _,
        ) => fetch::get(host, sources, dest).await,
        (None, Some(host)) => run_on(host, cli.run).await,
        (None, None) => Err(Error::Host("no host given".to_owned())),
    }
}

/// How a run starts: by syncing a snapshot, or without, in this repository's checkout if there is one.
enum Start {
    Sync(Snapshot),
    Enter(Option<Repository>),
}

#[tracing::instrument(level = "debug", skip_all, fields(%host))]
async fn run_on(host: String, run: RunArgs) -> Result<i32, Error> {
    let RunArgs {
        detach,
        no_sync,
        cmd: argv,
        ..
    } = run;
    let started = Instant::now();
    let cwd = std::env::current_dir()?;
    let open = async {
        let opened = daemon::open(&host).await;
        if opened.is_ok() {
            tracing::debug!(elapsed = ?started.elapsed(), "opened the host stream");
        }
        opened
    };
    // Outside a repository there is nothing to sync.
    let prepare = async {
        Ok::<_, Error>(match Repository::locate(&cwd).await? {
            Some(repository) if !no_sync => Start::Sync(Snapshot::take(repository).await?),
            repository => Start::Enter(repository),
        })
    };
    let (opened, start) = tokio::join!(open, prepare);
    let start = start?;
    let (stream, initiator) = opened?;

    let tty = terminal::is_interactive();
    let (cols, rows) = terminal::size();
    let mut exec = Exec {
        argv,
        cwd: String::new(),
        tty,
        cols,
        rows,
    };
    let stderr = &mut tokio::io::stderr();
    let (mut stream, position, checkout) = match start {
        Start::Sync(snapshot) => {
            let delivery = snapshot.deliver_to(&host, &initiator).await?;
            exec.cwd.clone_from(&snapshot.repository.prefix);
            let (stream, position) = session::start(stream, &delivery, &exec, stderr).await?;
            (stream, position, Some(delivery.checkout))
        }
        Start::Enter(repository) => {
            let checkout = repository.as_ref().map(|repo| repo.checkout(&initiator));
            if let Some(repository) = repository {
                exec.cwd = repository.prefix;
            }
            let enter = Enter {
                initiator,
                checkout: checkout.clone(),
            };
            let (stream, position) = session::enter(stream, &enter, &exec, stderr).await?;
            (stream, position, checkout)
        }
    };
    tracing::debug!(
        elapsed = ?started.elapsed(),
        %host,
        checkout = checkout.as_deref().unwrap_or_default(),
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
    let header = ["HOST", "RUN", "STATE", "AGE", "CHECKOUT", "COMMAND"].map(str::to_owned);
    let table: Vec<[String; 6]> = std::iter::once(header)
        .chain(rows.iter().map(|(host, run)| {
            let command = if run.argv.is_empty() {
                "(shell)".to_owned()
            } else {
                run.argv.join(" ")
            };
            [
                host.clone(),
                run.run.to_string(),
                run.state.to_string(),
                ovr_proto::format_age(now.saturating_sub(run.started)),
                run.checkout.clone().unwrap_or_else(|| "-".to_owned()),
                command,
            ]
        }))
        .collect();
    for line in ovr_proto::columns(&table) {
        println!("{line}");
    }
}

async fn clean(hosts: Vec<String>, all: bool) -> Result<i32, Error> {
    let mut code = 0;
    if all || hosts.is_empty() {
        match daemon::request(Op::CleanDownloads).await? {
            (_, Response::DownloadsCleaned { agents, bytes }) => println!(
                "this machine: removed {agents} downloaded agent{}; freed {}",
                if agents == 1 { "" } else { "s" },
                ovr_proto::format_bytes(bytes)
            ),
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
                println!("{host}: {cleaned}");
                for checkout in &cleaned.kept {
                    println!("{host}: kept {checkout}: in use");
                }
            }
            Err(err) => {
                eprintln!("ovr: {host}: {err}");
                code = FAILURE;
            }
        }
    }
    Ok(code)
}

async fn hosts() -> Result<i32, Error> {
    let hosts = match daemon::request(Op::Hosts).await? {
        (_, Response::Hosts { hosts }) => hosts,
        (_, other) => return Err(daemon::unexpected(other)),
    };
    if hosts.is_empty() {
        eprintln!("no dialed-in hosts; run `ovr pair` to add one");
        return Ok(0);
    }
    let width = hosts
        .iter()
        .map(|host| host.name.len())
        .fold("NAME".len(), usize::max);
    println!("{:width$}  STATE    PLATFORM", "NAME");
    for host in hosts {
        match (host.platform, host.about) {
            (Some(platform), about) => println!(
                "{:width$}  online   {}/{}{}",
                host.name,
                platform.os,
                platform.arch,
                about.map_or_default(|about| format!("  {about}"))
            ),
            (None, _) => println!("{:width$}  offline  -", host.name),
        }
    }
    Ok(0)
}

/// `ovr status`: reports the initiator's state as the local `ovrd` has it, checking nothing.
async fn status() -> Result<i32, Error> {
    let (_, response) = daemon::request(Op::Status).await?;
    let Response::Status {
        fingerprint,
        listening,
        tunnels,
        rules,
        paired,
        online,
        trusted_online,
        ssh_connected,
        paths,
    } = response
    else {
        return Err(daemon::unexpected(response));
    };
    println!("ovr {}", ovr_proto::VERSION);
    print_field("initiator", [fingerprint]);
    print_field(
        "listening",
        [listening.unwrap_or_else(|| {
            if paired == 0 && rules.is_empty() && tunnels.is_empty() {
                "not listening (nothing paired or trusted)".to_owned()
            } else {
                "not listening".to_owned()
            }
        })],
    );
    print_field(
        "tunnels",
        tunnels.into_iter().map(|tunnel| {
            let addr = tunnel.addr.unwrap_or_else(|| "(restarting)".to_owned());
            format!("{:<10}  {addr}", tunnel.provider)
        }),
    );
    print_field("trusted", rules);
    print_field(
        "hosts",
        [format!(
            "{paired} paired ({online} online), {trusted_online} trusted online, {ssh_connected} ssh connected"
        )],
    );
    for location in paths {
        print_field(&location.name, [&location]);
    }
    Ok(0)
}
