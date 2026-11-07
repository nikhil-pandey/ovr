//! End to end: the real `ovr` and `ovrd` binaries, with a stand-in `ssh` that runs the agent here as a host with a
//! home directory of its own. Runs are kept by real `ovrd keep` processes, so these tests kill agents and keepers
//! the way an overloaded machine would.

#![cfg(unix)]

use std::ffi::OsString;
use std::io::{BufRead, BufReader, Read};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use ovr_proto::msg::{Op, Request};
use ovr_proto::{Frame, FrameKind, frame};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Everything an XDG-aware tool could read instead of `HOME`.
const XDG: [&str; 5] = [
    "XDG_CACHE_HOME",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_RUNTIME_DIR",
    "XDG_STATE_HOME",
];

/// An initiator and a host on this machine, each with its own home, and a repository to run against.
struct World {
    /// Under `/tmp`, so socket paths stay within the platform limit.
    dir: tempfile::TempDir,
    ovr: PathBuf,
    path: OsString,
}

impl World {
    fn new() -> Result<Self> {
        let (ovr, ovrd) = binaries()?;
        let dir = tempfile::Builder::new().prefix("ovr").tempdir_in("/tmp")?;
        let host = dir.path().join("host");
        let installed = host.join(".cache/ovr/bin");
        std::fs::create_dir_all(&installed)?;
        std::fs::copy(
            &ovrd,
            installed.join(format!("ovrd-{}", env!("CARGO_PKG_VERSION"))),
        )?;

        let bin = dir.path().join("bin");
        std::fs::create_dir_all(&bin)?;
        let ssh = bin.join("ssh");
        std::fs::write(
            &ssh,
            format!(
                "#!/bin/sh\n\
                 while [ \"$1\" != \"--\" ]; do shift; done\n\
                 shift 2\n\
                 unset {xdg}\n\
                 export HOME='{host}'\n\
                 cd \"$HOME\" || exit 1\n\
                 echo $$ > agent.pid\n\
                 exec /bin/sh -c \"$*\"\n",
                xdg = XDG.join(" "),
                host = host.display(),
            ),
        )?;
        std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o755))?;

        let repo = dir.path().join("repo");
        std::fs::create_dir_all(&repo)?;
        git(&repo, &["init", "-q"])?;
        std::fs::write(repo.join("msg.txt"), "hello\n")?;

        let ovrd_dir = ovrd.parent().ok_or("ovrd has no directory")?;
        let inherited = std::env::var_os("PATH").unwrap_or_default();
        let path = std::env::join_paths(
            [bin.as_path(), ovrd_dir]
                .into_iter()
                .map(Path::to_owned)
                .chain(std::env::split_paths(&inherited)),
        )?;
        Ok(Self { dir, ovr, path })
    }

    fn host(&self) -> PathBuf {
        self.dir.path().join("host")
    }

    fn file(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn ovr(&self, args: &[&str]) -> Command {
        let mut command = Command::new(&self.ovr);
        command
            .args(args)
            .current_dir(self.file("repo"))
            .env("HOME", self.file("initiator"))
            .env("PATH", &self.path)
            .stdin(Stdio::null());
        for var in XDG {
            command.env_remove(var);
        }
        command
    }

    fn run(&self, args: &[&str]) -> Result<Output> {
        Ok(self.ovr(args).output()?)
    }

    /// The host's record of run `run`.
    fn run_dir(&self, run: &str) -> PathBuf {
        #[cfg(target_os = "macos")]
        let runs = self.host().join("Library/Application Support/ovr/runs");
        #[cfg(not(target_os = "macos"))]
        let runs = self.host().join(".local/state/ovr/runs");
        runs.join(run)
    }
}

impl Drop for World {
    /// Asks the initiator's `ovrd` to make way, as a client of another version would, so that it exits.
    fn drop(&mut self) {
        #[cfg(target_os = "macos")]
        let socket = self
            .file("initiator")
            .join("Library/Application Support/ovr/ovrd.sock");
        #[cfg(not(target_os = "macos"))]
        let socket = self.file("initiator").join(".local/state/ovr/ovrd.sock");
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return;
        };
        runtime.block_on(async {
            let Ok(stream) = ovr_proto::ipc::connect_to(&socket).await else {
                return;
            };
            let mut stream = frame::framed(stream);
            let request = Request {
                version: "stop".to_owned(),
                op: Op::Hosts,
            };
            if let Ok(frame) = Frame::json(FrameKind::Request, &request) {
                let _ = stream.send(frame).await;
                let _ = stream.next().await;
            }
        });
    }
}

/// `ovr` and `ovrd`: from Bazel's runfiles, or built by Cargo next to each other.
fn binaries() -> Result<(PathBuf, PathBuf)> {
    static OVRD: OnceLock<std::result::Result<PathBuf, String>> = OnceLock::new();
    let cwd = std::env::current_dir()?;
    let ovr = cwd.join(env!("CARGO_BIN_EXE_ovr"));
    let ovrd = OVRD.get_or_init(|| match option_env!("OVRD") {
        Some(ovrd) => Ok(cwd.join(ovrd)),
        None => build_ovrd(&ovr).map_err(|err| err.to_string()),
    });
    Ok((ovr, ovrd.clone()?))
}

/// Cargo builds only the binaries of the package under test, so build `ovrd` with the same profile.
fn build_ovrd(ovr: &Path) -> Result<PathBuf> {
    let dir = ovr.parent().ok_or("ovr has no directory")?;
    let profile = match dir.file_name().and_then(|name| name.to_str()) {
        Some("debug") | None => "dev",
        Some(other) => other,
    };
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let status = Command::new(cargo)
        .args([
            "build", "--quiet", "--locked", "-p", "ovrd", "--bin", "ovrd",
        ])
        .args(["--profile", profile])
        .status()?;
    if !status.success() {
        return Err("cannot build ovrd".into());
    }
    Ok(dir.join("ovrd"))
}

fn git(dir: &Path, args: &[&str]) -> Result {
    let status = Command::new("git").args(args).current_dir(dir).status()?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("git {args:?} failed").into())
    }
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) -> Result {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !done() {
        if Instant::now() > deadline {
            return Err(format!("timed out waiting for {what}").into());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

/// Sends `signal` to `pid`, returning whether there was such a process.
fn signal(pid: &str, signal: &str) -> Result<bool> {
    let status = Command::new("kill")
        .args([signal, pid.trim()])
        .stderr(Stdio::null())
        .status()?;
    Ok(status.success())
}

/// The run number and state in `ovr ps` output for the run whose command contains `needle`.
fn row(ps: &str, needle: &str) -> Option<(String, String)> {
    let line = ps.lines().find(|line| line.contains(needle))?;
    let cells: Vec<&str> = line.split_whitespace().collect();
    let state = if cells.get(2) == Some(&"exited") {
        format!("exited {}", cells.get(3)?)
    } else {
        (*cells.get(2)?).to_owned()
    };
    Some(((*cells.get(1)?).to_owned(), state))
}

#[test]
fn runs_in_the_foreground_and_the_background() -> Result {
    let world = World::new()?;

    let missing = world.run(&["box", "cat", "msg.txt"])?;
    assert_eq!(missing.status.code(), Some(125));
    assert!(text(&missing.stderr).contains("put `--` before the remote command"));

    let foreground = world.run(&["box", "--", "sh", "-c", "cat msg.txt; exit 7"])?;
    assert_eq!(
        (foreground.status.code(), text(&foreground.stdout).as_str()),
        (Some(7), "hello\n")
    );

    let started = world.file("started");
    let script = format!("echo begun; touch {}; sleep 30", started.display());
    let background = world.run(&["-d", "box", "--", "sh", "-c", &script])?;
    assert_eq!(background.status.code(), Some(0));
    let run = text(&background.stdout).trim().to_owned();
    wait_for("the background run", || started.exists())?;
    let ps = text(&world.run(&["ps", "box"])?.stdout);
    assert_eq!(
        row(&ps, "sleep 30"),
        Some((run.clone(), "running".to_owned()))
    );

    assert_eq!(world.run(&["kill", "box", &run])?.status.code(), Some(0));
    let logs = world.run(&["logs", "box", &run])?;
    assert_eq!(
        (logs.status.code(), text(&logs.stdout).as_str()),
        (Some(128 + 9), "begun\n")
    );
    let ps = text(&world.run(&["ps"])?.stdout);
    assert_eq!(row(&ps, "sleep 30"), Some((run, "exited 137".to_owned())));
    Ok(())
}

#[test]
fn runs_outlive_their_agent_and_resume_where_they_left_off() -> Result {
    let world = World::new()?;
    let go = world.file("go");
    let script = format!(
        "echo one; while [ ! -f {} ]; do sleep 0.05; done; echo two; exit 5",
        go.display()
    );
    let mut ovr = world
        .ovr(&["box", "--", "sh", "-c", &script])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stdout = BufReader::new(ovr.stdout.take().ok_or("no stdout")?);
    let mut first = String::new();
    stdout.read_line(&mut first)?;
    assert_eq!(first, "one\n");

    let agent = std::fs::read_to_string(world.host().join("agent.pid"))?;
    assert!(signal(&agent, "-KILL")?, "the agent was running");
    std::fs::write(&go, "")?;

    let mut rest = String::new();
    stdout.read_to_string(&mut rest)?;
    let mut stderr = String::new();
    ovr.stderr
        .take()
        .ok_or("no stderr")?
        .read_to_string(&mut stderr)?;
    let status = ovr.wait()?;
    assert_eq!(
        (status.code(), rest.as_str()),
        (Some(5), "two\n"),
        "{stderr}"
    );
    assert!(stderr.contains("reconnected"), "{stderr}");
    Ok(())
}

#[test]
fn runs_whose_keeper_dies_are_lost_and_cleaned_up() -> Result {
    let world = World::new()?;
    let started = world.file("started");
    let script = format!("touch {}; sleep 30", started.display());
    let background = world.run(&["-d", "box", "--", "sh", "-c", &script])?;
    let run = text(&background.stdout).trim().to_owned();
    wait_for("the background run", || started.exists())?;

    let command = std::fs::read_to_string(world.run_dir(&run).join("pid"))?;
    let keeper = Command::new("ps")
        .args(["-o", "ppid=", "-p", command.trim()])
        .output()?;
    assert!(
        signal(&text(&keeper.stdout), "-KILL")?,
        "the keeper was running"
    );

    let ps = text(&world.run(&["ps", "box"])?.stdout);
    assert_eq!(row(&ps, "sleep 30"), Some((run.clone(), "lost".to_owned())));
    wait_for("the command to be killed", || {
        !signal(&command, "-0").unwrap_or(false)
    })?;
    let logs = world.run(&["logs", "box", &run])?;
    assert_eq!(logs.status.code(), Some(125));
    assert!(text(&logs.stderr).contains("was lost"));
    Ok(())
}
