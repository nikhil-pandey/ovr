//! Running the system `git`, which both ends rely on for snapshots, packs, and checkouts.

use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use tokio::io::AsyncReadExt;
use tokio::process::{ChildStdin, ChildStdout, Command};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("git {command} failed: {stderr}")]
    Failed { command: String, stderr: String },
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Runs `git` in `dir` and returns its stdout.
///
/// # Errors
///
/// Returns an error when Git cannot be started, fails, or prints something other than UTF-8.
pub async fn run<I, S>(dir: &Path, args: I) -> Result<String, Error>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run_env(dir, args, &[]).await
}

/// Runs `git` in `dir` with extra environment `env` and returns its stdout.
///
/// # Errors
///
/// Returns an error when Git cannot be started, fails, or prints something other than UTF-8.
pub async fn run_env<I, S>(dir: &Path, args: I, env: &[(&str, &OsStr)]) -> Result<String, Error>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let args: Vec<_> = args
        .into_iter()
        .map(|arg| arg.as_ref().to_owned())
        .collect();
    let output = command(dir, &args)
        .envs(env.iter().copied())
        .stdin(Stdio::null())
        .output()
        .await?;
    if output.status.success() {
        String::from_utf8(output.stdout)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err).into())
    } else {
        Err(Error::Failed {
            command: subcommand(&args),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        })
    }
}

/// A running `git` whose stdin and stdout the caller streams.
pub struct Child {
    child: tokio::process::Child,
    command: String,
}

impl Child {
    /// Starts `git` in `dir` with piped stdin and stdout.
    ///
    /// # Errors
    ///
    /// Returns an error when Git cannot be started.
    pub fn spawn(dir: &Path, args: &[&str]) -> io::Result<Self> {
        let child = command(dir, args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        Ok(Self {
            child,
            command: subcommand(args),
        })
    }

    /// Takes Git's stdin.
    ///
    /// # Errors
    ///
    /// Returns an error when it was already taken.
    pub fn stdin(&mut self) -> io::Result<ChildStdin> {
        self.child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("git stdin is taken"))
    }

    /// Takes Git's stdout.
    ///
    /// # Errors
    ///
    /// Returns an error when it was already taken.
    pub fn stdout(&mut self) -> io::Result<ChildStdout> {
        self.child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("git stdout is taken"))
    }

    /// Waits for Git once its stdin is closed and its stdout drained.
    ///
    /// # Errors
    ///
    /// Returns an error when Git failed.
    pub async fn finish(mut self) -> Result<(), Error> {
        drop(self.child.stdin.take());
        let mut stderr = String::new();
        if let Some(mut pipe) = self.child.stderr.take() {
            pipe.read_to_string(&mut stderr).await?;
        }
        if self.child.wait().await?.success() {
            Ok(())
        } else {
            Err(Error::Failed {
                command: self.command,
                stderr: stderr.trim().to_owned(),
            })
        }
    }
}

/// Takes an exclusive lock on `path`, creating it, and waits for any other holder.
///
/// # Errors
///
/// Returns an error when the file cannot be opened or locked.
pub async fn lock(path: PathBuf) -> io::Result<std::fs::File> {
    lock_with(path, std::fs::File::lock).await
}

/// Like [`lock`], but shared with other shared holders.
///
/// # Errors
///
/// Returns an error when the file cannot be opened or locked.
pub async fn lock_shared(path: PathBuf) -> io::Result<std::fs::File> {
    lock_with(path, std::fs::File::lock_shared).await
}

async fn lock_with(
    path: PathBuf,
    lock: fn(&std::fs::File) -> io::Result<()>,
) -> io::Result<std::fs::File> {
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)?;
        lock(&file)?;
        Ok(file)
    })
    .await?
}

fn command<S: AsRef<OsStr>>(dir: &Path, args: &[S]) -> Command {
    let mut command = Command::new("git");
    command
        .args(args)
        .current_dir(dir)
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

/// The subcommand, for messages: the first argument that is neither an option nor its value.
fn subcommand<S: AsRef<OsStr>>(args: &[S]) -> String {
    let mut args = args.iter().map(|arg| arg.as_ref().to_string_lossy());
    let mut command = None;
    while let Some(arg) = args.next() {
        if arg == "-c" || arg == "-C" {
            args.next();
        } else if !arg.starts_with('-') {
            command = Some(arg.into_owned());
            break;
        }
    }
    command.unwrap_or_default()
}
