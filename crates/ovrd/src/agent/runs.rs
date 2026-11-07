//! Run records on a host: one directory per run under the runs directory, named by its number.
//!
//! | File | Written by | Holds |
//! |---|---|---|
//! | `run.json` | agent | the request: initiator, checkout, command, start time |
//! | `lock` | keeper | locked for the keeper's whole life, so a free lock means no keeper |
//! | `pid` | keeper | the command's process group, once it has started |
//! | `out.*`, `err.*` | keeper | the output log; see [`super::log`] |
//! | `exit.json` | keeper, or agent for a lost run | how the run ended |
//! | `sock` | keeper | the keeper's socket, on Unix |

use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ovr_proto::git;
use ovr_proto::msg::{Exec, RunInfo, RunState};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::state::write_private;

/// Finished runs are removed this long after they end.
const RETENTION: Duration = Duration::from_hours(7 * 24);

/// A keeper that has not reported its command started after this long is not coming.
const STARTING: Duration = Duration::from_secs(30);

/// What the agent asked the keeper to run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub initiator: String,
    pub checkout: String,
    /// Absolute: the checkout directory the command runs in, or below.
    pub dir: PathBuf,
    pub cwd: PathBuf,
    pub exec: Exec,
    /// Seconds since the Unix epoch.
    pub started: u64,
}

/// How a run ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Ended {
    Exited { code: i32 },
    Failed { message: String },
    Lost,
}

/// One run's directory.
#[derive(Debug, Clone)]
pub struct Run {
    pub id: u64,
    pub dir: PathBuf,
}

impl Run {
    /// The run in `dir`, a directory named by the run's number.
    ///
    /// # Errors
    ///
    /// Returns an error when `dir` is not named like a run.
    pub fn at(dir: PathBuf) -> Result<Self, Error> {
        let id = dir
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.parse().ok())
            .ok_or_else(|| Error::Invalid(format!("{} is not a run", dir.display())))?;
        Ok(Self { id, dir })
    }

    /// # Errors
    ///
    /// Returns an error when the request cannot be read.
    pub fn meta(&self) -> io::Result<Meta> {
        read(&self.dir.join("run.json"))?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "the run has no request"))
    }

    /// # Errors
    ///
    /// Returns an error when the record exists but cannot be read.
    pub fn ended(&self) -> io::Result<Option<Ended>> {
        read(&self.dir.join("exit.json"))
    }

    /// Records how the run ended.
    ///
    /// # Errors
    ///
    /// Returns an error when the record cannot be written.
    pub fn end(&self, ended: &Ended) -> io::Result<()> {
        write_json(&self.dir.join("exit.json"), ended)
    }

    /// Records that the command started, as process group `group` where there is one.
    ///
    /// # Errors
    ///
    /// Returns an error when the record cannot be written.
    pub fn started(&self, group: Option<u32>) -> io::Result<()> {
        let group = group.map(|group| group.to_string()).unwrap_or_default();
        write_private(&self.dir.join("pid"), group.as_bytes())
    }

    #[must_use]
    pub fn lock_path(&self) -> PathBuf {
        self.dir.join("lock")
    }

    /// Where the keeper listens: a socket in the run directory on Unix, a pipe named after it on Windows.
    #[must_use]
    pub fn endpoint(&self) -> PathBuf {
        #[cfg(unix)]
        {
            self.dir.join("sock")
        }
        #[cfg(windows)]
        {
            let key = ovr_proto::fnv1a(self.dir.as_os_str().as_encoded_bytes());
            PathBuf::from(format!(r"\\.\pipe\ovr-run-{key:016x}"))
        }
    }

    /// The keeper's lock, when no keeper holds it.
    fn lock_unkept(&self) -> io::Result<Option<std::fs::File>> {
        let file = std::fs::File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.lock_path())?;
        match file.try_lock() {
            Ok(()) => Ok(Some(file)),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(err)) => Err(err),
        }
    }

    /// The run's state. A run whose keeper is gone without a record is recorded as lost, after killing whatever
    /// is left of its command.
    ///
    /// # Errors
    ///
    /// Returns an error when the run's files cannot be read.
    pub fn state(&self) -> io::Result<RunState> {
        if let Some(ended) = self.ended()? {
            return Ok(ended.into());
        }
        // Holding the lock keeps a keeper that is still starting from starting while this decides.
        let Some(_lock) = self.lock_unkept()? else {
            return Ok(RunState::Running);
        };
        // The keeper ends the run before letting go of the lock.
        if let Some(ended) = self.ended()? {
            return Ok(ended.into());
        }
        let pid = std::fs::read_to_string(self.dir.join("pid")).ok();
        if pid.is_none() && self.age()? < STARTING {
            return Ok(RunState::Running);
        }
        #[cfg(unix)]
        if let Some(group) = pid.and_then(|pid| pid.trim().parse().ok()) {
            super::process::kill_group(group);
        }
        self.end(&Ended::Lost)?;
        Ok(RunState::Lost)
    }

    fn age(&self) -> io::Result<Duration> {
        let created = std::fs::metadata(self.dir.join("run.json"))?.modified()?;
        Ok(created.elapsed().unwrap_or_default())
    }
}

impl From<Ended> for RunState {
    fn from(ended: Ended) -> Self {
        match ended {
            Ended::Exited { code } => Self::Exited { code },
            Ended::Failed { message } => Self::Failed { message },
            Ended::Lost => Self::Lost,
        }
    }
}

/// The runs directory.
pub struct Store {
    dir: PathBuf,
}

impl Store {
    #[must_use]
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    /// Records a new run under the next number, and removes runs that ended long ago.
    ///
    /// # Errors
    ///
    /// Returns an error when the run cannot be recorded.
    pub async fn create(&self, meta: &Meta) -> Result<Run, Error> {
        std::fs::create_dir_all(&self.dir)?;
        let _lock = git::lock(self.dir.join("lock")).await?;
        let next = self.dir.join("next");
        let id = match std::fs::read_to_string(&next) {
            Ok(text) => text.trim().parse().unwrap_or(1),
            Err(err) if err.kind() == io::ErrorKind::NotFound => 1,
            Err(err) => return Err(err.into()),
        };
        write_private(&next, (id + 1).to_string().as_bytes())?;
        let run = Run {
            id,
            dir: self.dir.join(id.to_string()),
        };
        std::fs::create_dir(&run.dir)?;
        write_json(&run.dir.join("run.json"), meta)?;
        self.collect();
        Ok(run)
    }

    /// # Errors
    ///
    /// Returns an error when there is no such run.
    pub fn open(&self, id: u64) -> Result<Run, Error> {
        let dir = self.dir.join(id.to_string());
        if dir.join("run.json").is_file() {
            Ok(Run { id, dir })
        } else {
            Err(Error::Invalid(format!("there is no run {id}")))
        }
    }

    /// `initiator`'s runs, oldest first.
    ///
    /// # Errors
    ///
    /// Returns an error when the runs directory cannot be read.
    pub fn list(&self, initiator: &str) -> io::Result<Vec<RunInfo>> {
        let mut runs: Vec<RunInfo> = self
            .all()?
            .into_iter()
            .filter_map(|run| {
                let meta = run.meta().ok().filter(|meta| meta.initiator == initiator)?;
                let state = run.state().ok()?;
                Some(RunInfo {
                    run: run.id,
                    checkout: meta.checkout,
                    argv: meta.exec.argv,
                    started: meta.started,
                    state,
                })
            })
            .collect();
        runs.sort_by_key(|info| info.run);
        Ok(runs)
    }

    fn all(&self) -> io::Result<Vec<Run>> {
        match std::fs::read_dir(&self.dir) {
            Ok(entries) => Ok(entries
                .filter_map(|entry| Run::at(entry.ok()?.path()).ok())
                .collect()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }

    /// Removes runs that ended more than [`RETENTION`] ago.
    fn collect(&self) {
        let Ok(runs) = self.all() else { return };
        for run in runs {
            let ended =
                std::fs::metadata(run.dir.join("exit.json")).and_then(|meta| meta.modified());
            if ended.is_ok_and(|ended| ended.elapsed().unwrap_or_default() > RETENTION)
                && let Err(err) = std::fs::remove_dir_all(&run.dir)
            {
                tracing::warn!(%err, run = run.id, "cannot remove an old run");
            }
        }
    }
}

/// Seconds since the Unix epoch.
#[must_use]
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn read<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    write_private(path, &serde_json::to_vec(value).map_err(io::Error::other)?)
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::process::{CommandExt, ExitStatusExt};

    use super::*;

    fn meta(initiator: &str) -> Meta {
        Meta {
            initiator: initiator.to_owned(),
            checkout: "repo-1".to_owned(),
            dir: PathBuf::from("/"),
            cwd: PathBuf::from("/"),
            exec: Exec {
                argv: vec!["sleep".to_owned(), "30".to_owned()],
                cwd: String::new(),
                tty: false,
                cols: 80,
                rows: 24,
            },
            started: now(),
        }
    }

    #[tokio::test]
    async fn runs_without_a_keeper_are_lost_and_their_commands_killed() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let store = Store::new(dir.path().to_owned());
        let run = store.create(&meta("me")).await?;
        store.create(&meta("someone else")).await?;
        assert_eq!(
            run.state()?,
            RunState::Running,
            "a keeper may still be starting"
        );

        let mut command = std::process::Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()?;
        run.started(Some(command.id()))?;
        let keeper = git::lock(run.lock_path()).await?;
        assert_eq!(run.state()?, RunState::Running, "the keeper holds the run");

        drop(keeper);
        assert_eq!(run.state()?, RunState::Lost);
        assert_eq!(command.wait()?.signal(), Some(9), "what was left is killed");
        assert_eq!(run.ended()?, Some(Ended::Lost));

        let listed = store.list("me")?;
        assert_eq!(
            listed
                .iter()
                .map(|info| (info.run, &info.state))
                .collect::<Vec<_>>(),
            [(run.id, &RunState::Lost)]
        );
        Ok(())
    }
}
