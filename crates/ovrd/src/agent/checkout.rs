//! The host's persistent checkout of one repository, updated in place from snapshot packs.

use std::io;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;

use ovr_proto::{git, retained};
use tokio::io::AsyncWriteExt;
use tokio::process::{ChildStdin, Command};

use crate::Error;

/// Where claims on checkouts live, beside the owners' directories: `<root>/.locks/<owner>/<name>`.
pub const LOCKS: &str = ".locks";

/// In a checkout's Git directory: `<seq> <commit>` of the newest snapshot applied.
const APPLIED: &str = "ovr-applied";
const GC_LOCK: &str = "ovr-gc.lock";
/// Snapshots synced most recently, kept so that the initiator can pack against any of them.
const KEPT: &str = "refs/ovr/kept/";

pub struct Checkout {
    dir: PathBuf,
    /// Held shared while the checkout is synced or a client is attached to one of its runs, so cleaning leaves it
    /// alone.
    claim: Option<std::fs::File>,
}

impl Checkout {
    /// Opens `<root>/<owner>/<name>`, where `<owner>` stands for `initiator`, creating an empty repository there on
    /// first use. The checkout is claimed until this is dropped.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid initiator or name, or when the repository cannot be created.
    pub async fn open(root: &Path, initiator: &str, name: &str) -> Result<Self, Error> {
        let claim = claim(root, initiator, name).await?;
        let dir = root.join(owner(initiator)?).join(name);
        if !dir.join(".git").is_dir() {
            tokio::fs::create_dir_all(&dir).await?;
            git::run(&dir, &["init", "-q"]).await?;
            // Without reflogs, superseded snapshots become garbage as soon as the next one is checked out.
            git::run(&dir, &["config", "core.logAllRefUpdates", "false"]).await?;
            if cfg!(windows) {
                git::run(&dir, &["config", "core.longpaths", "true"]).await?;
            }
        }
        Ok(Self {
            dir,
            claim: Some(claim),
        })
    }

    /// Hands the checkout's claim over to a caller that needs it for longer.
    #[must_use]
    pub fn into_claim(self) -> Option<std::fs::File> {
        self.claim
    }

    /// The existing checkout in `dir`.
    #[must_use]
    pub fn at(dir: PathBuf) -> Self {
        Self { dir, claim: None }
    }

    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Keeps automatic collection from removing a base or received snapshot during a sync. Other syncs can
    /// still receive and apply concurrently.
    ///
    /// # Errors
    ///
    /// Returns an error when the lock cannot be opened or taken.
    pub async fn hold_objects(&self) -> io::Result<std::fs::File> {
        git::lock_shared(self.dir.join(".git").join(GC_LOCK)).await
    }

    /// Whether the object database has `commit`.
    pub async fn has_commit(&self, commit: &str) -> bool {
        git::has_commit(&self.dir, commit).await
    }

    /// Whether the object database has every one of `commits`.
    pub async fn has_commits(&self, commits: &[String]) -> bool {
        let found = git::commits(&self.dir, commits).await;
        commits.iter().all(|commit| found.contains(commit))
    }

    /// The newest snapshot applied here, if the checkout still has it: what a client lacking its base can send a
    /// pack against instead.
    ///
    /// # Errors
    ///
    /// Returns an error when the apply lock cannot be taken.
    pub async fn newest(&self) -> Result<Option<String>, Error> {
        let git_dir = self.dir.join(".git");
        let _lock = git::lock(git_dir.join("ovr.lock")).await?;
        let Some((_, commit)) = applied(&git_dir) else {
            return Ok(None);
        };
        Ok(self.has_commit(&commit).await.then_some(commit))
    }

    /// Starts `git index-pack` reading a pack from its stdin.
    ///
    /// # Errors
    ///
    /// Returns an error when Git cannot be started.
    pub fn index_pack(&self) -> io::Result<Indexer> {
        let mut child = git::Child::spawn(&self.dir, &["index-pack", "--stdin", "--fix-thin"])?;
        let stdin = child.stdin()?;
        Ok(Indexer { child, stdin })
    }

    /// Checks out the newest snapshot this checkout has seen as `HEAD`: `snapshot`, numbered `seq`, or one with a
    /// higher number that a concurrent run already applied, so a run that arrives late never rolls edits back. Either
    /// way `snapshot` becomes the most recent of the snapshots kept for the initiator to pack against. Only
    /// files whose content differs are rewritten, so unchanged files keep their timestamps; tracked files changed on
    /// the host are restored: the initiator is the source of truth. Commands already running carry on.
    ///
    /// Returns a warning for the client when submodules could not be updated.
    ///
    /// # Errors
    ///
    /// Returns an error when `snapshot` is missing or Git fails.
    pub async fn apply(&self, snapshot: &str, seq: u64) -> Result<Option<String>, Error> {
        if !self.has_commit(snapshot).await {
            return Err(Error::Invalid(format!(
                "snapshot {snapshot} did not arrive"
            )));
        }
        let git_dir = self.dir.join(".git");
        let _lock = git::lock(git_dir.join("ovr.lock")).await?;
        let (target, newest) = match applied(&git_dir) {
            Some((newer, commit)) if newer > seq && self.has_commit(&commit).await => {
                (commit, false)
            }
            _ => (snapshot.to_owned(), true),
        };
        git::run(&self.dir, &["reset", "--hard", "-q", &target]).await?;
        if newest {
            std::fs::write(git_dir.join(APPLIED), format!("{seq} {snapshot}\n"))?;
        }
        let kept = retained::read(&self.dir, KEPT).await?;
        retained::keep(&self.dir, KEPT, &kept, snapshot, &kept).await?;
        if !self.dir.join(".gitmodules").is_file() {
            return Ok(None);
        }
        // Submodules come from their remotes with the host's credentials.
        let updated = git::run(
            &self.dir,
            &["submodule", "update", "--init", "--recursive", "-q"],
        )
        .await;
        Ok(updated
            .err()
            .map(|err| format!("submodules were not updated: {err}")))
    }

    /// Resolves the client's working directory, given relative to the repository root.
    ///
    /// # Errors
    ///
    /// Returns an error when `prefix` escapes the checkout or does not exist in it.
    pub fn cwd(&self, prefix: &str) -> Result<PathBuf, Error> {
        let parts: Vec<&str> = prefix.split('/').filter(|part| !part.is_empty()).collect();
        let valid = parts.iter().all(|part| {
            let mut components = Path::new(part).components();
            matches!(
                (components.next(), components.next()),
                (Some(Component::Normal(_)), None)
            )
        });
        if !valid {
            return Err(Error::Invalid(format!(
                "invalid working directory {prefix:?}"
            )));
        }
        let cwd = parts
            .iter()
            .fold(self.dir.clone(), |dir, part| dir.join(part));
        if cwd.is_dir() {
            Ok(cwd)
        } else {
            Err(Error::Invalid(format!(
                "{prefix} does not exist on the host because it holds no files that are sent"
            )))
        }
    }

    /// Repacks when Git thinks it is time, unless a sync or another collection is using the objects.
    pub async fn collect_garbage(&self) {
        let Ok(lock) = std::fs::File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.dir.join(".git").join(GC_LOCK))
        else {
            return;
        };
        if lock.try_lock().is_err() {
            return;
        }
        let _ = Command::new("git")
            .args(["-c", "gc.autoDetach=false", "gc", "--auto", "--quiet"])
            .current_dir(&self.dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .status()
            .await;
    }
}

/// A running `git index-pack`, fed one frame at a time.
pub struct Indexer {
    child: git::Child,
    stdin: ChildStdin,
}

impl Indexer {
    /// # Errors
    ///
    /// Returns an error when Git stopped reading, typically because the pack is invalid.
    pub async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.stdin.write_all(data).await
    }

    /// Waits for Git to finish writing the pack.
    ///
    /// # Errors
    ///
    /// Returns an error when the pack could not be indexed.
    pub async fn finish(self) -> Result<(), Error> {
        drop(self.stdin);
        Ok(self.child.finish().await?)
    }
}

/// Claims `initiator`'s checkout `name` until the returned file is dropped, so that cleaning leaves it and its runs
/// alone.
///
/// # Errors
///
/// Returns an error for an invalid initiator or name, or when the claim cannot be taken.
pub async fn claim(root: &Path, initiator: &str, name: &str) -> Result<std::fs::File, Error> {
    let owner = owner(initiator)?;
    let valid = !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains(['/', '\\', ':', '\0'])
        && !name.starts_with('-');
    if !valid {
        return Err(Error::Invalid(format!("invalid checkout name {name:?}")));
    }
    let locks = root.join(LOCKS).join(owner);
    tokio::fs::create_dir_all(&locks).await?;
    Ok(git::lock_shared(locks.join(name)).await?)
}

/// The directory holding `initiator`'s checkouts: a prefix of its fingerprint, short for Windows' path limits.
///
/// # Errors
///
/// Returns an error when `initiator` is not alphanumeric.
pub fn owner(initiator: &str) -> Result<&str, Error> {
    if initiator.is_empty() || !initiator.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return Err(Error::Invalid(format!("invalid initiator {initiator:?}")));
    }
    Ok(&initiator[..initiator.len().min(16)])
}

/// The newest snapshot applied to the checkout whose Git directory is `git_dir`, and its number.
fn applied(git_dir: &Path) -> Option<(u64, String)> {
    let text = std::fs::read_to_string(git_dir.join(APPLIED)).ok()?;
    let (seq, commit) = text.trim().split_once(' ')?;
    Some((seq.parse().ok()?, commit.to_owned()))
}
