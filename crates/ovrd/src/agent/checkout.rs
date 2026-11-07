//! The host's persistent checkout of one repository, updated in place from snapshot packs.

use std::io;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;

use ovr_proto::git;
use tokio::io::AsyncWriteExt;
use tokio::process::{ChildStdin, Command};

use crate::Error;

pub struct Checkout {
    dir: PathBuf,
}

impl Checkout {
    /// Opens `<root>/<repo>`, creating an empty repository there on first use.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid name or when the repository cannot be created.
    pub async fn open(root: &Path, repo: &str) -> Result<Self, Error> {
        let valid = !repo.is_empty()
            && repo != "."
            && repo != ".."
            && !repo.contains(['/', '\\', ':', '\0'])
            && !repo.starts_with('-');
        if !valid {
            return Err(Error::Invalid(format!("invalid repository name {repo:?}")));
        }
        let dir = root.join(repo);
        if !dir.join(".git").is_dir() {
            tokio::fs::create_dir_all(&dir).await?;
            git::run(&dir, &["init", "-q"]).await?;
            // Without reflogs, superseded snapshots become garbage as soon as the next one is checked out.
            git::run(&dir, &["config", "core.logAllRefUpdates", "false"]).await?;
            if cfg!(windows) {
                git::run(&dir, &["config", "core.longpaths", "true"]).await?;
            }
        }
        Ok(Self { dir })
    }

    /// Whether the object database has `commit` and therefore every object of that snapshot.
    pub async fn has_commit(&self, commit: &str) -> bool {
        is_object_id(commit)
            && git::run(
                &self.dir,
                &["cat-file", "-e", &format!("{commit}^{{commit}}")],
            )
            .await
            .is_ok()
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

    /// Serialises runs against this checkout across connections and processes.
    ///
    /// # Errors
    ///
    /// Returns an error when the lock file cannot be opened.
    pub async fn lock(&self) -> io::Result<std::fs::File> {
        git::lock(self.dir.join(".git").join("ovr.lock")).await
    }

    /// Checks out `snapshot` as `HEAD`. Only files whose content differs are rewritten, so unchanged files keep
    /// their timestamps; tracked files changed on the host are restored: the laptop is the source of truth.
    ///
    /// Returns a warning for the client when submodules could not be updated.
    ///
    /// # Errors
    ///
    /// Returns an error when `snapshot` is missing or Git fails.
    pub async fn apply(&self, snapshot: &str) -> Result<Option<String>, Error> {
        if !self.has_commit(snapshot).await {
            return Err(Error::Invalid(format!(
                "snapshot {snapshot} did not arrive"
            )));
        }
        git::run(&self.dir, &["reset", "--hard", "-q", snapshot]).await?;
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

    /// Repacks in the background when Git thinks it is time, off every run's critical path.
    pub fn collect_garbage(&self) {
        let _ = Command::new("git")
            .args(["gc", "--auto", "--quiet"])
            .current_dir(&self.dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
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

fn is_object_id(id: &str) -> bool {
    matches!(id.len(), 40 | 64) && id.bytes().all(|b| b.is_ascii_hexdigit())
}
