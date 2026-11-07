//! Snapshots of the working tree as parentless commits, and the thin packs that carry them to a host.
//!
//! `ovr` keeps its own index at `<git-dir>/ovr/index`, so the user's index, branches, stash, and reflog are never
//! touched. Snapshots are deterministic: the same tree always yields the same commit id, so an unchanged tree
//! sends nothing.

use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use ovr_proto::msg::Hello;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdout, Command};

use crate::Error;

/// Fixed identity and date for snapshot commits, so identical trees produce identical commits.
const IDENTITY: [(&str, &str); 6] = [
    ("GIT_AUTHOR_NAME", "ovr"),
    ("GIT_AUTHOR_EMAIL", "ovr@localhost"),
    ("GIT_AUTHOR_DATE", "@0 +0000"),
    ("GIT_COMMITTER_NAME", "ovr"),
    ("GIT_COMMITTER_EMAIL", "ovr@localhost"),
    ("GIT_COMMITTER_DATE", "@0 +0000"),
];

#[derive(Debug)]
pub struct Snapshot {
    root: PathBuf,
    /// Checkout name on the host: the basename of the repository root.
    pub repo: String,
    /// Current directory relative to the repository root, `/`-separated with a trailing slash, or empty.
    pub prefix: String,
    /// The snapshot commit.
    pub commit: String,
    /// The snapshot this host last acknowledged, if any.
    pub base: Option<String>,
    /// Records `base` per host and checkout, and keeps it from being collected by `git gc`.
    host_ref: String,
}

impl Snapshot {
    /// Snapshots the working tree containing `cwd` for delivery to `host`.
    ///
    /// # Errors
    ///
    /// Returns an error outside a Git working tree or when Git fails.
    pub async fn take(cwd: &Path, host: &str) -> Result<Self, Error> {
        let located = git(
            cwd,
            [
                "rev-parse",
                "--show-toplevel",
                "--absolute-git-dir",
                "--show-prefix",
            ],
            &[],
        )
        .await
        .map_err(|_| Error::NotARepository)?;
        let mut lines = located.lines();
        let (Some(root), Some(git_dir)) = (lines.next(), lines.next()) else {
            return Err(Error::NotARepository);
        };
        let root = PathBuf::from(root);
        let git_dir = PathBuf::from(git_dir);
        let prefix = lines.next().unwrap_or_default().to_owned();
        let repo = root
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or(Error::NotARepository)?
            .to_owned();

        let state = git_dir.join("ovr");
        std::fs::create_dir_all(&state)?;
        let _lock = lock(state.join("lock")).await?;
        let index = state.join("index");
        if !index.exists() {
            match std::fs::copy(git_dir.join("index"), &index) {
                Err(err) if err.kind() != io::ErrorKind::NotFound => return Err(err.into()),
                _ => {}
            }
        }
        let index_env = [("GIT_INDEX_FILE", index.as_os_str())];
        git(
            &root,
            ["-c", "core.untrackedCache=true", "add", "-A"],
            &index_env,
        )
        .await?;
        let tree = git(&root, ["write-tree"], &index_env).await?;
        let identity = IDENTITY.map(|(key, value)| (key, OsStr::new(value)));
        let commit = git(
            &root,
            ["commit-tree", "--no-gpg-sign", "-m", "ovr", tree.trim()],
            &identity,
        )
        .await?;

        let host_ref = format!(
            "refs/ovr/hosts/{}/{}",
            ref_component(host),
            ref_component(&repo)
        );
        let base = git(
            &root,
            [
                "rev-parse",
                "-q",
                "--verify",
                &format!("{host_ref}^{{commit}}"),
            ],
            &[],
        )
        .await
        .ok()
        .map(|base| base.trim().to_owned());

        Ok(Self {
            root,
            repo,
            prefix,
            commit: commit.trim().to_owned(),
            base,
            host_ref,
        })
    }

    #[must_use]
    pub fn hello(&self) -> Hello {
        Hello {
            repo: self.repo.clone(),
            base: self.base.clone(),
            snapshot: self.commit.clone(),
        }
    }

    /// Streams the objects of this snapshot that `base` lacks, or the whole snapshot without a base. Returns `None`
    /// when there is nothing to send.
    ///
    /// # Errors
    ///
    /// Returns an error when `git pack-objects` cannot be started.
    pub async fn pack(&self, base: Option<&str>) -> Result<Option<Pack>, Error> {
        if base == Some(self.commit.as_str()) {
            return Ok(None);
        }
        let mut child = Command::new("git")
            .args([
                "pack-objects",
                "--stdout",
                "--revs",
                "--thin",
                "--delta-base-offset",
                "-q",
            ])
            .current_dir(&self.root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let revs = match base {
            Some(base) => format!("{}\n^{base}\n", self.commit),
            None => format!("{}\n", self.commit),
        };
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("no stdin"))?;
        stdin.write_all(revs.as_bytes()).await?;
        drop(stdin);
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("no stdout"))?;
        Ok(Some(Pack { child, stdout }))
    }

    /// Records that the host now has this snapshot, making it the base of the next run.
    ///
    /// # Errors
    ///
    /// Returns an error when the ref cannot be updated.
    pub async fn synced(&self) -> Result<(), Error> {
        git(
            &self.root,
            ["update-ref", &self.host_ref, &self.commit],
            &[],
        )
        .await
        .map(drop)
    }

    /// Forgets the host's base after a failed sync, so the next run sends the full snapshot.
    pub async fn forget(&self) {
        let _ = git(&self.root, ["update-ref", "-d", &self.host_ref], &[]).await;
    }
}

/// A running `git pack-objects`.
pub struct Pack {
    child: Child,
    pub stdout: ChildStdout,
}

impl Pack {
    /// Waits for Git after `stdout` has been read to the end.
    ///
    /// # Errors
    ///
    /// Returns an error when `git pack-objects` failed.
    pub async fn finish(mut self) -> Result<(), Error> {
        let mut stderr = String::new();
        if let Some(mut pipe) = self.child.stderr.take() {
            let _ = pipe.read_to_string(&mut stderr).await;
        }
        let status = self.child.wait().await?;
        if status.success() {
            Ok(())
        } else {
            Err(Error::Git {
                command: "pack-objects".to_owned(),
                stderr: stderr.trim().to_owned(),
            })
        }
    }
}

/// Runs `git` in `dir` and returns its stdout.
async fn git<I, S>(dir: &Path, args: I, env: &[(&str, &OsStr)]) -> Result<String, Error>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let args: Vec<_> = args
        .into_iter()
        .map(|arg| arg.as_ref().to_owned())
        .collect();
    let output = Command::new("git")
        .args(&args)
        .current_dir(dir)
        .envs(env.iter().copied())
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await?;
    if output.status.success() {
        String::from_utf8(output.stdout)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err).into())
    } else {
        Err(Error::Git {
            command: args
                .iter()
                .map(|arg| arg.to_string_lossy())
                .find(|arg| !arg.starts_with('-') && !arg.contains('='))
                .unwrap_or_default()
                .into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        })
    }
}

/// Serialises snapshots of one repository; Git's own index lock would fail the second run instead of waiting.
async fn lock(path: PathBuf) -> io::Result<std::fs::File> {
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)?;
        file.lock()?;
        Ok(file)
    })
    .await?
}

/// Maps a host or repository name onto characters that are always valid in a ref name component.
fn ref_component(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn run(dir: &Path, args: &[&str]) -> String {
        let identity = IDENTITY.map(|(key, value)| (key, OsStr::new(value)));
        git(dir, args, &identity)
            .await
            .unwrap_or_else(|err| format!("error: {err}"))
    }

    async fn files(snapshot: &Snapshot) -> Vec<String> {
        let tree = run(
            &snapshot.root,
            &["ls-tree", "-r", "--name-only", &snapshot.commit],
        )
        .await;
        tree.lines().map(str::to_owned).collect()
    }

    #[tokio::test]
    async fn sends_tracked_and_untracked_files_but_never_ignored_ones() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        run(root, &["init", "-q"]).await;
        std::fs::write(root.join(".gitignore"), "target/\n.env\n")?;
        std::fs::write(root.join("tracked.rs"), "fn main() {}\n")?;
        run(root, &["add", "."]).await;
        run(root, &["commit", "-qm", "init"]).await;
        std::fs::create_dir(root.join("target"))?;
        std::fs::write(root.join("target/out"), "binary")?;
        std::fs::write(root.join(".env"), "TOKEN=secret")?;
        std::fs::create_dir(root.join("src"))?;
        std::fs::write(root.join("src/new.rs"), "// untracked\n")?;
        let index_before = std::fs::read(root.join(".git/index"))?;

        let snapshot = Snapshot::take(&root.join("src"), "box").await?;

        assert_eq!(
            files(&snapshot).await,
            [".gitignore", "src/new.rs", "tracked.rs"]
        );
        assert_eq!(snapshot.prefix, "src/");
        assert_eq!(std::fs::read(root.join(".git/index"))?, index_before);
        assert_eq!(run(root, &["status", "--porcelain"]).await, "?? src/\n");
        Ok(())
    }

    #[tokio::test]
    async fn snapshots_are_deterministic_and_track_the_host_base() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        run(root, &["init", "-q"]).await;
        std::fs::write(root.join("a"), "1")?;

        let first = Snapshot::take(root, "user@box").await?;
        assert_eq!(first.base, None);
        assert_eq!(Snapshot::take(root, "user@box").await?.commit, first.commit);

        first.synced().await?;
        std::fs::write(root.join("a"), "2")?;
        let second = Snapshot::take(root, "user@box").await?;
        assert_ne!(second.commit, first.commit);
        assert_eq!(second.base.as_deref(), Some(first.commit.as_str()));
        assert_eq!(Snapshot::take(root, "other").await?.base, None);

        second.forget().await;
        assert_eq!(Snapshot::take(root, "user@box").await?.base, None);
        Ok(())
    }
}
