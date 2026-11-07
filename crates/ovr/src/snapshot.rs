//! Snapshots of the working tree as parentless commits, and the thin packs that carry them to a host.
//!
//! `ovr` keeps its own index at `<git-dir>/ovr/index`, and numbers snapshots in `<git-dir>/ovr/seq`, so the user's
//! index, branches, stash, and reflog are never touched. Snapshots are deterministic: the same tree always yields the same commit id, so an unchanged tree
//! sends nothing.

use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use ovr_proto::git::{self, lock};
use ovr_proto::msg::Hello;
use ovr_proto::retained;
use tokio::io::AsyncWriteExt;
use tokio::process::ChildStdout;

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
    /// Locks for the shared refs, including when cleaning another worktree's bases.
    bases_dir: PathBuf,
    /// The basename of the repository root.
    repo: String,
    /// Current directory relative to the repository root, `/`-separated with a trailing slash, or empty.
    pub prefix: String,
    /// The snapshot commit.
    pub commit: String,
    /// Orders this repository's snapshots: later snapshots have higher numbers.
    pub seq: u64,
}

impl Snapshot {
    /// Snapshots the working tree containing `cwd`.
    ///
    /// # Errors
    ///
    /// Returns an error outside a Git working tree or when Git fails.
    pub async fn take(cwd: &Path) -> Result<Self, Error> {
        let located = git::run(
            cwd,
            [
                "rev-parse",
                "--show-toplevel",
                "--absolute-git-dir",
                "--path-format=absolute",
                "--git-common-dir",
                "--show-prefix",
            ],
        )
        .await
        .map_err(|_| Error::NotARepository)?;
        let mut lines = located.lines();
        let (Some(root), Some(git_dir), Some(common_dir)) =
            (lines.next(), lines.next(), lines.next())
        else {
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
        let waited = Instant::now();
        let _lock = lock(state.join("lock")).await?;
        tracing::debug!(elapsed = ?waited.elapsed(), "took the snapshot lock");
        let built = Instant::now();
        let index = state.join("index");
        if !index.exists() {
            match std::fs::copy(git_dir.join("index"), &index) {
                Err(err) if err.kind() != io::ErrorKind::NotFound => return Err(err.into()),
                _ => {}
            }
        }
        let index_env = [("GIT_INDEX_FILE", index.as_os_str())];
        git::run_env(
            &root,
            ["-c", "core.untrackedCache=true", "add", "-A"],
            &index_env,
        )
        .await?;
        let tree = git::run_env(&root, ["write-tree"], &index_env).await?;
        let identity = IDENTITY.map(|(key, value)| (key, OsStr::new(value)));
        let commit = git::run_env(
            &root,
            ["commit-tree", "--no-gpg-sign", "-m", "ovr", tree.trim()],
            &identity,
        )
        .await?;
        let seq = next_seq(&state.join("seq"))?;
        tracing::debug!(elapsed = ?built.elapsed(), commit = commit.trim(), seq, "built the snapshot");

        Ok(Self {
            root,
            bases_dir: PathBuf::from(common_dir).join("ovr").join("bases"),
            repo,
            prefix,
            commit: commit.trim().to_owned(),
            seq,
        })
    }

    /// Addresses this snapshot to `host`'s checkout for `initiator` and this repository.
    ///
    /// # Errors
    ///
    /// Returns an error when the bases cannot be read.
    pub async fn deliver_to(&self, host: &str, initiator: &str) -> Result<Delivery<'_>, Error> {
        let mut key = initiator.as_bytes().to_vec();
        key.push(0);
        key.extend_from_slice(self.root.as_os_str().as_encoded_bytes());
        let checkout = format!("{}-{:08x}", self.repo, ovr_proto::fnv1a(&key) >> 32);
        let (host, name) = (ref_component(host), ref_component(&checkout));
        let refs = format!("refs/ovr/bases/{host}/{name}/");
        let lock_file = self.bases_dir.join(host).join(format!("{name}.lock"));
        let looked = Instant::now();
        let bases = {
            let _lock = lock_bases(&lock_file).await?;
            retained::read(&self.root, &refs).await?
        };
        tracing::debug!(elapsed = ?looked.elapsed(), %checkout, ?bases, "looked up the bases");
        Ok(Delivery {
            snapshot: self,
            checkout,
            initiator: initiator.to_owned(),
            bases,
            refs,
            lock_file,
        })
    }
}

/// A snapshot on its way to one host's checkout.
#[derive(Debug)]
pub struct Delivery<'a> {
    pub snapshot: &'a Snapshot,
    /// The host's checkout: `<repo>-<key>`, where the key hashes the initiator and the repository root.
    pub checkout: String,
    initiator: String,
    /// The snapshots this host acknowledged most recently, newest first: what the pack is built against.
    pub bases: Vec<String>,
    /// Records the bases per host and checkout, and keeps them from being collected by `git gc`.
    refs: String,
    /// Held while reading or rewriting the bases.
    lock_file: PathBuf,
}

impl Delivery<'_> {
    #[must_use]
    pub fn hello(&self) -> Hello {
        Hello {
            checkout: self.checkout.clone(),
            seq: self.snapshot.seq,
            bases: self.bases.clone(),
            snapshot: self.snapshot.commit.clone(),
            initiator: self.initiator.clone(),
        }
    }

    /// Streams the objects of the snapshot that none of `bases` has, or the whole snapshot without bases. Returns
    /// `None` when there is nothing to send.
    ///
    /// Snapshots have no parents, and Git leaves out the trees and blobs of an excluded commit only when it is a
    /// parent of a sent one, unless told to look at every excluded commit with `--shallow`.
    ///
    /// # Errors
    ///
    /// Returns an error when `git pack-objects` cannot be started.
    pub async fn pack(&self, bases: &[String]) -> Result<Option<Pack>, Error> {
        let commit = &self.snapshot.commit;
        if bases.contains(commit) {
            return Ok(None);
        }
        let mut child = git::Child::spawn(
            &self.snapshot.root,
            &[
                "pack-objects",
                "--stdout",
                "--revs",
                "--thin",
                "--shallow",
                "--delta-base-offset",
                "-q",
            ],
        )?;
        let mut revs = format!("{commit}\n");
        for base in bases {
            revs.push('^');
            revs.push_str(base);
            revs.push('\n');
        }
        let mut stdin = child.stdin()?;
        stdin.write_all(revs.as_bytes()).await?;
        drop(stdin);
        let stdout = child.stdout()?;
        Ok(Some(Pack { child, stdout }))
    }

    /// Records that the host now has the snapshot, making it the most recent base of the next run. The other bases
    /// stay, unless the host lacked one of them: then only `recovered`, what this run's pack was built against
    /// instead, is known to be on the host.
    ///
    /// # Errors
    ///
    /// Returns an error when the refs cannot be updated.
    pub async fn synced(&self, recovered: Option<&[String]>) -> Result<(), Error> {
        let root = &self.snapshot.root;
        let _lock = lock_bases(&self.lock_file).await?;
        let current = retained::read(root, &self.refs).await?;
        let rest = recovered.unwrap_or(&current);
        retained::keep(root, &self.refs, &current, &self.snapshot.commit, rest).await?;
        Ok(())
    }

    /// Whether `commit`, which a host offered as a base, is a snapshot this repository still has to pack against.
    pub async fn has(&self, commit: &str) -> bool {
        git::has_commit(&self.snapshot.root, commit).await
    }
}

/// Locks the bases of one host and checkout for reading or rewriting.
async fn lock_bases(path: &Path) -> Result<std::fs::File, Error> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    Ok(lock(path.to_owned()).await?)
}

/// The next snapshot number: one more than the last, and at least the time in milliseconds, so numbering keeps
/// rising even when `.git/ovr` is deleted.
fn next_seq(path: &Path) -> Result<u64, Error> {
    let last = std::fs::read_to_string(path)
        .ok()
        .and_then(|text| text.trim().parse::<u64>().ok())
        .unwrap_or(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |time| {
            u64::try_from(time.as_millis()).unwrap_or(u64::MAX)
        });
    let seq = (last + 1).max(now);
    std::fs::write(path, seq.to_string())?;
    Ok(seq)
}

/// A running `git pack-objects`.
pub struct Pack {
    child: git::Child,
    pub stdout: ChildStdout,
}

impl Pack {
    /// Waits for Git after `stdout` has been read to the end.
    ///
    /// # Errors
    ///
    /// Returns an error when `git pack-objects` failed.
    pub async fn finish(self) -> Result<(), Error> {
        Ok(self.child.finish().await?)
    }
}

/// Forgets the bases of every checkout on `host` in the repository around `cwd`, if there is one, after the host's
/// checkouts were removed.
pub async fn forget_host(cwd: &Path, host: &str) {
    let host = ref_component(host);
    let prefix = format!("refs/ovr/bases/{host}/");
    let Ok(common_dir) = git::run(
        cwd,
        ["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )
    .await
    else {
        return;
    };
    let Ok(refs) = git::run(cwd, ["for-each-ref", "--format=%(refname)", &prefix]).await else {
        return;
    };
    let checkouts: std::collections::BTreeSet<_> = refs
        .lines()
        .filter_map(|name| {
            name.strip_prefix(&prefix)?
                .split_once('/')
                .map(|(name, _)| name)
        })
        .collect();
    let locks = Path::new(common_dir.trim())
        .join("ovr")
        .join("bases")
        .join(host);
    for checkout in checkouts {
        let Ok(_lock) = lock_bases(&locks.join(format!("{checkout}.lock"))).await else {
            continue;
        };
        let _ = retained::forget(cwd, &format!("{prefix}{checkout}/")).await;
    }
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
        git::run_env(dir, args, &identity)
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

        let snapshot = Snapshot::take(&root.join("src")).await?;

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
    async fn snapshots_are_deterministic_ordered_and_track_the_host_base() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        run(root, &["init", "-q"]).await;
        std::fs::write(root.join("a"), "1")?;

        let first = Snapshot::take(root).await?;
        let to_box = first.deliver_to("user@box", "me").await?;
        assert_eq!(to_box.bases, [] as [String; 0]);
        let again = Snapshot::take(root).await?;
        assert_eq!(again.commit, first.commit);
        assert!(again.seq > first.seq);

        to_box.synced(None).await?;
        std::fs::write(root.join("a"), "2")?;
        let second = Snapshot::take(root).await?;
        assert_ne!(second.commit, first.commit);
        let to_box = second.deliver_to("user@box", "me").await?;
        assert_eq!(to_box.bases, [first.commit]);
        let none: [String; 0] = [];
        assert_eq!(second.deliver_to("other", "me").await?.bases, none);
        assert_eq!(second.deliver_to("user@box", "you").await?.bases, none);
        Ok(())
    }

    #[tokio::test]
    async fn bases_are_distinct_most_recent_first_and_bounded() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        run(root, &["init", "-q"]).await;
        // Syncs a tree whose one file holds `content`, returning its snapshot.
        let sync = async |content: &str, recovered: Option<&[String]>| {
            std::fs::write(root.join("file"), content)?;
            let snapshot = Snapshot::take(root).await?;
            let delivery = snapshot.deliver_to("box", "me").await?;
            delivery.synced(recovered).await?;
            Ok::<_, Error>(snapshot.commit)
        };
        // The bases of the next run, by content.
        let bases = async || {
            let snapshot = Snapshot::take(root).await?;
            let mut contents = Vec::new();
            for base in snapshot.deliver_to("box", "me").await?.bases {
                contents.push(run(root, &["show", &format!("{base}:file")]).await);
            }
            Ok::<_, Error>(contents)
        };

        sync("a", None).await?;
        for _ in 0..=retained::KEPT {
            sync("b", None).await?;
        }
        assert_eq!(bases().await?, ["b", "a"], "repeats take no slot");
        for content in ["c", "a", "d", "e"] {
            sync(content, None).await?;
        }
        assert_eq!(
            bases().await?,
            ["e", "d", "a", "c"],
            "b was used least recently"
        );

        let c = sync("c", None).await?;
        sync("f", Some(&[c])).await?;
        assert_eq!(
            bases().await?,
            ["f", "c"],
            "after recovery, only what the host had"
        );

        std::fs::write(root.join("file"), "g")?;
        let g = Snapshot::take(root).await?;
        let g_delivery = g.deliver_to("box", "me").await?;
        std::fs::write(root.join("file"), "h")?;
        let h = Snapshot::take(root).await?;
        let h_delivery = h.deliver_to("box", "me").await?;
        let (g_synced, h_synced) = tokio::join!(g_delivery.synced(None), h_delivery.synced(None));
        g_synced?;
        h_synced?;
        let mut contents = bases().await?;
        contents.sort();
        assert_eq!(
            contents,
            ["c", "f", "g", "h"],
            "both acknowledgements survive"
        );

        h.deliver_to("other", "me").await?.synced(None).await?;
        let linked = tempfile::tempdir()?;
        git::run(
            root,
            [
                OsStr::new("worktree"),
                OsStr::new("add"),
                OsStr::new("--detach"),
                linked.path().as_os_str(),
                OsStr::new(&h.commit),
            ],
        )
        .await?;
        let linked_snapshot = Snapshot::take(linked.path()).await?;
        let linked_delivery = linked_snapshot.deliver_to("box", "me").await?;
        linked_delivery.synced(None).await?;
        let held = lock_bases(&linked_delivery.lock_file).await?;
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), forget_host(root, "box"))
                .await
                .is_err(),
            "clean must take the same metadata lock, even for another worktree"
        );
        drop(held);
        forget_host(root, "box").await;
        assert_eq!(bases().await?, [] as [String; 0]);
        assert_eq!(
            linked_snapshot.deliver_to("box", "me").await?.bases,
            [] as [String; 0]
        );
        assert_eq!(h.deliver_to("other", "me").await?.bases, [h.commit]);
        Ok(())
    }

    async fn pack_len(snapshot: &Snapshot, bases: &[String]) -> Result<usize, Error> {
        let delivery = snapshot.deliver_to("box", "me").await?;
        let mut pack = delivery
            .pack(bases)
            .await?
            .ok_or_else(|| io::Error::other("nothing to send"))?;
        let mut bytes = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut pack.stdout, &mut bytes).await?;
        pack.finish().await?;
        Ok(bytes.len())
    }

    #[tokio::test]
    async fn a_pack_against_a_base_leaves_out_what_the_base_has() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        run(root, &["init", "-q"]).await;
        std::fs::create_dir(root.join("src"))?;
        // Pseudo-random text, so that neither compression nor deltas can shrink the files.
        let mut state: u64 = 1;
        for index in 0..20 {
            let text: String = (0..4096)
                .map(|_| {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1);
                    char::from(b'a' + u8::try_from(state >> 59).unwrap_or(0) % 26)
                })
                .collect();
            std::fs::write(root.join(format!("src/{index}.rs")), text)?;
        }
        let first = Snapshot::take(root).await?;
        std::fs::write(root.join("src/new.rs"), "fn new() {}\n")?;
        let second = Snapshot::take(root).await?;

        let full = pack_len(&second, &[]).await?;
        let thin = pack_len(&second, std::slice::from_ref(&first.commit)).await?;
        assert!(
            thin * 10 < full,
            "{thin} bytes against the base, {full} without"
        );
        Ok(())
    }
}
