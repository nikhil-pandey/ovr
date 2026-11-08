//! Removing what runs leave behind on a host: checkouts, finished runs, and uploaded agents of other versions.
//!
//! Checkouts claimed by a sync are left, with their runs, as are runs still running, watched by a client, or whose
//! keeper is still serving them, with their checkouts. A checkout is first moved into `<root>/.trash` and then deleted, so one that cannot be removed whole, for example
//! because a leftover build server on Windows holds a file open, is left as it was and reported as in use.

use std::io;
use std::path::{Path, PathBuf};

use ovr_proto::VERSION;
use ovr_proto::lock::Lock;
use ovr_proto::msg::{Cleaned, RunState};

use super::Config;
use super::checkout::{self, LOCKS};
use super::runs::Store;
use crate::Error;

const TRASH: &str = ".trash";

/// Removes `initiator`'s checkouts and finished runs, or everyone's when `None`, leaving checkouts that are syncing
/// or have a run still running, and removes agents of other versions uploaded over SSH.
///
/// # Errors
///
/// Returns an error for an invalid initiator or when a directory cannot be read.
pub fn clean(config: &Config, initiator: Option<&str>) -> Result<Cleaned, Error> {
    let mut cleaned = Cleaned::default();
    let trash = config.root.join(TRASH);
    // Whatever an earlier clean could not delete.
    cleaned.bytes += remove(&trash);

    let owners = match initiator {
        Some(initiator) => vec![checkout::owner(initiator)?.to_owned()],
        None => entries(&config.root)?
            .into_iter()
            .filter(|(name, path)| !name.starts_with('.') && path.is_dir())
            .map(|(name, _)| name)
            .collect(),
    };
    // Locked before looking at runs: a sync records its run before letting go of its claim.
    let mut claimed = Vec::new();
    let mut in_use = Vec::new();
    for owner in owners {
        let locks = config.root.join(LOCKS).join(&owner);
        for (name, dir) in entries(&config.root.join(&owner))? {
            if !dir.is_dir() {
                continue;
            }
            std::fs::create_dir_all(&locks)?;
            match Lock::try_exclusive(&locks.join(&name))? {
                Some(lock) => claimed.push((format!("{owner}-{name}"), name, dir, lock)),
                None => in_use.push((dir, name)),
            }
        }
    }

    let mut busy = Vec::new();
    for run in Store::new(config.runs.clone()).all()? {
        let Ok(meta) = run.meta() else { continue };
        if initiator.is_some_and(|initiator| initiator != meta.initiator) {
            continue;
        }
        // A finished run may still be serving its end to a client, being replayed to one, or about to be attached to
        // by the sync that started it.
        let claimed = in_use.iter().any(|(dir, _)| meta.dir.starts_with(dir));
        if claimed || run.kept()? || run.watched()? || run.state()? == RunState::Running {
            busy.push(meta.dir);
            continue;
        }
        cleaned.bytes += remove(&run.dir);
        cleaned.runs += 1;
    }

    cleaned
        .kept
        .extend(in_use.into_iter().map(|(_, name)| name));
    for (trashed, name, dir, _lock) in claimed {
        let moved = trash.join(trashed);
        let removable = !busy.iter().any(|run| run.starts_with(&dir))
            && std::fs::create_dir_all(&trash).is_ok()
            && std::fs::rename(&dir, &moved).is_ok();
        if removable {
            cleaned.bytes += remove(&moved);
            cleaned.checkouts += 1;
        } else {
            cleaned.kept.push(name);
        }
    }

    // This version's agent and its upload in progress stay; a running agent of another version, on Windows, stays
    // until the next clean.
    let current = format!("ovrd-{VERSION}");
    for (name, path) in entries(&config.agents)? {
        if name.starts_with("ovrd-")
            && !name.starts_with(&current)
            && std::fs::remove_file(&path).is_ok()
        {
            cleaned.agents += 1;
        }
    }
    Ok(cleaned)
}

/// The entries of `dir`, none when it does not exist.
fn entries(dir: &Path) -> io::Result<Vec<(String, PathBuf)>> {
    match std::fs::read_dir(dir) {
        Ok(entries) => Ok(entries
            .filter_map(Result::ok)
            .map(|entry| {
                (
                    entry.file_name().to_string_lossy().into_owned(),
                    entry.path(),
                )
            })
            .collect()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err),
    }
}

/// Deletes `path` and returns the bytes it held; what cannot be deleted is left for the next clean.
pub fn remove(path: &Path) -> u64 {
    let bytes = size(path);
    match std::fs::remove_dir_all(path) {
        Ok(()) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => 0,
        Err(err) => {
            tracing::warn!(%err, path = %path.display(), "cannot remove");
            bytes.saturating_sub(size(path))
        }
    }
}

fn size(path: &Path) -> u64 {
    let Ok(meta) = std::fs::symlink_metadata(path) else {
        return 0;
    };
    if !meta.is_dir() {
        return meta.len();
    }
    std::fs::read_dir(path).map_or(0, |entries| {
        entries
            .filter_map(Result::ok)
            .map(|entry| size(&entry.path()))
            .sum()
    })
}

#[cfg(test)]
mod tests {
    use ovr_proto::msg::Exec;

    use super::*;
    use crate::agent::Shell;
    use crate::agent::checkout::Checkout;
    use crate::agent::runs::{Ended, Meta};

    fn meta(initiator: &str, dir: &Path) -> Meta {
        Meta {
            initiator: initiator.to_owned(),
            checkout: Some(String::new()),
            dir: dir.to_owned(),
            cwd: dir.to_owned(),
            exec: Exec {
                argv: Vec::new(),
                cwd: String::new(),
                tty: false,
                cols: 80,
                rows: 24,
            },
            started: 0,
            synced_snapshot: None,
        }
    }

    /// A checkout whose sync has finished.
    async fn synced(config: &Config, initiator: &str, name: &str) -> Result<PathBuf, Error> {
        Ok(Checkout::open(&config.root, initiator, name)
            .await?
            .dir()
            .to_owned())
    }

    #[tokio::test]
    async fn removes_what_is_idle_and_leaves_what_is_in_use() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let config = Config {
            root: dir.path().join("checkouts"),
            runs: dir.path().join("runs"),
            agents: dir.path().join("agents"),
            base: dir.path().to_owned(),
            shell: Shell::new("/bin/sh"),
            keeper: None,
        };
        let store = Store::new(config.runs.clone());
        let idle = synced(&config, "me", "idle").await?;
        let running = synced(&config, "me", "running").await?;
        let theirs = synced(&config, "you", "idle").await?;
        store
            .create(&meta("me", &idle))
            .await?
            .end(&Ended::Exited { code: 0 })?;
        // Recorded moments ago and not yet ended: still starting, so running.
        store.create(&meta("me", &running)).await?;
        // Ended, but its keeper is still serving the end to a client.
        let draining = synced(&config, "me", "draining").await?;
        let drained = store.create(&meta("me", &draining)).await?;
        drained.end(&Ended::Exited { code: 0 })?;
        let keeper = Lock::exclusive(drained.lock_path()).await?;
        store
            .create(&meta("you", &theirs))
            .await?
            .end(&Ended::Lost)?;
        // Claimed until dropped, as by a sync in progress.
        let syncing = Checkout::open(&config.root, "me", "syncing").await?;
        std::fs::create_dir_all(&config.agents)?;
        let old = config.agents.join("ovrd-0.0.1");
        let current = config.agents.join(format!("ovrd-{VERSION}"));
        std::fs::write(&old, "old")?;
        std::fs::write(&current, "current")?;

        let mut mine = clean(&config, Some("me"))?;
        mine.kept.sort();
        assert_eq!(
            (mine.checkouts, mine.runs, mine.agents, mine.kept),
            (
                1,
                1,
                1,
                vec![
                    "draining".to_owned(),
                    "running".to_owned(),
                    "syncing".to_owned()
                ]
            )
        );
        assert!(mine.bytes > 0);
        assert!(!idle.exists() && running.exists() && theirs.exists());
        assert!(!old.exists() && current.exists());

        drop((syncing, keeper));
        let everyone = clean(&config, None)?;
        assert_eq!(
            (everyone.checkouts, everyone.runs, everyone.kept),
            (3, 2, vec!["running".to_owned()])
        );
        assert!(running.exists() && !theirs.exists());
        Ok(())
    }

    #[tokio::test]
    async fn runs_watched_by_a_client_are_kept_with_their_checkout() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let config = Config {
            root: dir.path().join("checkouts"),
            runs: dir.path().join("runs"),
            agents: dir.path().join("agents"),
            base: dir.path().to_owned(),
            shell: Shell::new("/bin/sh"),
            keeper: None,
        };
        let store = Store::new(config.runs.clone());
        let idle = synced(&config, "me", "idle").await?;
        let in_base = Meta {
            checkout: None,
            ..meta("me", &config.base)
        };
        let running = store.create(&in_base).await?;
        store
            .create(&in_base)
            .await?
            .end(&Ended::Exited { code: 0 })?;
        let replayed = store.create(&in_base).await?;
        replayed.end(&Ended::Exited { code: 0 })?;
        let watching = replayed.watch().await?;
        let followed = synced(&config, "me", "followed").await?;
        let in_checkout = store.create(&meta("me", &followed)).await?;
        in_checkout.end(&Ended::Exited { code: 0 })?;
        let following = in_checkout.watch().await?;

        let cleaned = clean(&config, Some("me"))?;
        assert_eq!(
            (cleaned.checkouts, cleaned.runs, cleaned.kept),
            (1, 1, vec!["followed".to_owned()]),
            "a run in the base directory keeps no checkout but its own"
        );
        assert!(!idle.exists() && running.dir.exists() && replayed.dir.exists());

        drop((watching, following));
        let cleaned = clean(&config, Some("me"))?;
        assert_eq!((cleaned.checkouts, cleaned.runs), (1, 2));
        Ok(())
    }
}
