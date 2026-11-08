//! A few snapshots kept as refs `<prefix>0` to `<prefix>3`, most recently confirmed first. The initiator keeps one
//! set per host and checkout as the bases it may pack against; a host keeps one per checkout so that those bases
//! stay. Callers hold a lock from reading a set until rewriting it, so concurrent updates cannot lose each other's.

use std::path::Path;

use crate::git::{self, Error};

/// How many distinct snapshots a set holds.
pub const KEPT: usize = 4;

/// The commits in the set under `prefix`, most recent first. Slots that are missing, broken, or not commits are
/// left out.
///
/// # Errors
///
/// Returns an error when Git fails.
pub async fn read(dir: &Path, prefix: &str) -> Result<Vec<String>, Error> {
    let refs = git::run(
        dir,
        ["for-each-ref", "--format=%(refname) %(objectname)", prefix],
    )
    .await?;
    let mut slots: Vec<(usize, String)> = refs
        .lines()
        .filter_map(|line| {
            let (name, id) = line.split_once(' ')?;
            let slot = name.strip_prefix(prefix)?.parse().ok()?;
            (slot < KEPT).then(|| (slot, id.to_owned()))
        })
        .collect();
    slots.sort_unstable();
    let ids: Vec<String> = slots.into_iter().map(|(_, id)| id).collect();
    Ok(git::commits(dir, &ids).await)
}

/// Makes `newest` the most recent snapshot of the set under `prefix`, followed by `rest` without it or duplicates,
/// and evicts what no longer fits. The refs are rewritten in one transaction, and not at all when that leaves
/// `current`, the set as read, unchanged.
///
/// # Errors
///
/// Returns an error when Git fails.
pub async fn keep(
    dir: &Path,
    prefix: &str,
    current: &[String],
    newest: &str,
    rest: &[String],
) -> Result<(), Error> {
    let mut next = vec![newest];
    for id in rest {
        if next.len() < KEPT && !next.contains(&id.as_str()) {
            next.push(id);
        }
    }
    if next == current {
        return Ok(());
    }
    let transaction: Vec<String> = (0..KEPT)
        .map(|slot| match next.get(slot) {
            Some(id) => format!("update {prefix}{slot} {id}\n"),
            None => format!("delete {prefix}{slot}\n"),
        })
        .collect();
    let transaction = transaction.concat();
    git::run_input(dir, &["update-ref", "--stdin"], transaction.as_bytes()).await?;
    Ok(())
}

/// Deletes every ref under `prefix` in one transaction.
///
/// # Errors
///
/// Returns an error when Git fails.
pub async fn forget(dir: &Path, prefix: &str) -> Result<(), Error> {
    let refs = git::run(dir, ["for-each-ref", "--format=delete %(refname)", prefix]).await?;
    if !refs.is_empty() {
        git::run_input(dir, &["update-ref", "--stdin"], refs.as_bytes()).await?;
    }
    Ok(())
}
