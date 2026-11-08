//! Advisory file locks that are released when dropped.
//!
//! A lock belongs to the open file, and a process spawned by any thread shares the parent's open files from fork
//! until exec closes them. Closing the parent's handle in that window would leave the lock held by the child, so a
//! [`Lock`] unlocks explicitly before closing.

use std::fs::{File, TryLockError};
use std::io;
use std::path::{Path, PathBuf};

/// A held lock on a file, released when dropped.
#[derive(Debug)]
pub struct Lock(File);

impl Lock {
    /// Takes an exclusive lock on `path`, creating it, and waits for any other holder.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be opened or locked.
    pub async fn exclusive(path: PathBuf) -> io::Result<Self> {
        Self::wait(path, File::lock).await
    }

    /// Like [`Lock::exclusive`], but shared with other shared holders.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be opened or locked.
    pub async fn shared(path: PathBuf) -> io::Result<Self> {
        Self::wait(path, File::lock_shared).await
    }

    /// Takes an exclusive lock on `path`, creating it, unless someone else holds it.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be opened or locked.
    pub fn try_exclusive(path: &Path) -> io::Result<Option<Self>> {
        let file = open(path)?;
        match file.try_lock() {
            Ok(()) => Ok(Some(Self(file))),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Error(err)) => Err(err),
        }
    }

    async fn wait(path: PathBuf, lock: fn(&File) -> io::Result<()>) -> io::Result<Self> {
        tokio::task::spawn_blocking(move || {
            let file = open(&path)?;
            lock(&file)?;
            Ok(Self(file))
        })
        .await?
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

fn open(path: &Path) -> io::Result<File> {
    File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
}
