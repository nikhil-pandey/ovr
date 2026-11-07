//! Where `ovr` and `ovrd` keep their files on this machine, following each platform's conventions. Every directory
//! is created private on first use.
//!
//! | | Linux | macOS | Windows |
//! |---|---|---|---|
//! | data: identity, pairings, locks | `~/.local/share/ovr` | `~/Library/Application Support/ovr` | `%LOCALAPPDATA%\ovr\data` |
//! | cache: downloaded agents | `~/.cache/ovr` | `~/Library/Caches/ovr` | `%LOCALAPPDATA%\ovr\cache` |
//! | logs | `~/.local/state/ovr` | `~/Library/Logs/ovr` | data |
//! | runs, on hosts | `~/.local/state/ovr/runs` | data `/runs` | data `\runs` |
//! | checkouts, on hosts | data `/checkouts` | data `/checkouts` | data `\checkouts` |
//! | runtime: socket | `$XDG_RUNTIME_DIR/ovr`, else logs | data | data |
//!
//! [`OVERRIDES`] move them on every platform: `OVR_DATA_DIR`, `OVR_CACHE_DIR`, `OVR_STATE_DIR` for logs and runs,
//! and `OVR_CHECKOUTS`. XDG variables are honoured on Linux. Empty or relative values fall back to the defaults.

use std::io;
use std::path::{Path, PathBuf};

use directories::ProjectDirs;

/// The variables that move `ovr`'s directories, which a service started at login must be given as well.
pub const OVERRIDES: [&str; 4] = [
    "OVR_DATA_DIR",
    "OVR_CACHE_DIR",
    "OVR_STATE_DIR",
    "OVR_CHECKOUTS",
];

/// Identity and pairing records, whose loss means pairing again, and the locks that keep one `ovrd` per user.
///
/// # Errors
///
/// Returns an error when the home directory is unknown or the directory cannot be created.
pub fn data_dir() -> io::Result<PathBuf> {
    match overridden("OVR_DATA_DIR") {
        Some(dir) => create(&dir),
        None => create(project()?.data_local_dir()),
    }
}

/// Files that can be downloaded again.
///
/// # Errors
///
/// Returns an error when the home directory is unknown or the directory cannot be created.
pub fn cache_dir() -> io::Result<PathBuf> {
    match overridden("OVR_CACHE_DIR") {
        Some(dir) => create(&dir),
        None => create(project()?.cache_dir()),
    }
}

/// Logs of the background `ovrd` processes.
///
/// # Errors
///
/// Returns an error when the home directory is unknown or the directory cannot be created.
pub fn log_dir() -> io::Result<PathBuf> {
    if let Some(dir) = overridden("OVR_STATE_DIR") {
        return create(&dir);
    }
    #[cfg(target_os = "macos")]
    let dir = home_dir()?.join("Library/Logs/ovr");
    #[cfg(not(target_os = "macos"))]
    let dir = {
        let project = project()?;
        project
            .state_dir()
            .unwrap_or_else(|| project.data_local_dir())
            .to_owned()
    };
    create(&dir)
}

/// Run records on a host: each run's request, output log, and end.
///
/// # Errors
///
/// Returns an error when the home directory is unknown or the directory cannot be created.
pub fn runs_dir() -> io::Result<PathBuf> {
    if let Some(dir) = overridden("OVR_STATE_DIR") {
        return create(&dir.join("runs"));
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    let parent = log_dir()?;
    #[cfg(any(windows, target_os = "macos"))]
    let parent = data_dir()?;
    create(&parent.join("runs"))
}

/// The local socket. Without a per-session runtime
/// directory, e.g. over SSH without a login manager, a private persistent directory stands in.
///
/// # Errors
///
/// Returns an error when the home directory is unknown or the directory cannot be created.
pub fn runtime_dir() -> io::Result<PathBuf> {
    let project = project()?;
    match project.runtime_dir() {
        Some(dir) => create(dir),
        #[cfg(all(unix, not(target_os = "macos")))]
        None => log_dir(),
        #[cfg(any(windows, target_os = "macos"))]
        None => data_dir(),
    }
}

/// A host's checkouts, one per initiator and repository.
///
/// # Errors
///
/// Returns an error when the home directory is unknown or the directory cannot be created.
pub fn checkouts_dir() -> io::Result<PathBuf> {
    match overridden("OVR_CHECKOUTS") {
        Some(dir) => create(&dir),
        None => create(&data_dir()?.join("checkouts")),
    }
}

/// The current user's home directory.
///
/// # Errors
///
/// Returns an error when it cannot be determined.
pub fn home_dir() -> io::Result<PathBuf> {
    directories::BaseDirs::new()
        .map(|dirs| dirs.home_dir().to_owned())
        .ok_or_else(unknown_home)
}

/// `var` when it holds an absolute path.
fn overridden(var: &str) -> Option<PathBuf> {
    std::env::var_os(var)
        .map(PathBuf::from)
        .filter(|dir| dir.is_absolute())
}

fn project() -> io::Result<ProjectDirs> {
    ProjectDirs::from("", "", "ovr").ok_or_else(unknown_home)
}

fn unknown_home() -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        "cannot determine the home directory",
    )
}

fn create(dir: &Path) -> io::Result<PathBuf> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(dir)?;
    Ok(dir.to_owned())
}
