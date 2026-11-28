//! Pairing records in the data directory, and other JSON records, written atomically and readable only by the user.

use std::io;
use std::path::{Path, PathBuf};

use ovr_proto::msg::Provider;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::trust::Rule;

/// A host paired with this initiator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairedHost {
    pub name: String,
    pub fingerprint: String,
}

/// The initiator this host dials, as recorded by `ovrd connect ADDR FINGERPRINT-CODE`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Initiator {
    pub addr: String,
    pub fingerprint: String,
    /// The name this host paired under, which the initiator keeps for it whatever the host is later called.
    pub name: String,
}

/// A tunnel the initiator keeps open, as recorded by `ovr pair PROVIDER`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tunnel {
    pub provider: Provider,
    /// What `ovrd` created at the provider, for Dev Tunnels the tunnel ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

fn hosts_path(dir: &Path) -> PathBuf {
    dir.join("hosts.json")
}

fn initiator_path(dir: &Path) -> PathBuf {
    dir.join("initiator.json")
}

fn tunnels_path(dir: &Path) -> PathBuf {
    dir.join("tunnels.json")
}

fn trust_path(dir: &Path) -> PathBuf {
    dir.join("trust.json")
}

/// # Errors
///
/// Returns an error when the file exists but cannot be read or parsed.
pub fn hosts(dir: &Path) -> io::Result<Vec<PairedHost>> {
    Ok(read(&hosts_path(dir))?.unwrap_or_default())
}

/// Records the host with `fingerprint` as paired, returning its name: the one it already has if it paired before,
/// else `hostname`, `hostname-2`, `hostname-3`, and so on, the first no other paired host has.
///
/// # Errors
///
/// Returns an error when the records cannot be read or written.
pub fn add_host(dir: &Path, hostname: &str, fingerprint: &str) -> io::Result<String> {
    let mut hosts = hosts(dir)?;
    if let Some(known) = hosts.iter().find(|known| known.fingerprint == fingerprint) {
        return Ok(known.name.clone());
    }
    // At most one name per host is taken, so one of these is free.
    let name = (1..=hosts.len() + 1)
        .map(|n| match n {
            1 => hostname.to_owned(),
            n => format!("{hostname}-{n}"),
        })
        .find(|name| hosts.iter().all(|known| &known.name != name))
        .unwrap_or_else(|| hostname.to_owned());
    hosts.push(PairedHost {
        name: name.clone(),
        fingerprint: fingerprint.to_owned(),
    });
    hosts.sort_by(|a, b| a.name.cmp(&b.name));
    write(&hosts_path(dir), &hosts)?;
    Ok(name)
}

/// Forgets the host paired as `name`, returning whether there was one.
///
/// # Errors
///
/// Returns an error when the records cannot be read or written.
pub fn remove_host(dir: &Path, name: &str) -> io::Result<bool> {
    let mut hosts = hosts(dir)?;
    let before = hosts.len();
    hosts.retain(|known| known.name != name);
    if hosts.len() == before {
        return Ok(false);
    }
    write(&hosts_path(dir), &hosts)?;
    Ok(true)
}

/// # Errors
///
/// Returns an error when the file exists but cannot be read or parsed.
pub fn initiator(dir: &Path) -> io::Result<Option<Initiator>> {
    read(&initiator_path(dir))
}

/// # Errors
///
/// Returns an error when the record cannot be written.
pub fn set_initiator(dir: &Path, initiator: &Initiator) -> io::Result<()> {
    write(&initiator_path(dir), initiator)
}

/// Forgets the initiator this host dials, returning it if there was one.
///
/// # Errors
///
/// Returns an error when the record cannot be read or removed.
pub fn forget_initiator(dir: &Path) -> io::Result<Option<Initiator>> {
    let initiator = initiator(dir)?;
    if initiator.is_some() {
        std::fs::remove_file(initiator_path(dir))?;
    }
    Ok(initiator)
}

/// # Errors
///
/// Returns an error when the file exists but cannot be read or parsed.
pub fn tunnels(dir: &Path) -> io::Result<Vec<Tunnel>> {
    Ok(read(&tunnels_path(dir))?.unwrap_or_default())
}

/// # Errors
///
/// Returns an error when the records cannot be written.
pub fn set_tunnels(dir: &Path, tunnels: &[Tunnel]) -> io::Result<()> {
    write(&tunnels_path(dir), &tunnels)
}

/// # Errors
///
/// Returns an error when the file exists but cannot be read or parsed.
pub fn trust_rules(dir: &Path) -> io::Result<Vec<Rule>> {
    Ok(read(&trust_path(dir))?.unwrap_or_default())
}

/// # Errors
///
/// Returns an error when the rules cannot be written.
pub fn set_trust_rules(dir: &Path, rules: &[Rule]) -> io::Result<()> {
    write(&trust_path(dir), &rules)
}

/// # Errors
///
/// Returns an error when the file exists but cannot be read or parsed.
pub fn read<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: {err}", path.display()),
            )
        }),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

/// Replaces `path` with `value` as JSON, atomically and readable only by the owner.
///
/// # Errors
///
/// Returns an error when the file cannot be written.
pub fn write<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    bytes.push(b'\n');
    write_private(path, &bytes)
}

/// Replaces `path` with `bytes` atomically, readable only by the owner.
///
/// # Errors
///
/// Returns an error when the file cannot be written.
pub fn write_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let dir = path.parent().ok_or(io::ErrorKind::InvalidInput)?;
    let mut file = tempfile::NamedTempFile::new_in(dir)?;
    io::Write::write_all(&mut file, bytes)?;
    file.as_file().sync_all()?;
    // NamedTempFile creates files with mode 0600 on Unix; %LOCALAPPDATA% is already private on Windows.
    file.persist(path).map_err(|err| err.error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_named_hosts_get_numbers_and_re_pairing_keeps_the_name() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let host = |name: &str, fingerprint: &str| PairedHost {
            name: name.to_owned(),
            fingerprint: fingerprint.to_owned(),
        };
        assert_eq!(add_host(dir.path(), "winbox", "a")?, "winbox");
        assert_eq!(add_host(dir.path(), "winbox", "b")?, "winbox-2");
        assert_eq!(add_host(dir.path(), "renamed", "a")?, "winbox");
        assert_eq!(add_host(dir.path(), "winbox", "c")?, "winbox-3");
        assert_eq!(
            hosts(dir.path())?,
            [
                host("winbox", "a"),
                host("winbox-2", "b"),
                host("winbox-3", "c")
            ]
        );
        Ok(())
    }
}
