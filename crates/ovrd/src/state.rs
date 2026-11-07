//! Pairing records in the data directory, written atomically and readable only by the user.

use std::io;
use std::path::{Path, PathBuf};

use ovr_proto::msg::Provider;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// A host paired with this initiator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairedHost {
    pub name: String,
    pub fingerprint: String,
}

/// The initiator this host dials, as recorded by `ovrd connect ADDR CODE`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Initiator {
    pub addr: String,
    pub fingerprint: String,
}

/// A tunnel the initiator keeps open, as recorded by `ovrd pair PROVIDER`.
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

/// # Errors
///
/// Returns an error when the file exists but cannot be read or parsed.
pub fn hosts(dir: &Path) -> io::Result<Vec<PairedHost>> {
    Ok(read(&hosts_path(dir))?.unwrap_or_default())
}

/// Records `host`, replacing an earlier pairing under the same name or with the same identity.
///
/// # Errors
///
/// Returns an error when the records cannot be read or written.
pub fn add_host(dir: &Path, host: PairedHost) -> io::Result<()> {
    let mut hosts = hosts(dir)?;
    hosts.retain(|known| known.name != host.name && known.fingerprint != host.fingerprint);
    hosts.push(host);
    hosts.sort_by(|a, b| a.name.cmp(&b.name));
    write(&hosts_path(dir), &hosts)
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

fn read<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
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

fn write<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
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
    fn re_pairing_replaces_records_by_name_or_identity() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let host = |name: &str, fingerprint: &str| PairedHost {
            name: name.to_owned(),
            fingerprint: fingerprint.to_owned(),
        };
        add_host(dir.path(), host("winbox", "a"))?;
        add_host(dir.path(), host("linux", "b"))?;
        add_host(dir.path(), host("winbox", "c"))?;
        add_host(dir.path(), host("renamed", "b"))?;
        assert_eq!(
            hosts(dir.path())?,
            [host("renamed", "b"), host("winbox", "c")]
        );
        Ok(())
    }
}
