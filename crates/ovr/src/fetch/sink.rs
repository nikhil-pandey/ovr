//! Writes what a host sends below DEST. A host is not necessarily trustworthy, so every entry is untrusted input:
//! only plain relative names, nothing written through a symlink, never a `.git`, and every file written to a
//! temporary name and renamed into place once complete, so a file is either whole or absent.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use ovr_proto::fetch as rules;
use ovr_proto::msg::{Done, Entry, EntryKind, Note};
use zstd::stream::raw::{Decoder, InBuffer, Operation, OutBuffer};

use super::plan::Plan;
use super::report::Report;
use crate::Error;

/// Decompressed bytes are written in pieces of this size, however well the host's data compresses.
const DECODED: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Files and symlinks written.
    pub files: u64,
    /// Their size.
    pub bytes: u64,
    /// Compressed bytes received.
    pub wire: u64,
    /// Files the host skipped because the copy here was current.
    pub unchanged: u64,
    /// Entries or sources that could not be fetched.
    pub failed: u64,
}

/// The file whose data is arriving.
enum Incoming {
    Writing(Temp),
    /// The file cannot be written; its data is decoded all the same, to keep the stream in step.
    Discarding,
}

/// Why an entry is not written.
struct Refusal {
    path: PathBuf,
    why: String,
    failed: bool,
}

impl Refusal {
    fn failed(path: &Path, why: impl std::fmt::Display) -> Self {
        Self {
            path: path.to_owned(),
            why: why.to_string(),
            failed: true,
        }
    }
}

pub struct Sink<'a, W> {
    plan: &'a Plan,
    pub report: &'a mut Report<W>,
    decoder: Decoder<'static>,
    decoded: Vec<u8>,
    incoming: Option<Incoming>,
    /// Directories that could not be created; what the host sends below them is skipped quietly.
    blocked: Vec<PathBuf>,
    /// DEST is a directory that sources land in: as planned, or since a source copied as DEST was a directory.
    into: bool,
    /// Where each source's entries go below DEST, as planned until a source copied as DEST turns out a directory.
    below: Vec<Vec<String>>,
    /// DEST as a directory, resolved once it exists, so that nothing the host creates in it can redirect it.
    base: Option<PathBuf>,
    pub stats: Stats,
}

impl<'a, W: Write> Sink<'a, W> {
    /// # Errors
    ///
    /// Returns an error when zstd cannot allocate its context.
    pub fn new(plan: &'a Plan, report: &'a mut Report<W>) -> Result<Self, Error> {
        Ok(Self {
            plan,
            report,
            decoder: Decoder::new()?,
            decoded: vec![0; DECODED],
            incoming: None,
            blocked: Vec::new(),
            into: plan.into,
            below: plan
                .targets
                .iter()
                .map(|target| target.below.clone())
                .collect(),
            base: None,
            stats: Stats::default(),
        })
    }

    /// Creates a directory or symlink, or starts a file whose data follows.
    ///
    /// # Errors
    ///
    /// Returns an error when the entry breaks the protocol: during a file, or for a source not asked for.
    pub fn entry(&mut self, entry: &Entry) -> Result<(), Error> {
        if self.incoming.is_some() {
            return Err(ovr_proto::Error::Unexpected(ovr_proto::FrameKind::Entry).into());
        }
        let file = matches!(entry.kind, EntryKind::File { .. });
        let placed = self.place(entry);
        let incoming = match placed {
            Ok(temp) => temp.map(Incoming::Writing),
            Err(refusal) => {
                self.refused(refusal);
                None
            }
        };
        if file {
            self.incoming = Some(incoming.unwrap_or(Incoming::Discarding));
        }
        Ok(())
    }

    /// Writes the next compressed piece of the current file, or finishes it on an empty one.
    ///
    /// # Errors
    ///
    /// Returns an error outside a file, or when the data does not decompress.
    pub fn data(&mut self, data: &[u8]) -> Result<(), Error> {
        if self.incoming.is_none() {
            return Err(ovr_proto::Error::Unexpected(ovr_proto::FrameKind::Data).into());
        }
        if data.is_empty() {
            self.finish();
            return Ok(());
        }
        self.stats.wire += data.len() as u64;
        let mut input = InBuffer::around(data);
        let mut refusal = None;
        loop {
            let mut output = OutBuffer::around(&mut self.decoded[..]);
            self.decoder.run(&mut input, &mut output).map_err(|err| {
                Error::Host(format!(
                    "the host sent data that does not decompress: {err}"
                ))
            })?;
            let written = output.pos();
            if let Some(Incoming::Writing(temp)) = &mut self.incoming
                && let Err(err) = temp.write(&self.decoded[..written])
            {
                refusal = Some(Refusal::failed(&temp.dest, err));
            }
            if refusal.is_some() {
                self.incoming = Some(Incoming::Discarding);
            }
            // Everything decodable has been decoded once the input is used up without filling the output.
            if input.pos() == data.len() && written < self.decoded.len() {
                break;
            }
        }
        if let Some(refusal) = refusal {
            self.refused(refusal);
        }
        Ok(())
    }

    /// Something the host did not send; during a file, the file could not be read to the end and is discarded.
    pub fn note(&mut self, note: &Note) {
        self.incoming = None;
        self.stats.failed += u64::from(note.failed);
        self.report.note(&note.message);
    }

    /// The end of the fetch.
    ///
    /// # Errors
    ///
    /// Returns an error when it comes during a file.
    pub fn done(mut self, done: Done) -> Result<Stats, Error> {
        if self.incoming.is_some() {
            return Err(ovr_proto::Error::Unexpected(ovr_proto::FrameKind::Done).into());
        }
        self.stats.unchanged = done.unchanged;
        Ok(self.stats)
    }

    fn finish(&mut self) {
        let Some(Incoming::Writing(temp)) = self.incoming.take() else {
            return;
        };
        let dest = temp.dest.clone();
        match temp.persist() {
            Ok(bytes) => {
                self.stats.files += 1;
                self.stats.bytes += bytes;
            }
            Err(err) => self.refused(Refusal::failed(&dest, err)),
        }
    }

    #[allow(clippy::needless_pass_by_value)] // consumed: a refusal is reported once
    fn refused(&mut self, refusal: Refusal) {
        self.stats.failed += u64::from(refusal.failed);
        if !refusal.why.is_empty() {
            let message = format!("{}: {}", refusal.path.display(), refusal.why);
            self.report.note(&message);
        }
    }

    /// Creates what `entry` names, or for a file, a temporary file to write it to.
    fn place(&mut self, entry: &Entry) -> Result<Option<Temp>, Refusal> {
        let plan = self.plan;
        let index = usize::try_from(entry.source)
            .ok()
            .filter(|&index| index < plan.targets.len())
            .ok_or_else(|| {
                let why = format!(
                    "refused an entry for source {}, which was not asked for",
                    entry.source
                );
                Refusal::failed(&plan.dest, why)
            })?;
        if !self.into && entry.path.is_empty() && entry.kind == EntryKind::Dir {
            self.land_inside(index)?;
        }
        let below = self.below[index].clone();
        let root = below
            .iter()
            .fold(plan.dest.clone(), |path, part| path.join(part));
        let entry_parts = rules::components(&entry.path, cfg!(windows))
            .map_err(|why| Refusal::failed(&root, why))?;
        let parts: Vec<&str> = below
            .iter()
            .map(String::as_str)
            .chain(entry_parts)
            .collect();
        let path = parts
            .iter()
            .fold(plan.dest.clone(), |path, part| path.join(part));
        if parts.iter().any(|part| rules::is_git(part)) {
            return Err(Refusal {
                path,
                why: "skipped: .git directories are never written".to_owned(),
                failed: false,
            });
        }
        if self.blocked.iter().any(|dir| path.starts_with(dir)) {
            return Err(Refusal {
                path,
                why: String::new(),
                failed: true,
            });
        }
        match &entry.kind {
            EntryKind::Dir => {
                let made = match parts.split_last() {
                    Some(_) => self.dirs(&parts).map(drop),
                    None => self.base().map(drop),
                };
                if let Err(refusal) = made {
                    self.blocked.push(path);
                    return Err(refusal);
                }
                Ok(None)
            }
            EntryKind::File { mtime_ns, exec, .. } => {
                let parent = self.parent(&parts)?;
                let real = parts
                    .last()
                    .map_or_else(|| path.clone(), |last| parent.join(last));
                not_a_dir(&real, "a file")?;
                let temp = Temp::create(&parent, real, *mtime_ns, *exec)
                    .map_err(|err| Refusal::failed(&path, err))?;
                Ok(Some(temp))
            }
            EntryKind::Symlink { target } => {
                if parts.is_empty() {
                    // A named source is followed on the host, so it never arrives as a symlink; one here would make
                    // DEST itself a symlink that later entries are written through.
                    let why = "refused a symlink in place of the source itself";
                    return Err(Refusal::failed(&path, why));
                }
                let parent = self.parent(&parts)?;
                let real = parts
                    .last()
                    .map_or_else(|| path.clone(), |last| parent.join(last));
                not_a_dir(&real, "a symlink")?;
                symlink(&parent, &real, target)?;
                self.stats.files += 1;
                Ok(None)
            }
        }
    }

    /// Lands a source copied as DEST inside DEST instead, now that it turned out to be a directory, so that the same
    /// command puts it in the same place whether DEST exists yet or not.
    fn land_inside(&mut self, index: usize) -> Result<(), Refusal> {
        self.into = true;
        let Some(name) = &self.plan.targets[index].name else {
            return Ok(());
        };
        let path = self.plan.dest.join(name);
        if rules::is_git(name) || rules::components(name, cfg!(windows)).is_err() {
            self.blocked.push(path.clone());
            let why = "cannot be created here under the source's name; copy what is in it instead, as SRC/.";
            return Err(Refusal::failed(&path, why));
        }
        self.below[index] = vec![name.clone()];
        Ok(())
    }

    /// The directory an entry at `parts` below DEST goes in, created if missing.
    fn parent(&mut self, parts: &[&str]) -> Result<PathBuf, Refusal> {
        match parts.split_last() {
            Some((_, parents)) => self.dirs(parents),
            // The source is copied as DEST itself, which goes where the user named it.
            None => Ok(self
                .plan
                .dest
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .map_or_else(|| PathBuf::from("."), Path::to_owned)),
        }
    }

    /// Makes sure each of `parts` below DEST is a real directory, creating what is missing, and returns the last.
    /// A symlink is never gone through, so a host cannot send `link -> ~` and then `link/.bashrc`.
    fn dirs(&mut self, parts: &[&str]) -> Result<PathBuf, Refusal> {
        if !self.into {
            // A file copied as DEST has nothing below it; anything there would go through what the host made.
            let why = "is not a directory here, where the host has one";
            return Err(Refusal::failed(&self.plan.dest, why));
        }
        let mut dir = self.base()?;
        for part in parts {
            dir.push(part);
            match fs::symlink_metadata(&dir) {
                Ok(meta) if meta.is_dir() => continue,
                Ok(meta) if meta.is_symlink() => {
                    let why = "is a symlink here, and nothing is written through one";
                    return Err(Refusal::failed(&dir, why));
                }
                Ok(_) => {
                    let why = "is a file here, where the host has a directory";
                    return Err(Refusal::failed(&dir, why));
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(Refusal::failed(&dir, err)),
            }
            match fs::create_dir(&dir) {
                Err(err) if !(err.kind() == io::ErrorKind::AlreadyExists && real_dir(&dir)) => {
                    return Err(Refusal::failed(&dir, err));
                }
                _ => {}
            }
        }
        Ok(dir)
    }

    /// DEST as a directory, created if missing and resolved once: it is the user's to name and may be a symlink, but a
    /// symlink the host sends into it, even one replacing DEST through an alias, cannot redirect later entries.
    fn base(&mut self) -> Result<PathBuf, Refusal> {
        if let Some(base) = &self.base {
            return Ok(base.clone());
        }
        self.dest_dir()?;
        let dest = &self.plan.dest;
        // Windows initiators create no symlinks, so there is nothing to resolve against there.
        let base = if cfg!(unix) {
            fs::canonicalize(dest).map_err(|err| Refusal::failed(dest, err))?
        } else {
            dest.clone()
        };
        self.base = Some(base.clone());
        Ok(base)
    }

    /// DEST as a directory, created if missing. It is the user's to name, so it may be a symlink to one.
    fn dest_dir(&self) -> Result<(), Refusal> {
        let dest = &self.plan.dest;
        if dest.is_dir() {
            return Ok(());
        }
        if dest.symlink_metadata().is_ok() {
            let why = "is a file here, where the host has a directory";
            return Err(Refusal::failed(dest, why));
        }
        fs::create_dir_all(dest).or_else(|err| {
            if dest.is_dir() {
                Ok(())
            } else {
                Err(Refusal::failed(dest, err))
            }
        })
    }
}

fn real_dir(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|meta| meta.is_dir())
}

/// Refuses to replace a directory with `what`; anything else is replaced whole, symlinks included.
fn not_a_dir(path: &Path, what: &str) -> Result<(), Refusal> {
    if real_dir(path) {
        let why = format!("is a directory here, where the host has {what}");
        return Err(Refusal::failed(path, why));
    }
    Ok(())
}

/// Creates the symlink `path -> target` in `parent`, replacing whatever but a directory is there. Its target is
/// only a name: nothing is written through it.
#[cfg(unix)]
fn symlink(parent: &Path, path: &Path, target: &str) -> Result<(), Refusal> {
    let mut attempt = 0;
    loop {
        let temp = temp_path(parent, attempt);
        match std::os::unix::fs::symlink(target, &temp) {
            Ok(()) => {
                return fs::rename(&temp, path).map_err(|err| {
                    let _ = fs::remove_file(&temp);
                    Refusal::failed(path, err)
                });
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => attempt += 1,
            Err(err) => return Err(Refusal::failed(path, err)),
        }
    }
}

#[cfg(not(unix))]
fn symlink(_: &Path, path: &Path, target: &str) -> Result<(), Refusal> {
    Err(Refusal {
        path: path.to_owned(),
        why: format!("skipped the symlink to {target}: symlinks are not created on Windows"),
        failed: false,
    })
}

/// A file being written beside where it goes, private until it is complete, and removed unless it is persisted.
struct Temp {
    path: PathBuf,
    file: Option<File>,
    dest: PathBuf,
    mtime_ns: i64,
    /// What the file gets once complete: the permissions of the file it replaces, as `cp` keeps them, or a new
    /// file's.
    #[cfg(unix)]
    mode: u32,
    written: u64,
}

impl Temp {
    fn create(parent: &Path, dest: PathBuf, mtime_ns: i64, exec: bool) -> io::Result<Self> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, if exec { 0o777 } else { 0o666 });
        #[cfg(not(unix))]
        let _ = exec;
        let mut attempt = 0;
        loop {
            // Never an existing name: creating it new does not follow a symlink a host may have planted there.
            let path = temp_path(parent, attempt);
            match options.open(&path) {
                Ok(file) => {
                    #[cfg(unix)]
                    let mode = {
                        use std::os::unix::fs::PermissionsExt;
                        // Created empty with a new file's permissions, as the umask leaves them, then made private.
                        let new = file.metadata()?.permissions().mode() & 0o7777;
                        file.set_permissions(fs::Permissions::from_mode(0o600))?;
                        match fs::symlink_metadata(&dest) {
                            Ok(meta) if meta.is_file() => meta.permissions().mode() & 0o7777,
                            _ => new,
                        }
                    };
                    return Ok(Self {
                        path,
                        file: Some(file),
                        dest,
                        mtime_ns,
                        #[cfg(unix)]
                        mode,
                        written: 0,
                    });
                }
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => attempt += 1,
                Err(err) => return Err(err),
            }
        }
    }

    fn write(&mut self, data: &[u8]) -> io::Result<()> {
        if let Some(file) = &mut self.file {
            file.write_all(data)?;
            self.written += data.len() as u64;
        }
        Ok(())
    }

    /// Stamps the host's modification time, which the next fetch compares, and renames the file into place.
    fn persist(mut self) -> io::Result<u64> {
        if let Some(file) = self.file.take() {
            file.set_modified(rules::system_time(self.mtime_ns))?;
            #[cfg(unix)]
            file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(self.mode))?;
        }
        fs::rename(&self.path, &self.dest)?;
        self.path = PathBuf::new();
        Ok(self.written)
    }
}

impl Drop for Temp {
    fn drop(&mut self) {
        self.file = None;
        if !self.path.as_os_str().is_empty() {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// A short hidden name in `parent`, unique to this process and `attempt`, so that it fits wherever the name it
/// stands in for does.
fn temp_path(parent: &Path, attempt: u64) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed) + attempt;
    parent.join(format!(".ovr-{}-{unique}", std::process::id()))
}
