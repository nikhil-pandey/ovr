//! The agent half of `ovr get`: resolve the sources, walk them, and stream every file the client lacks.
//!
//! The client sends `GET` and its manifest in `HAVE` frames without waiting. Walking, reading, and compressing run
//! on a blocking thread a few frames ahead of the network; the stream sends `DATA` only while the [`Pacer`] has
//! room, so a fetch never queues much in front of the runs sharing its connection.

use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::{self, File, Metadata};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use bytes::Bytes;
use futures::SinkExt;
use ovr_proto::fetch::{self as rules, Glob};
use ovr_proto::frame::{CHUNK, Reader, Writer};
use ovr_proto::msg::{Consumed, Done, Entry, EntryKind, Get, Have, Note};
use ovr_proto::{Frame, FrameKind};
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

use super::Config;
use super::checkout::{self, Checkout};
use super::pacer::Pacer;
use super::session::next;
use crate::Error;

/// zstd's default: fast, and close to its best ratio for logs and build outputs.
const LEVEL: i32 = 3;
/// Frames the walker may prepare ahead of the network: a small read-ahead, so disk latency overlaps sending.
const READ_AHEAD: usize = 16;
const READ: usize = 256 * 1024;

/// What the client has: size and modification time by source and path.
type Manifest = HashMap<(u32, String), (u64, i64)>;

/// Serves a `GET`: reads the manifest, then sends the sources' entries, notes, and `DONE`.
///
/// # Errors
///
/// Returns an error for an invalid request, or when the stream breaks.
pub async fn serve<S: AsyncRead + AsyncWrite>(
    writer: &mut Writer<S>,
    frames: &mut Reader<S>,
    config: &Config,
    get: Get,
) -> Result<(), Error> {
    let started = Instant::now();
    checkout::owner(&get.initiator)?;
    // Claimed while reading, so that cleaning leaves the checkout alone; nothing is locked against syncs.
    let found = match &get.checkout {
        Some(name) => Checkout::find(&config.root, &get.initiator, name).await?,
        None => None,
    };
    let base = match &found {
        Some(found) => found.path(&get.cwd)?,
        None => config.base.clone(),
    };
    if found.is_none()
        && let Some(name) = &get.checkout
        && get.sources.iter().any(|source| relative(source))
    {
        let prefix = get.cwd.trim_end_matches('/');
        let instead = if prefix.is_empty() {
            String::new()
        } else {
            format!(", not from {prefix}")
        };
        let warning = format!(
            "ovr: this host has no checkout of {name} yet; reading relative paths from {}{instead}\n",
            base.display()
        );
        writer.send(Frame::new(FrameKind::Err, warning)).await?;
    }
    let have = manifest(frames).await?;
    let listed = have.len();

    let encoder = zstd::stream::write::Encoder::new(Vec::new(), LEVEL)?;
    let home = ovr_proto::paths::home_dir()?;
    let (tx, rx) = mpsc::channel(READ_AHEAD);
    let walker = Walker {
        tx,
        have,
        encoder,
        buf: vec![0; READ],
        source: Source::default(),
        stats: Stats::default(),
    };
    let sources = get.sources.clone();
    let walk = tokio::task::spawn_blocking(move || walker.run(&sources, &base, &home));
    let sent = send(writer, frames, rx).await;
    let stats = walk.await.map_err(io::Error::from)?;
    sent?;
    let stats = stats?;
    tracing::info!(
        sources = ?get.sources,
        checkout = get.checkout.as_deref().unwrap_or_default(),
        listed,
        files = stats.files,
        unchanged = stats.unchanged,
        failed = stats.failed,
        bytes = stats.bytes,
        elapsed_ms = started.elapsed().as_millis(),
        "fetched"
    );
    Ok(())
}

/// Whether `source` is read relative to the checkout or base directory.
fn relative(source: &str) -> bool {
    !source.starts_with('~') && Path::new(source).is_relative()
}

/// Reads `HAVE` frames up to the empty one that ends them.
async fn manifest<S: AsyncRead>(frames: &mut Reader<S>) -> Result<Manifest, Error> {
    let mut have = Manifest::new();
    loop {
        let batch: Vec<Have> = next(frames).await?.expect(FrameKind::Have)?;
        if batch.is_empty() {
            return Ok(have);
        }
        for file in batch {
            have.insert((file.source, file.path), (file.size, file.mtime_ns));
        }
    }
}

/// Sends what the walker prepares, holding `DATA` back while the pacer has no room, and reading the client's
/// `CONSUMED` frames all along.
async fn send<S: AsyncRead + AsyncWrite>(
    writer: &mut Writer<S>,
    frames: &mut Reader<S>,
    mut prepared: mpsc::Receiver<Frame>,
) -> Result<(), Error> {
    let mut pacer = Pacer::default();
    let consumed = |frame: Frame| frame.expect::<Consumed>(FrameKind::Consumed);
    loop {
        let frame = tokio::select! {
            biased;
            frame = next(frames) => {
                pacer.acked(consumed(frame?)?.bytes, Instant::now());
                continue;
            }
            frame = prepared.recv() => match frame {
                Some(frame) => frame,
                None => return Ok(()),
            },
        };
        let len = frame.payload.len() as u64;
        let data = frame.kind == FrameKind::Data && len > 0;
        while data && !pacer.has_room() {
            pacer.acked(consumed(next(frames).await?)?.bytes, Instant::now());
        }
        writer.send(frame).await?;
        if data {
            pacer.sent(len, Instant::now());
        }
    }
}

#[derive(Debug, Default)]
struct Stats {
    files: u64,
    unchanged: u64,
    bytes: u64,
    failed: u64,
}

/// Why a walk stopped early.
enum Stop {
    /// The stream is gone; nobody is listening.
    Gone,
    /// Compression failed, so nothing more can be sent that the client could decode.
    Failed(io::Error),
}

/// The source being walked.
#[derive(Debug, Default)]
struct Source {
    index: u32,
    /// As given, to name it in notes.
    text: String,
    /// Prepended to an entry's path to name it in notes.
    label: String,
}

/// Walks the sources on a blocking thread and prepares the frames that carry them.
struct Walker {
    tx: mpsc::Sender<Frame>,
    have: Manifest,
    /// One zstd stream for every file, flushed at the end of each, so similar files compress together.
    encoder: zstd::stream::write::Encoder<'static, Vec<u8>>,
    buf: Vec<u8>,
    source: Source,
    stats: Stats,
}

impl Walker {
    fn run(mut self, sources: &[String], base: &Path, home: &Path) -> io::Result<Stats> {
        let walked = sources.iter().enumerate().try_for_each(|(index, text)| {
            self.source = Source {
                index: u32::try_from(index).unwrap_or(u32::MAX),
                text: text.clone(),
                label: format!("{}/", text.trim_end_matches(['/', '\\'])),
            };
            self.walk(base, home)
        });
        let done = Done {
            unchanged: self.stats.unchanged,
        };
        match walked.and_then(|()| self.json(FrameKind::Done, &done)) {
            Ok(()) | Err(Stop::Gone) => Ok(self.stats),
            Err(Stop::Failed(err)) => Err(err),
        }
    }

    fn walk(&mut self, base: &Path, home: &Path) -> Result<(), Stop> {
        let text = self.source.text.clone();
        let glob = match Glob::parse(&text, cfg!(windows)) {
            Ok(glob) => glob,
            Err(why) => return self.note("", &format!("invalid glob: {why}"), true),
        };
        if let Some(glob) = glob {
            return self.glob(&glob, base, home);
        }
        // A named source follows symlinks, as `cp -RH` does: Bazel's output directories are symlinks.
        let path = resolve(&text, base, home);
        match fs::metadata(&path) {
            Ok(meta) => self.node(&path, "", &meta),
            Err(err) => self.note("", &unreadable(&err, &path), true),
        }
    }

    fn glob(&mut self, glob: &Glob, base: &Path, home: &Path) -> Result<(), Stop> {
        glob.prefix.clone_into(&mut self.source.label);
        let dir = if glob.prefix.is_empty() {
            base.to_owned()
        } else {
            resolve(&glob.prefix, base, home)
        };
        match fs::metadata(&dir) {
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => {
                let why = format!("{} is not a directory", dir.display());
                return self.note("", &why, true);
            }
            Err(err) => return self.note("", &unreadable(&err, &dir), true),
        }
        let mut matched = 0;
        self.matches(glob, &dir, "", 0, &mut matched)?;
        if matched == 0 {
            let why = format!("matched nothing in {}", dir.display());
            return self.note("", &why, true);
        }
        Ok(())
    }

    /// Sends what matches `glob` in `dir`, `depth` components below its prefix, whole, and looks for more below
    /// what does not.
    fn matches(
        &mut self,
        glob: &Glob,
        dir: &Path,
        rel: &str,
        depth: usize,
        matched: &mut u64,
    ) -> Result<(), Stop> {
        for (name, path) in self.children(dir, rel)? {
            if rules::is_git(&name) {
                continue;
            }
            let rel = join(rel, &name);
            let meta = match fs::symlink_metadata(&path) {
                Ok(meta) => meta,
                Err(err) => {
                    self.note(&rel, &unreadable(&err, &path), true)?;
                    continue;
                }
            };
            if glob.matches(&rel) {
                *matched += 1;
                self.node(&path, &rel, &meta)?;
            } else if meta.is_dir() && glob.descends(depth + 1) {
                self.matches(glob, &path, &rel, depth + 1, matched)?;
            }
        }
        Ok(())
    }

    fn node(&mut self, path: &Path, rel: &str, meta: &Metadata) -> Result<(), Stop> {
        let kind = meta.file_type();
        if kind.is_dir() {
            self.dir(path, rel)
        } else if kind.is_file() {
            self.file(path, rel, meta)
        } else if kind.is_symlink() {
            self.symlink(path, rel)
        } else {
            self.note(rel, "skipped: not a file, directory, or symlink", false)
        }
    }

    /// Sends a directory and everything below it. Symlinks inside are sent as symlinks, never followed.
    fn dir(&mut self, path: &Path, rel: &str) -> Result<(), Stop> {
        self.entry(rel, EntryKind::Dir)?;
        for (name, child) in self.children(path, rel)? {
            let rel = join(rel, &name);
            if rules::is_git(&name) {
                self.note(&rel, "skipped: .git directories are never fetched", false)?;
                continue;
            }
            match fs::symlink_metadata(&child) {
                Ok(meta) => self.node(&child, &rel, &meta)?,
                Err(err) => self.note(&rel, &unreadable(&err, &child), true)?,
            }
        }
        Ok(())
    }

    /// The UTF-8 names and paths in `dir`, sorted; others are noted and left out.
    fn children(&mut self, dir: &Path, rel: &str) -> Result<Vec<(String, PathBuf)>, Stop> {
        let listed: io::Result<Vec<(OsString, PathBuf)>> = fs::read_dir(dir).and_then(|entries| {
            entries
                .map(|entry| entry.map(|entry| (entry.file_name(), entry.path())))
                .collect()
        });
        let mut listed = match listed {
            Ok(listed) => listed,
            Err(err) => {
                self.note(rel, &unreadable(&err, dir), true)?;
                return Ok(Vec::new());
            }
        };
        listed.sort();
        let mut children = Vec::with_capacity(listed.len());
        for (name, path) in listed {
            match name.into_string() {
                Ok(name) => children.push((name, path)),
                Err(name) => {
                    let why = format!("skipped {}: the name is not UTF-8", name.to_string_lossy());
                    self.note(rel, &why, true)?;
                }
            }
        }
        Ok(children)
    }

    /// Sends a file the client lacks: its entry, its content as read, and the empty `DATA` that ends it.
    fn file(&mut self, path: &Path, rel: &str, meta: &Metadata) -> Result<(), Stop> {
        let key = (self.source.index, rel.to_owned());
        let unchanged = self.have.get(&key).is_some_and(|&(size, mtime)| {
            size == meta.len() && rules::same_mtime(modified(meta), mtime)
        });
        if unchanged {
            self.stats.unchanged += 1;
            return Ok(());
        }
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(err) => return self.note(rel, &unreadable(&err, path), true),
        };
        let meta = file.metadata().unwrap_or_else(|_| meta.clone());
        let kind = EntryKind::File {
            size: meta.len(),
            mtime_ns: modified(&meta),
            exec: executable(&meta),
        };
        self.entry(rel, kind)?;
        loop {
            let read = match file.read(&mut self.buf) {
                Ok(0) => break,
                Ok(read) => read,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => {
                    // The client discards the file on hearing why; what it has decoded so far stays in step.
                    self.flush()?;
                    return self.note(rel, &format!("could not be read to the end: {err}"), true);
                }
            };
            self.stats.bytes += read as u64;
            self.encoder
                .write_all(&self.buf[..read])
                .map_err(Stop::Failed)?;
            if self.encoder.get_ref().len() >= CHUNK {
                self.drain()?;
            }
        }
        self.flush()?;
        self.emit(Frame::empty(FrameKind::Data))?;
        self.stats.files += 1;
        Ok(())
    }

    fn symlink(&mut self, path: &Path, rel: &str) -> Result<(), Stop> {
        match fs::read_link(path).map(PathBuf::into_os_string) {
            Ok(target) => match target.into_string() {
                Ok(target) => self.entry(rel, EntryKind::Symlink { target }),
                Err(_) => self.note(rel, "skipped: the symlink's target is not UTF-8", true),
            },
            Err(err) => self.note(rel, &unreadable(&err, path), true),
        }
    }

    /// Ends the compressed block, so the client can decode everything sent so far, and sends it.
    fn flush(&mut self) -> Result<(), Stop> {
        self.encoder.flush().map_err(Stop::Failed)?;
        self.drain()
    }

    /// Sends the compressed bytes ready so far, in frames of at most [`CHUNK`].
    fn drain(&mut self) -> Result<(), Stop> {
        let mut out = Bytes::from(std::mem::take(self.encoder.get_mut()));
        while !out.is_empty() {
            let frame = out.split_to(out.len().min(CHUNK));
            self.emit(Frame::new(FrameKind::Data, frame))?;
        }
        Ok(())
    }

    fn entry(&self, rel: &str, kind: EntryKind) -> Result<(), Stop> {
        let entry = Entry {
            source: self.source.index,
            path: rel.to_owned(),
            kind,
        };
        self.json(FrameKind::Entry, &entry)
    }

    /// Tells the client about something of `rel` it does not get.
    fn note(&mut self, rel: &str, why: &str, failed: bool) -> Result<(), Stop> {
        self.stats.failed += u64::from(failed);
        let what = if rel.is_empty() {
            self.source.text.clone()
        } else {
            format!("{}{rel}", self.source.label)
        };
        let note = Note {
            source: self.source.index,
            message: format!("{what}: {why}"),
            failed,
        };
        self.json(FrameKind::Note, &note)
    }

    fn json<T: Serialize>(&self, kind: FrameKind, message: &T) -> Result<(), Stop> {
        let frame =
            Frame::json(kind, message).map_err(|err| Stop::Failed(io::Error::other(err)))?;
        self.emit(frame)
    }

    fn emit(&self, frame: Frame) -> Result<(), Stop> {
        self.tx.blocking_send(frame).map_err(|_| Stop::Gone)
    }
}

/// Where `source` is on the host: `~` is the home directory, a relative path starts from `base`, and an absolute
/// one is taken as is, by the host's rules. There is no shell.
fn resolve(source: &str, base: &Path, home: &Path) -> PathBuf {
    if source == "~" {
        return home.to_owned();
    }
    let below_home = source
        .strip_prefix("~/")
        .or_else(|| source.strip_prefix("~\\").filter(|_| cfg!(windows)));
    match below_home {
        Some(rest) => home.join(rest),
        None => base.join(source),
    }
}

fn join(rel: &str, name: &str) -> String {
    if rel.is_empty() {
        name.to_owned()
    } else {
        format!("{rel}/{name}")
    }
}

/// Why `path` could not be read, naming where it was looked for.
fn unreadable(err: &io::Error, path: &Path) -> String {
    let path = path.display();
    match err.kind() {
        io::ErrorKind::NotFound => format!("not found on the host at {path}"),
        io::ErrorKind::PermissionDenied => format!("permission denied at {path}"),
        _ => format!("cannot read {path}: {err}"),
    }
}

fn modified(meta: &Metadata) -> i64 {
    meta.modified().map(rules::mtime_ns).unwrap_or_default()
}

#[cfg(unix)]
fn executable(meta: &Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn executable(_: &Metadata) -> bool {
    false
}
