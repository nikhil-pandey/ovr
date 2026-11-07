//! A run's output log. Each stream is a series of segment files named by the offset of their first byte, such as
//! `out.0` and `out.33554432`, so offsets count from the start of the run however much has been dropped. Only the
//! last two segments are kept.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use ovr_proto::frame::CHUNK;
use tokio::sync::watch;

/// A new segment starts once the current one holds this much, so a stream keeps between this and twice this.
const SEGMENT: u64 = 32 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    Out,
    Err,
}

impl Stream {
    fn name(self) -> &'static str {
        match self {
            Self::Out => "out",
            Self::Err => "err",
        }
    }
}

/// How much of each stream has been written, and whether that is all.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Progress {
    pub out: u64,
    pub err: u64,
    pub ended: bool,
}

impl Progress {
    fn len(self, stream: Stream) -> u64 {
        match stream {
            Stream::Out => self.out,
            Stream::Err => self.err,
        }
    }

    /// The complete log of a run that has ended, as found on disk.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be read.
    pub fn finished(dir: &Path) -> io::Result<Self> {
        Ok(Self {
            out: length(dir, Stream::Out)?,
            err: length(dir, Stream::Err)?,
            ended: true,
        })
    }
}

/// Appends to one stream of the log.
pub struct Writer {
    dir: PathBuf,
    stream: Stream,
    start: u64,
    len: u64,
    file: File,
}

impl Writer {
    /// Starts an empty stream in `dir`.
    ///
    /// # Errors
    ///
    /// Returns an error when the first segment cannot be created.
    pub fn create(dir: &Path, stream: Stream) -> io::Result<Self> {
        let file = File::create(segment(dir, stream, 0))?;
        Ok(Self {
            dir: dir.to_owned(),
            stream,
            start: 0,
            len: 0,
            file,
        })
    }

    #[must_use]
    pub fn len(&self) -> u64 {
        self.len
    }

    /// # Errors
    ///
    /// Returns an error when the data cannot be written; offsets stay consistent, but the data is lost.
    pub fn append(&mut self, data: &[u8]) -> io::Result<()> {
        if self.len - self.start >= SEGMENT {
            let file = File::create(segment(&self.dir, self.stream, self.len))?;
            let dropped = segments(&self.dir, self.stream)?
                .into_iter()
                .filter(|&start| start < self.start);
            for start in dropped {
                // A reader still on it keeps reading; the file is gone once it lets go.
                let _ = std::fs::remove_file(segment(&self.dir, self.stream, start));
            }
            self.file = file;
            self.start = self.len;
        }
        self.file.write_all(data)?;
        self.len += data.len() as u64;
        Ok(())
    }
}

/// Reads one stream of the log from an offset, following it while it grows.
pub struct Tail {
    dir: PathBuf,
    stream: Stream,
    pos: u64,
    file: Option<File>,
    progress: watch::Receiver<Progress>,
}

impl Tail {
    /// Starts at `from`, or at the earliest offset still kept when that is later, and at most at the end.
    ///
    /// # Errors
    ///
    /// Returns an error when the log cannot be read.
    pub fn open(
        dir: &Path,
        stream: Stream,
        from: u64,
        progress: watch::Receiver<Progress>,
    ) -> io::Result<Self> {
        let len = progress.borrow().len(stream);
        let earliest = segments(dir, stream)?.first().copied().unwrap_or(0);
        Ok(Self {
            dir: dir.to_owned(),
            stream,
            pos: from.clamp(earliest, len.max(earliest)),
            file: None,
            progress,
        })
    }

    /// The offset of the next byte.
    #[must_use]
    pub fn pos(&self) -> u64 {
        self.pos
    }

    /// The next chunk, waiting for one while the stream grows; `None` at the end of a finished stream. Cancel-safe.
    ///
    /// # Errors
    ///
    /// Returns an error when the log cannot be read, including when this reader fell so far behind that the part
    /// it needs was dropped; attaching again resumes from what is kept.
    pub async fn next(&mut self) -> io::Result<Option<Bytes>> {
        loop {
            let progress = *self.progress.borrow_and_update();
            if self.pos < progress.len(self.stream) {
                return self.read().map(Some);
            }
            if progress.ended || self.progress.changed().await.is_err() {
                return Ok(None);
            }
        }
    }

    /// Reads at `pos`, which is before the end, moving on to the next segment when the current one is exhausted.
    fn read(&mut self) -> io::Result<Bytes> {
        let mut buf = vec![0; CHUNK];
        for _ in 0..2 {
            let file = match &mut self.file {
                Some(file) => file,
                None => self.file.insert(self.open_at()?),
            };
            let n = file.read(&mut buf)?;
            if n > 0 {
                buf.truncate(n);
                self.pos += n as u64;
                return Ok(buf.into());
            }
            // The writer has moved on to a segment starting here.
            self.file = None;
        }
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "the log ended early",
        ))
    }

    /// Opens the segment holding `pos`, positioned there.
    fn open_at(&self) -> io::Result<File> {
        let start = segments(&self.dir, self.stream)?
            .into_iter()
            .rev()
            .find(|&start| start <= self.pos)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "that output was dropped"))?;
        let mut file = File::open(segment(&self.dir, self.stream, start))?;
        file.seek(SeekFrom::Start(self.pos - start))?;
        Ok(file)
    }
}

fn segment(dir: &Path, stream: Stream, start: u64) -> PathBuf {
    dir.join(format!("{}.{start}", stream.name()))
}

/// Start offsets of the stream's segments, in order.
fn segments(dir: &Path, stream: Stream) -> io::Result<Vec<u64>> {
    let prefix = format!("{}.", stream.name());
    let mut starts: Vec<u64> = std::fs::read_dir(dir)?
        .filter_map(|entry| {
            let name = entry.ok()?.file_name();
            name.to_str()?.strip_prefix(&prefix)?.parse().ok()
        })
        .collect();
    starts.sort_unstable();
    Ok(starts)
}

/// The total length written to the stream.
fn length(dir: &Path, stream: Stream) -> io::Result<u64> {
    let Some(&start) = segments(dir, stream)?.last() else {
        return Ok(0);
    };
    Ok(start + std::fs::metadata(segment(dir, stream, start))?.len())
}
