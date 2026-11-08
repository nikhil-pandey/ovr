//! What `ovr get` says on stderr: a progress line on a terminal, what could not be fetched, and a summary.

use std::fmt::Write as _;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use crossterm::cursor::MoveToColumn;
use crossterm::terminal::{Clear, ClearType};

use super::sink::Stats;

/// How often the progress line is redrawn.
const REDRAW: Duration = Duration::from_millis(100);

pub struct Report<W> {
    out: W,
    /// Draw a progress line, which only a terminal can update in place.
    tty: bool,
    drawn: Option<Instant>,
}

impl<W: Write> Report<W> {
    pub fn new(out: W, tty: bool) -> Self {
        Self {
            out,
            tty,
            drawn: None,
        }
    }

    /// Something that was not fetched, or another message for the user.
    pub fn note(&mut self, message: &str) {
        self.clear();
        let _ = writeln!(self.out, "ovr: {message}");
    }

    /// A warning from the host, as it wrote it.
    pub fn warn(&mut self, warning: &[u8]) {
        self.clear();
        let _ = self.out.write_all(warning);
        let _ = self.out.flush();
    }

    /// Redraws the progress line, at most every [`REDRAW`].
    pub fn progress(&mut self, stats: &Stats) {
        if !self.tty || self.drawn.is_some_and(|drawn| drawn.elapsed() < REDRAW) {
            return;
        }
        let _ = crossterm::queue!(self.out, MoveToColumn(0), Clear(ClearType::CurrentLine));
        let _ = write!(
            self.out,
            "fetching: {}, {} so far",
            count(stats.files, "file"),
            crate::bytes(stats.bytes)
        );
        let _ = self.out.flush();
        self.drawn = Some(Instant::now());
    }

    /// Clears the progress line and says what was fetched.
    pub fn summary(&mut self, stats: &Stats, dest: &Path, elapsed: Duration) {
        self.clear();
        let _ = writeln!(self.out, "{}", summary(stats, dest, elapsed));
    }

    fn clear(&mut self) {
        if self.drawn.take().is_some() {
            let _ = crossterm::queue!(self.out, MoveToColumn(0), Clear(ClearType::CurrentLine));
        }
    }
}

/// As in `fetched 37 files (412 unchanged) to out/, 18.2 MiB sent as 2.1 MiB in 0.9 s; 2 failed`.
fn summary(stats: &Stats, dest: &Path, elapsed: Duration) -> String {
    let mut line = format!("fetched {}", count(stats.files, "file"));
    if stats.unchanged > 0 {
        let _ = write!(line, " ({} unchanged)", stats.unchanged);
    }
    let _ = write!(line, " to {}", dest.display());
    if stats.bytes > 0 {
        let _ = write!(
            line,
            ", {} sent as {}",
            crate::bytes(stats.bytes),
            crate::bytes(stats.wire)
        );
    }
    let _ = write!(line, " in {:.1} s", elapsed.as_secs_f64());
    if stats.failed > 0 {
        let _ = write!(line, "; {} failed", stats.failed);
    }
    line
}

fn count(n: u64, what: &str) -> String {
    format!("{n} {what}{}", if n == 1 { "" } else { "s" })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_summary_says_what_was_fetched_and_what_was_not() {
        let stats = Stats {
            files: 37,
            bytes: 18 * 1024 * 1024 + 200 * 1024,
            wire: 2 * 1024 * 1024 + 100 * 1024,
            unchanged: 412,
            failed: 2,
        };
        assert_eq!(
            summary(&stats, Path::new("out/"), Duration::from_millis(900)),
            "fetched 37 files (412 unchanged) to out/, 18.2 MiB sent as 2.1 MiB in 0.9 s; 2 failed"
        );
        let nothing = Stats {
            files: 1,
            ..Stats::default()
        };
        assert_eq!(
            summary(&nothing, Path::new("."), Duration::ZERO),
            "fetched 1 file to . in 0.0 s"
        );
    }

    #[test]
    fn notes_clear_the_progress_line() {
        let mut out = Vec::new();
        let mut report = Report::new(&mut out, true);
        report.progress(&Stats::default());
        report.note("x: not found");
        let printed = String::from_utf8_lossy(&out);
        let (progress, note) = printed.split_once("so far").unwrap_or_default();
        assert!(progress.ends_with("fetching: 0 files, 0 B "), "{printed:?}");
        // Moved back to the start and cleared, then the note on a line of its own.
        assert!(
            note.len() > 1 && note.ends_with("ovr: x: not found\n"),
            "{printed:?}"
        );
    }
}
