//! Wire protocol and local IPC shared by `ovr` and `ovrd`.
//!
//! A stream is a sequence of frames: `len:u32le` covering the rest, `kind:u8`, then the payload. Control frames
//! carry JSON; data frames carry raw bytes.

pub mod fetch;
pub mod frame;
pub mod git;
pub mod ipc;
pub mod lock;
pub mod msg;
pub mod paths;
pub mod retained;

use std::io;

pub use frame::{Frame, FrameKind};

/// Package version of this build. `ovr`, `ovrd`, and every SSH agent run the same version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Agent protocol version. Dial-in hosts may run an older `ovrd`; they are accepted while this matches.
pub const PROTOCOL_VERSION: u32 = 8;

/// FNV-1a: a stable, dependency-free hash for short names derived from paths. Not for anything secret.
#[must_use]
pub fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// `bytes` for people: `512 B`, `1.5 KiB`, `12.3 GiB`.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

/// A duration in seconds for people, in its largest whole unit: `42s`, `3m`, `2h`, `5d`.
#[must_use]
pub fn format_age(seconds: u64) -> String {
    match seconds {
        0..60 => format!("{seconds}s"),
        60..3600 => format!("{}m", seconds / 60),
        3600..86400 => format!("{}h", seconds / 3600),
        _ => format!("{}d", seconds / 86400),
    }
}

/// Lays out `rows`, a header first, in left-aligned columns two spaces apart.
#[must_use]
pub fn columns<const N: usize>(rows: &[[String; N]]) -> Vec<String> {
    let mut widths = [0; N];
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    rows.iter()
        .map(|row| {
            let cells: Vec<String> = row
                .iter()
                .zip(widths)
                .map(|(cell, width)| format!("{cell:width$}"))
                .collect();
            cells.join("  ").trim_end().to_owned()
        })
        .collect()
}

/// Prints one field of a status report: `label` beside the first line, the others aligned under it, or `none`.
pub fn print_field(label: &str, lines: impl IntoIterator<Item = impl std::fmt::Display>) {
    let mut label = label;
    for line in lines {
        println!("{label:<11} {line}");
        label = "";
    }
    if !label.is_empty() {
        println!("{label:<11} none");
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unknown frame kind {0}")]
    UnknownFrame(u8),
    #[error("unexpected {0:?} frame")]
    Unexpected(FrameKind),
    #[error("malformed control frame")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}
