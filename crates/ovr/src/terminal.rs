//! The local terminal: raw mode, stdin, resize, and Ctrl-C, turned into input and [`Signal`]s.

use std::io::{IsTerminal, Read};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::session::Signal;

/// What a terminal sends for Ctrl-C in raw mode.
const CTRL_C: u8 = 0x03;

/// Restores the terminal when dropped.
pub struct Terminal {
    raw: bool,
    #[cfg(windows)]
    _console: Option<console::Modes>,
}

impl Drop for Terminal {
    fn drop(&mut self) {
        if self.raw {
            let _ = crossterm::terminal::disable_raw_mode();
        }
    }
}

/// Whether the run should get a pseudo-terminal: only when both stdin and stdout are terminals.
#[must_use]
pub fn is_interactive() -> bool {
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

/// The local terminal size, or 80x24 when there is none.
#[must_use]
pub fn size() -> (u16, u16) {
    crossterm::terminal::size().unwrap_or((80, 24))
}

/// Starts forwarding local input and signals. With `tty`, puts the terminal in raw mode so that keys reach the
/// remote terminal unchanged; Ctrl-C is then read as a key and sent as an interrupt signal.
///
/// # Errors
///
/// Returns an error when raw mode cannot be enabled.
pub fn start(
    tty: bool,
) -> std::io::Result<(Terminal, mpsc::Receiver<Bytes>, mpsc::Receiver<Signal>)> {
    let (signals_tx, signals) = mpsc::channel(16);
    let (stdin_tx, stdin) = mpsc::channel(4);
    let terminal = if tty {
        #[cfg(windows)]
        let console = console::Modes::enable_vt();
        crossterm::terminal::enable_raw_mode()?;
        tokio::spawn(forward_resizes(signals_tx.clone()));
        // Typed input is queued without limit so the reader never stalls and always sees the next Ctrl-C.
        let (typed_tx, typed) = mpsc::unbounded_channel();
        tokio::spawn(forward_typed(typed, stdin_tx));
        std::thread::spawn(move || {
            read_stdin(
                true,
                |data| typed_tx.send(data).is_ok(),
                |signal| signals_tx.blocking_send(signal).is_ok(),
            );
        });
        Terminal {
            raw: true,
            #[cfg(windows)]
            _console: console,
        }
    } else {
        tokio::spawn(forward_interrupts(signals_tx));
        std::thread::spawn(move || {
            read_stdin(false, |data| stdin_tx.blocking_send(data).is_ok(), |_| true);
        });
        Terminal {
            raw: false,
            #[cfg(windows)]
            _console: None,
        }
    };
    Ok((terminal, stdin, signals))
}

/// Reads stdin until it ends, passing data to `data`. From a raw terminal (`raw`), Ctrl-C is cut out and passed to
/// `signal` instead. Blocking reads cannot be cancelled, so this runs on its own thread, which ends with the process.
fn read_stdin(raw: bool, data: impl Fn(Bytes) -> bool, signal: impl Fn(Signal) -> bool) {
    let mut stdin = std::io::stdin().lock();
    let mut buf = vec![0; 16 * 1024];
    loop {
        let n = match stdin.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => n,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => {
                eprintln!("ovr: reading stdin failed: {err}");
                return;
            }
        };
        let mut rest = &buf[..n];
        while !rest.is_empty() {
            let cut = raw
                .then(|| rest.iter().position(|&b| b == CTRL_C))
                .flatten();
            let (chunk, after) = cut.map_or((rest, &[][..]), |at| (&rest[..at], &rest[at + 1..]));
            if !chunk.is_empty() && !data(Bytes::copy_from_slice(chunk)) {
                return;
            }
            if cut.is_some() && !signal(Signal::Interrupt) {
                return;
            }
            rest = after;
        }
    }
}

async fn forward_typed(mut typed: mpsc::UnboundedReceiver<Bytes>, stdin: mpsc::Sender<Bytes>) {
    while let Some(data) = typed.recv().await {
        if stdin.send(data).await.is_err() {
            return;
        }
    }
}

async fn forward_interrupts(tx: mpsc::Sender<Signal>) {
    while tokio::signal::ctrl_c().await.is_ok() {
        if tx.send(Signal::Interrupt).await.is_err() {
            return;
        }
    }
}

#[cfg(unix)]
async fn forward_resizes(tx: mpsc::Sender<Signal>) {
    use tokio::signal::unix::{SignalKind, signal};
    let Ok(mut winch) = signal(SignalKind::window_change()) else {
        return;
    };
    while winch.recv().await.is_some() {
        let (cols, rows) = size();
        if tx.send(Signal::Resize { cols, rows }).await.is_err() {
            return;
        }
    }
}

/// Windows has no resize signal for console applications, so poll.
#[cfg(windows)]
async fn forward_resizes(tx: mpsc::Sender<Signal>) {
    let mut last = size();
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
    loop {
        interval.tick().await;
        let current = size();
        if current != last {
            last = current;
            let (cols, rows) = current;
            if tx.send(Signal::Resize { cols, rows }).await.is_err() {
                return;
            }
        }
    }
}

#[cfg(windows)]
#[allow(unsafe_code)]
mod console {
    use windows_sys::Win32::System::Console::{
        CONSOLE_MODE, ENABLE_VIRTUAL_TERMINAL_INPUT, ENABLE_VIRTUAL_TERMINAL_PROCESSING,
        GetConsoleMode, GetStdHandle, STD_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
        SetConsoleMode,
    };

    /// Console modes to restore on drop.
    pub struct Modes {
        input: CONSOLE_MODE,
        output: CONSOLE_MODE,
    }

    impl Modes {
        /// Makes the console speak VT sequences in both directions, as remote terminals expect.
        pub fn enable_vt() -> Option<Self> {
            let input = get(STD_INPUT_HANDLE)?;
            let output = get(STD_OUTPUT_HANDLE)?;
            set(STD_INPUT_HANDLE, input | ENABLE_VIRTUAL_TERMINAL_INPUT);
            set(
                STD_OUTPUT_HANDLE,
                output | ENABLE_VIRTUAL_TERMINAL_PROCESSING,
            );
            Some(Self { input, output })
        }
    }

    impl Drop for Modes {
        fn drop(&mut self) {
            set(STD_INPUT_HANDLE, self.input);
            set(STD_OUTPUT_HANDLE, self.output);
        }
    }

    fn get(which: STD_HANDLE) -> Option<CONSOLE_MODE> {
        let mut mode = 0;
        // SAFETY: GetStdHandle has no preconditions, and GetConsoleMode only writes to the valid `mode` pointer.
        let ok = unsafe { GetConsoleMode(GetStdHandle(which), &raw mut mode) };
        (ok != 0).then_some(mode)
    }

    fn set(which: STD_HANDLE, mode: CONSOLE_MODE) {
        // SAFETY: GetStdHandle has no preconditions; SetConsoleMode takes the handle and mode by value.
        unsafe { SetConsoleMode(GetStdHandle(which), mode) };
    }
}
