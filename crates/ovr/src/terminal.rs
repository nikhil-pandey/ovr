//! The local terminal: raw mode, stdin, resize, and Ctrl-C, turned into [`Input`] events.

use std::io::{IsTerminal, Read};

use tokio::sync::mpsc;

use crate::session::Input;

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

/// Starts forwarding local input. With `tty`, puts the terminal in raw mode so that keys, including Ctrl-C, reach
/// the remote terminal unchanged.
///
/// # Errors
///
/// Returns an error when raw mode cannot be enabled.
pub fn start(tty: bool) -> std::io::Result<(Terminal, mpsc::Receiver<Input>)> {
    let (tx, rx) = mpsc::channel(64);
    let terminal = if tty {
        #[cfg(windows)]
        let console = console::Modes::enable_vt();
        crossterm::terminal::enable_raw_mode()?;
        tokio::spawn(forward_resizes(tx.clone()));
        Terminal {
            raw: true,
            #[cfg(windows)]
            _console: console,
        }
    } else {
        tokio::spawn(forward_interrupts(tx.clone()));
        Terminal {
            raw: false,
            #[cfg(windows)]
            _console: None,
        }
    };
    // Blocking reads of stdin cannot be cancelled; the thread ends with the process.
    std::thread::spawn(move || forward_stdin(&tx));
    Ok((terminal, rx))
}

fn forward_stdin(tx: &mpsc::Sender<Input>) {
    let mut stdin = std::io::stdin().lock();
    let mut buf = vec![0; 16 * 1024];
    loop {
        match stdin.read(&mut buf) {
            Ok(0) => {
                let _ = tx.blocking_send(Input::Eof);
                return;
            }
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
            Err(err) => {
                eprintln!("ovr: reading stdin failed: {err}");
                let _ = tx.blocking_send(Input::Eof);
                return;
            }
            Ok(n) => {
                if tx
                    .blocking_send(Input::Data(bytes::Bytes::copy_from_slice(&buf[..n])))
                    .is_err()
                {
                    return;
                }
            }
        }
    }
}

async fn forward_interrupts(tx: mpsc::Sender<Input>) {
    while tokio::signal::ctrl_c().await.is_ok() {
        if tx.send(Input::Interrupt).await.is_err() {
            return;
        }
    }
}

#[cfg(unix)]
async fn forward_resizes(tx: mpsc::Sender<Input>) {
    use tokio::signal::unix::{SignalKind, signal};
    let Ok(mut winch) = signal(SignalKind::window_change()) else {
        return;
    };
    while winch.recv().await.is_some() {
        let (cols, rows) = size();
        if tx.send(Input::Resize { cols, rows }).await.is_err() {
            return;
        }
    }
}

/// Windows has no resize signal for console applications, so poll.
#[cfg(windows)]
async fn forward_resizes(tx: mpsc::Sender<Input>) {
    let mut last = size();
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
    loop {
        interval.tick().await;
        let current = size();
        if current != last {
            last = current;
            let (cols, rows) = current;
            if tx.send(Input::Resize { cols, rows }).await.is_err() {
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
