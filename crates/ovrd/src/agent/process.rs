//! Running the remote command, under a pseudo-terminal or with plain pipes, and tearing down its whole process
//! tree if the run is abandoned.

use std::ffi::OsString;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ovr_proto::frame::CHUNK;
use ovr_proto::msg::Exec;
use portable_pty::{ChildKiller, CommandBuilder, MasterPty, PtySize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

/// How long output may keep draining after the command exits, for descendants still holding the terminal or pipes.
const DRAIN: Duration = Duration::from_secs(1);

#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    /// Always the last event, after all output.
    Exit(i32),
}

/// How commands are started on this host.
#[derive(Debug, Clone)]
pub struct Shell {
    program: PathBuf,
}

impl Shell {
    /// The user's login shell on Unix, the command interpreter on Windows.
    #[must_use]
    pub fn detect() -> Self {
        #[cfg(unix)]
        let program =
            std::env::var_os("SHELL").map_or_else(|| PathBuf::from("/bin/sh"), PathBuf::from);
        #[cfg(windows)]
        let program =
            std::env::var_os("COMSPEC").map_or_else(|| PathBuf::from("cmd.exe"), PathBuf::from);
        Self { program }
    }

    #[must_use]
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
        }
    }

    /// The full argument vector for `argv`; an empty `argv` starts the shell itself.
    ///
    /// On Unix the command runs through a login shell so that it sees the same `PATH` and toolchains as an SSH
    /// login; the shell `exec`s it, so signals and exit codes are the command's own. Shells without POSIX syntax
    /// fall back to `/bin/sh` for this.
    fn argv(&self, argv: &[String]) -> Vec<OsString> {
        let mut full = Vec::new();
        if argv.is_empty() {
            full.push(self.program.clone().into_os_string());
            if cfg!(unix) {
                full.push("-l".into());
            }
            return full;
        }
        if cfg!(unix) {
            let posix = self
                .program
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| matches!(name, "sh" | "bash" | "zsh" | "dash" | "ksh"));
            let shell = if posix {
                self.program.clone()
            } else {
                PathBuf::from("/bin/sh")
            };
            full.extend([
                shell.into_os_string(),
                "-l".into(),
                "-c".into(),
                r#"exec "$@""#.into(),
                "ovr".into(),
            ]);
        }
        full.extend(argv.iter().map(OsString::from));
        full
    }
}

/// A running command.
pub struct Process {
    events: mpsc::Receiver<Event>,
    input: Option<mpsc::Sender<Vec<u8>>>,
    control: Control,
    exited: Arc<AtomicBool>,
}

enum Control {
    Pipe {
        #[cfg(unix)]
        pid: Option<u32>,
        #[cfg(windows)]
        job: Arc<job::Job>,
    },
    Pty {
        master: Arc<Mutex<Option<Box<dyn MasterPty + Send>>>>,
        killer: Box<dyn ChildKiller + Send + Sync>,
        #[cfg(unix)]
        pid: Option<u32>,
        #[cfg(windows)]
        _job: Option<job::Job>,
    },
}

impl Process {
    /// Starts `exec` in `cwd`.
    ///
    /// # Errors
    ///
    /// Returns an error when the command or its terminal cannot be created.
    pub fn spawn(shell: &Shell, exec: &Exec, cwd: &Path) -> io::Result<Self> {
        let argv = shell.argv(&exec.argv);
        if exec.tty {
            Self::spawn_pty(argv, exec, cwd)
        } else {
            Self::spawn_pipes(&argv, cwd)
        }
    }

    fn spawn_pipes(argv: &[OsString], cwd: &Path) -> io::Result<Self> {
        let (program, rest) = argv.split_first().ok_or(io::ErrorKind::InvalidInput)?;
        let mut command = tokio::process::Command::new(program);
        command
            .args(rest)
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn()?;
        #[cfg(windows)]
        let job = {
            let job = job::Job::new()?;
            if let Some(handle) = child.raw_handle() {
                job.assign(handle)?;
            }
            Arc::new(job)
        };

        let (events_tx, events) = mpsc::channel(16);
        let (input_tx, mut input_rx) = mpsc::channel::<Vec<u8>>(16);
        if let Some(mut stdin) = child.stdin.take() {
            tokio::spawn(async move {
                while let Some(data) = input_rx.recv().await {
                    if stdin.write_all(&data).await.is_err() {
                        return;
                    }
                }
            });
        }
        let stdout = child
            .stdout
            .take()
            .map(|pipe| tokio::spawn(forward(pipe, events_tx.clone(), Event::Stdout)));
        let stderr = child
            .stderr
            .take()
            .map(|pipe| tokio::spawn(forward(pipe, events_tx.clone(), Event::Stderr)));

        #[cfg(unix)]
        let pid = child.id();
        let exited = Arc::new(AtomicBool::new(false));
        let done = Arc::clone(&exited);
        tokio::spawn(async move {
            let code = child.wait().await.map_or(1, exit_code);
            done.store(true, Ordering::SeqCst);
            let drained = async {
                for task in [stdout, stderr].into_iter().flatten() {
                    let _ = task.await;
                }
            };
            let _ = tokio::time::timeout(DRAIN, drained).await;
            let _ = events_tx.send(Event::Exit(code)).await;
        });

        Ok(Self {
            events,
            input: Some(input_tx),
            control: Control::Pipe {
                #[cfg(unix)]
                pid,
                #[cfg(windows)]
                job,
            },
            exited,
        })
    }

    fn spawn_pty(argv: Vec<OsString>, exec: &Exec, cwd: &Path) -> io::Result<Self> {
        let pair = portable_pty::native_pty_system()
            .openpty(PtySize {
                rows: exec.rows,
                cols: exec.cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(io::Error::other)?;
        let mut command = CommandBuilder::from_argv(argv);
        command.cwd(cwd);
        command.env("TERM", "xterm-256color");
        let mut child = pair
            .slave
            .spawn_command(command)
            .map_err(io::Error::other)?;
        drop(pair.slave);
        #[cfg(windows)]
        let job = {
            let job = job::Job::new()?;
            child
                .as_raw_handle()
                .map(|handle| job.assign(handle))
                .transpose()?;
            Some(job)
        };
        let killer = child.clone_killer();
        #[cfg(unix)]
        let pid = child.process_id();
        let mut reader = pair.master.try_clone_reader().map_err(io::Error::other)?;
        let mut writer = pair.master.take_writer().map_err(io::Error::other)?;
        let master = Arc::new(Mutex::new(Some(pair.master)));

        let (events_tx, events) = mpsc::channel(16);
        let (input_tx, mut input_rx) = mpsc::channel::<Vec<u8>>(16);
        std::thread::spawn(move || {
            while let Some(data) = input_rx.blocking_recv() {
                if writer
                    .write_all(&data)
                    .and_then(|()| writer.flush())
                    .is_err()
                {
                    return;
                }
            }
        });
        let (eof_tx, eof_rx) = oneshot::channel::<()>();
        let output = events_tx.clone();
        std::thread::spawn(move || {
            let mut buf = vec![0; CHUNK];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if output
                            .blocking_send(Event::Stdout(buf[..n].to_vec()))
                            .is_err()
                        {
                            break;
                        }
                    }
                }
            }
            drop(eof_tx);
        });

        let exited = Arc::new(AtomicBool::new(false));
        let done = Arc::clone(&exited);
        let closing = Arc::clone(&master);
        tokio::spawn(async move {
            let status = tokio::task::spawn_blocking(move || child.wait()).await;
            done.store(true, Ordering::SeqCst);
            let code = match status {
                Ok(Ok(status)) => pty_exit_code(&status),
                _ => 1,
            };
            // Closing the terminal ends the output stream once it is drained; ConPTY needs this to report EOF.
            let master = closing
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            let _ = tokio::task::spawn_blocking(move || drop(master)).await;
            let _ = tokio::time::timeout(DRAIN, eof_rx).await;
            let _ = events_tx.send(Event::Exit(code)).await;
        });

        Ok(Self {
            events,
            input: Some(input_tx),
            control: Control::Pty {
                master,
                killer,
                #[cfg(unix)]
                pid,
                #[cfg(windows)]
                _job: job,
            },
            exited,
        })
    }

    /// The next output or exit event; `None` after [`Event::Exit`].
    pub async fn next(&mut self) -> Option<Event> {
        self.events.recv().await
    }

    /// Where input goes, until [`Process::close_input`].
    #[must_use]
    pub fn input(&self) -> Option<&mpsc::Sender<Vec<u8>>> {
        self.input.as_ref()
    }

    /// Ends the command's stdin. A terminal has no end of input, so this only applies to pipes.
    pub fn close_input(&mut self) {
        if matches!(self.control, Control::Pipe { .. }) {
            self.input = None;
        }
    }

    pub fn resize(&self, cols: u16, rows: u16) {
        if let Control::Pty { master, .. } = &self.control
            && let Some(master) = master
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_ref()
        {
            let _ = master.resize(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            });
        }
    }

    /// Interrupts the command as Ctrl-C would: `SIGINT` to its process group on Unix. Windows has no way to deliver
    /// Ctrl-C to a process without a shared console, so the job is terminated with the Ctrl-C exit status instead.
    pub fn interrupt(&self) {
        match &self.control {
            Control::Pty { .. } => {
                if let Some(input) = &self.input {
                    let _ = input.try_send(vec![0x03]);
                }
            }
            #[cfg(unix)]
            Control::Pipe { pid } => signal_group(*pid, nix::sys::signal::Signal::SIGINT),
            #[cfg(windows)]
            Control::Pipe { job } => job.terminate(job::CONTROL_C_EXIT),
        }
    }
}

impl Drop for Process {
    /// An abandoned run takes its whole process tree with it. Descendants of a command that exited normally, such as
    /// build servers, are left alone.
    fn drop(&mut self) {
        if self.exited.load(Ordering::SeqCst) {
            return;
        }
        match &mut self.control {
            #[cfg(unix)]
            Control::Pipe { pid } => signal_group(*pid, nix::sys::signal::Signal::SIGKILL),
            #[cfg(windows)]
            Control::Pipe { job } => job.terminate(1),
            Control::Pty {
                killer,
                #[cfg(unix)]
                pid,
                ..
            } => {
                #[cfg(unix)]
                signal_group(*pid, nix::sys::signal::Signal::SIGKILL);
                let _ = killer.kill();
            }
        }
    }
}

async fn forward<R: AsyncRead + Unpin>(
    mut pipe: R,
    events: mpsc::Sender<Event>,
    wrap: fn(Vec<u8>) -> Event,
) {
    let mut buf = vec![0; CHUNK];
    loop {
        match pipe.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                if events.send(wrap(buf[..n].to_vec())).await.is_err() {
                    return;
                }
            }
        }
    }
}

#[cfg(unix)]
fn signal_group(pid: Option<u32>, signal: nix::sys::signal::Signal) {
    if let Some(pid) = pid.and_then(|pid| i32::try_from(pid).ok()) {
        let _ = nix::sys::signal::killpg(nix::unistd::Pid::from_raw(pid), signal);
    }
}

/// The exit code a shell would report: the status, or 128 plus the signal that killed the command.
fn exit_code(status: std::process::ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return 128 + signal;
        }
    }
    status.code().unwrap_or(1)
}

fn pty_exit_code(status: &portable_pty::ExitStatus) -> i32 {
    #[cfg(unix)]
    if let Some(name) = status.signal() {
        return signal::number(name).map_or(1, |signal| 128 + signal);
    }
    #[allow(clippy::cast_possible_wrap)]
    // Windows exit codes are u32 NTSTATUS values, reported as i32 like cmd does
    let code = status.exit_code() as i32;
    code
}

#[cfg(unix)]
#[allow(unsafe_code)]
mod signal {
    use std::ffi::CStr;

    /// Recovers a signal number from the description `portable-pty` reports, which comes from `strsignal`.
    pub fn number(description: &str) -> Option<i32> {
        (1..32).find(|&signal| {
            // SAFETY: strsignal accepts any signal number and returns null or a NUL-terminated string that stays
            // valid until the next call; it is copied before this closure returns.
            let name = unsafe { libc::strsignal(signal) };
            // SAFETY: `name` is non-null and NUL-terminated, per above.
            !name.is_null() && unsafe { CStr::from_ptr(name) }.to_string_lossy() == description
        })
    }
}

#[cfg(windows)]
#[allow(unsafe_code)]
mod job {
    use std::io;
    use std::os::windows::io::RawHandle;

    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, TerminateJobObject,
    };

    /// `STATUS_CONTROL_C_EXIT`, the exit status of a console process ended by Ctrl-C.
    pub const CONTROL_C_EXIT: u32 = 0xC000_013A;

    /// A Job Object holding a command and every process it starts.
    pub struct Job(HANDLE);

    // SAFETY: a job handle may be used from any thread.
    unsafe impl Send for Job {}
    // SAFETY: the job APIs used here are thread-safe.
    unsafe impl Sync for Job {}

    impl Job {
        pub fn new() -> io::Result<Self> {
            // SAFETY: null attributes and name create an anonymous job with default security.
            let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
            if handle.is_null() {
                Err(io::Error::last_os_error())
            } else {
                Ok(Self(handle))
            }
        }

        pub fn assign(&self, process: RawHandle) -> io::Result<()> {
            // SAFETY: both handles are valid for the duration of the call.
            if unsafe { AssignProcessToJobObject(self.0, process) } == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }

        pub fn terminate(&self, code: u32) {
            // SAFETY: the job handle is valid until drop.
            unsafe { TerminateJobObject(self.0, code) };
        }
    }

    impl Drop for Job {
        fn drop(&mut self) {
            // SAFETY: the handle is owned and closed exactly once.
            unsafe { CloseHandle(self.0) };
        }
    }
}
