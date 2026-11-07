//! Running the remote command, under a pseudo-terminal or with plain pipes, and tearing down its whole process
//! tree if the run is abandoned.

use std::ffi::OsString;
use std::io;
#[cfg(windows)]
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
#[cfg(windows)]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use ovr_proto::frame::CHUNK;
use ovr_proto::msg::Exec;
use portable_pty::{CommandBuilder, MasterPty, PtySize};
use process_wrap::tokio::{ChildWrapper, CommandWrap};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
#[cfg(windows)]
use tokio::sync::oneshot;
use tokio::sync::{mpsc, watch};

/// Once the command exits, output keeps draining until nothing has arrived for this long. Descendants such as build
/// servers may hold the terminal or pipes open indefinitely; a slow client, by contrast, never cuts output short.
const DRAIN: Duration = Duration::from_secs(1);

#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    Stdout(Bytes),
    Stderr(Bytes),
    /// Always the last event, after all output.
    Exit(i32),
    /// The command's status could not be collected; also the last event.
    Lost(String),
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

/// A running command. Dropping it before the command exits kills the command's whole process tree, synchronously,
/// so that nothing outlives an abandoned run even when the runtime is shutting down.
pub struct Process {
    input: Option<mpsc::Sender<Bytes>>,
    control: Control,
    exited: watch::Receiver<bool>,
}

/// The command's output, ending with its exit.
pub struct Output(mpsc::Receiver<Event>);

impl Output {
    /// The next output or exit event; `None` after the last one.
    pub async fn next(&mut self) -> Option<Event> {
        self.0.recv().await
    }
}

enum Control {
    Pipe {
        /// Delivered by the task that waits for the command, which owns it.
        signals: mpsc::UnboundedSender<Signal>,
        #[cfg(unix)]
        group: Option<u32>,
    },
    Pty {
        master: Arc<Mutex<Option<Box<dyn MasterPty + Send>>>>,
        /// Written ahead of pending input.
        urgent: mpsc::UnboundedSender<Bytes>,
        #[cfg(unix)]
        group: Option<u32>,
        #[cfg(windows)]
        job: Arc<job::Job>,
    },
}

impl Process {
    /// Starts `exec` in `cwd`.
    ///
    /// # Errors
    ///
    /// Returns an error when the command or its terminal cannot be created.
    pub fn spawn(shell: &Shell, exec: &Exec, cwd: &Path) -> io::Result<(Self, Output)> {
        let argv = shell.argv(&exec.argv);
        if exec.tty {
            spawn_pty(argv, exec, cwd)
        } else {
            spawn_pipes(&argv, cwd)
        }
    }

    /// Where input goes, until [`Process::close_input`]. Input is taken one chunk at a time, so a sent chunk is
    /// about to be written to the command.
    #[must_use]
    pub fn input(&self) -> Option<&mpsc::Sender<Bytes>> {
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
            let resized = master.resize(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            });
            if let Err(err) = resized {
                tracing::warn!(%err, "cannot resize the terminal");
            }
        }
    }

    /// Interrupts the command as Ctrl-C would: `^C` on its terminal, or `SIGINT` to its process group on Unix.
    /// Windows has no way to deliver Ctrl-C to a process without a shared console, so the job is terminated instead.
    pub fn interrupt(&self) {
        match &self.control {
            Control::Pty { urgent, .. } => {
                let _ = urgent.send(Bytes::from_static(&[0x03]));
            }
            Control::Pipe { signals, .. } => {
                let _ = signals.send(Signal::Interrupt);
            }
        }
    }
}

impl Drop for Process {
    /// Descendants of a command that exited normally, such as build servers, are left alone.
    fn drop(&mut self) {
        if *self.exited.borrow() {
            return;
        }
        match &self.control {
            #[cfg(unix)]
            Control::Pipe { group, .. } | Control::Pty { group, .. } => {
                if let Some(group) = group.and_then(|pid| i32::try_from(pid).ok()) {
                    let _ = nix::sys::signal::killpg(
                        nix::unistd::Pid::from_raw(group),
                        nix::sys::signal::Signal::SIGKILL,
                    );
                }
            }
            // The job also kills its processes when its last handle closes, should the waiting task go first.
            #[cfg(windows)]
            Control::Pipe { signals } => {
                let _ = signals.send(Signal::Kill);
            }
            #[cfg(windows)]
            Control::Pty { job, .. } => job.terminate(1),
        }
    }
}

/// The command leads a new process group on Unix and starts suspended inside a Job Object on Windows, so that every
/// descendant can be reached.
fn spawn_pipes(argv: &[OsString], cwd: &Path) -> io::Result<(Process, Output)> {
    let (program, rest) = argv.split_first().ok_or(io::ErrorKind::InvalidInput)?;
    let mut command = CommandWrap::with_new(program, |command| {
        command
            .args(rest)
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
    });
    #[cfg(unix)]
    command.wrap(process_wrap::tokio::ProcessGroup::leader());
    #[cfg(windows)]
    command
        .wrap(process_wrap::tokio::KillOnDrop)
        .wrap(process_wrap::tokio::JobObject);
    let mut child = command.spawn()?;
    #[cfg(unix)]
    let group = child.id();

    let (events_tx, events) = mpsc::channel(16);
    let (input, input_rx) = mpsc::channel(1);
    if let Some(stdin) = child.stdin().take() {
        tokio::spawn(feed(stdin, input_rx, mpsc::unbounded_channel().1));
    }
    let (done, exited) = watch::channel(false);
    let output = [
        child.stdout().take().map(|pipe| {
            tokio::spawn(forward(
                pipe,
                events_tx.clone(),
                Event::Stdout,
                exited.clone(),
            ))
        }),
        child.stderr().take().map(|pipe| {
            tokio::spawn(forward(
                pipe,
                events_tx.clone(),
                Event::Stderr,
                exited.clone(),
            ))
        }),
    ];

    let (signals, mut signals_rx) = mpsc::unbounded_channel::<Signal>();
    tokio::spawn(async move {
        let status = loop {
            tokio::select! {
                // The command itself, not its whole tree: descendants such as build servers may outlive it.
                status = child.inner_mut().wait() => break status,
                Some(signal) = signals_rx.recv() => signal.deliver(&mut *child),
            }
        };
        done.send_replace(true);
        // Closing the job would kill whatever the command left running, so its handle is deliberately kept.
        #[cfg(windows)]
        drop(child.into_inner());
        for task in output.into_iter().flatten() {
            let _ = task.await;
        }
        let last = match status {
            Ok(status) => Event::Exit(exit_code(status)),
            Err(err) => Event::Lost(err.to_string()),
        };
        let _ = events_tx.send(last).await;
    });

    let process = Process {
        input: Some(input),
        control: Control::Pipe {
            signals,
            #[cfg(unix)]
            group,
        },
        exited,
    };
    Ok((process, Output(events)))
}

fn open_pty(
    argv: Vec<OsString>,
    exec: &Exec,
    cwd: &Path,
) -> io::Result<(portable_pty::PtyPair, CommandBuilder)> {
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
    Ok((pair, command))
}

/// The terminal is read and written through the runtime, so nothing is left blocked on it once the run ends.
#[cfg(unix)]
fn spawn_pty(argv: Vec<OsString>, exec: &Exec, cwd: &Path) -> io::Result<(Process, Output)> {
    let (pair, command) = open_pty(argv, exec, cwd)?;
    let child = pair
        .slave
        .spawn_command(command)
        .map_err(io::Error::other)?;
    drop(pair.slave);
    // The terminal's session leader, so also its process group. Reaped below with `waitpid`, never through `child`.
    let group = child.process_id();
    drop(child);
    let (reader, writer) = tty::open(&*pair.master)?;
    let master = Arc::new(Mutex::new(Some(pair.master)));

    let (events_tx, events) = mpsc::channel(16);
    let (input, input_rx) = mpsc::channel(1);
    let (urgent, urgent_rx) = mpsc::unbounded_channel();
    tokio::spawn(feed(writer, input_rx, urgent_rx));
    let (done, exited) = watch::channel(false);
    let output = tokio::spawn(forward(
        reader,
        events_tx.clone(),
        Event::Stdout,
        exited.clone(),
    ));

    let closing = Arc::clone(&master);
    tokio::spawn(async move {
        let status = match group {
            Some(pid) => tokio::task::spawn_blocking(move || tty::wait(pid))
                .await
                .unwrap_or_else(|err| Err(io::Error::other(err))),
            None => Err(io::Error::other(
                "the terminal did not report the command's process id",
            )),
        };
        done.send_replace(true);
        let _ = output.await;
        drop(
            closing
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take(),
        );
        let last = match status {
            Ok(code) => Event::Exit(code),
            Err(err) => Event::Lost(err.to_string()),
        };
        let _ = events_tx.send(last).await;
    });

    let process = Process {
        input: Some(input),
        control: Control::Pty {
            master,
            urgent,
            group,
        },
        exited,
    };
    Ok((process, Output(events)))
}

/// The Windows pseudo-console offers only blocking handles, so threads read and write it. Closing it once the command
/// exits ends the output, which releases the reader.
#[cfg(windows)]
fn spawn_pty(argv: Vec<OsString>, exec: &Exec, cwd: &Path) -> io::Result<(Process, Output)> {
    let (pair, command) = open_pty(argv, exec, cwd)?;
    let mut child = pair
        .slave
        .spawn_command(command)
        .map_err(io::Error::other)?;
    drop(pair.slave);
    // ConPTY cannot start the command suspended, so a descendant started before this assignment escapes the job.
    let job = child
        .as_raw_handle()
        .ok_or_else(|| io::Error::other("the terminal did not report the command's handle"))
        .and_then(|handle| {
            let job = job::Job::new()?;
            job.assign(handle)?;
            Ok(job)
        });
    let job = match job {
        Ok(job) => Arc::new(job),
        Err(err) => {
            let _ = child.kill();
            return Err(err);
        }
    };
    let mut reader = pair.master.try_clone_reader().map_err(io::Error::other)?;
    let mut writer = pair.master.take_writer().map_err(io::Error::other)?;
    let master = Arc::new(Mutex::new(Some(pair.master)));

    let (events_tx, events) = mpsc::channel(16);
    let (input, input_rx) = mpsc::channel(1);
    let (urgent, urgent_rx) = mpsc::unbounded_channel();
    let (merged, mut merged_rx) = mpsc::channel::<Bytes>(1);
    tokio::spawn(prioritize(input_rx, urgent_rx, merged));
    std::thread::spawn(move || {
        while let Some(data) = merged_rx.blocking_recv() {
            if let Err(err) = writer.write_all(&data).and_then(|()| writer.flush()) {
                tracing::debug!(%err, "the terminal stopped taking input");
                return;
            }
        }
    });
    let (eof_tx, eof_rx) = oneshot::channel::<()>();
    let output = events_tx.clone();
    let activity = Arc::new(Activity::default());
    let reading = Arc::clone(&activity);
    std::thread::spawn(move || {
        let mut buf = vec![0; CHUNK];
        loop {
            reading.waiting.store(true, Ordering::SeqCst);
            let read = reader.read(&mut buf);
            reading.waiting.store(false, Ordering::SeqCst);
            reading.reads.fetch_add(1, Ordering::SeqCst);
            match read {
                Ok(0) => break,
                Err(err) => {
                    tracing::warn!(%err, "reading the terminal failed");
                    break;
                }
                Ok(n) => {
                    if output
                        .blocking_send(Event::Stdout(Bytes::copy_from_slice(&buf[..n])))
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
        drop(eof_tx);
    });

    let (done, exited) = watch::channel(false);
    let closing = Arc::clone(&master);
    let releasing = Arc::clone(&job);
    tokio::spawn(async move {
        let status = tokio::task::spawn_blocking(move || child.wait()).await;
        done.send_replace(true);
        // The command is gone; whatever it left running, such as build servers, may outlive the job.
        releasing.release();
        let last = match status {
            Ok(Ok(status)) => {
                #[allow(clippy::cast_possible_wrap)]
                // Windows exit codes are u32 NTSTATUS values, reported as i32 like cmd does.
                let code = status.exit_code() as i32;
                Event::Exit(code)
            }
            Ok(Err(err)) => Event::Lost(err.to_string()),
            Err(err) => Event::Lost(err.to_string()),
        };
        let master = closing
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let _ = tokio::task::spawn_blocking(move || drop(master)).await;
        activity.drain(eof_rx).await;
        let _ = events_tx.send(last).await;
    });

    let process = Process {
        input: Some(input),
        control: Control::Pty {
            master,
            urgent,
            job,
        },
        exited,
    };
    Ok((process, Output(events)))
}

enum Signal {
    Interrupt,
    #[cfg(windows)]
    Kill,
}

impl Signal {
    fn deliver(self, child: &mut dyn ChildWrapper) {
        let delivered = match self {
            #[cfg(unix)]
            Self::Interrupt => child.signal(libc::SIGINT),
            #[cfg(windows)]
            Self::Interrupt | Self::Kill => child.start_kill(),
        };
        if let Err(err) = delivered {
            tracing::warn!(%err, "cannot signal the command");
        }
    }
}

/// Writes input to the command, letting `urgent` bytes overtake input the command has not read yet.
async fn feed<W: AsyncWrite + Unpin>(
    mut sink: W,
    mut input: mpsc::Receiver<Bytes>,
    mut urgent: mpsc::UnboundedReceiver<Bytes>,
) {
    loop {
        let data = tokio::select! {
            biased;
            Some(data) = urgent.recv() => data,
            data = input.recv() => match data {
                Some(data) => data,
                None => return,
            },
        };
        if let Err(err) = sink.write_all(&data).await {
            // Commonly the command closing its input; anything still sent is discarded.
            tracing::debug!(%err, "the command stopped taking input");
            return;
        }
    }
}

async fn forward<R: AsyncRead + Unpin>(
    mut pipe: R,
    events: mpsc::Sender<Event>,
    wrap: fn(Bytes) -> Event,
    mut exited: watch::Receiver<bool>,
) {
    let mut buf = BytesMut::with_capacity(CHUNK);
    loop {
        buf.reserve(CHUNK);
        let read = tokio::select! {
            read = pipe.read_buf(&mut buf) => read,
            () = async {
                let _ = exited.wait_for(|exited| *exited).await;
                tokio::time::sleep(DRAIN).await;
            } => return,
        };
        match read {
            Ok(0) => return,
            // How a Unix terminal reports that every process holding it has gone.
            #[cfg(unix)]
            Err(err) if err.raw_os_error() == Some(libc::EIO) => return,
            Err(err) => {
                tracing::warn!(%err, "reading command output failed");
                return;
            }
            Ok(_) => {
                if events.send(wrap(buf.split().freeze())).await.is_err() {
                    return;
                }
            }
        }
    }
}

#[cfg(windows)]
#[cfg(windows)]
/// [`feed`] for a blocking writer on its own thread.
async fn prioritize(
    mut input: mpsc::Receiver<Bytes>,
    mut urgent: mpsc::UnboundedReceiver<Bytes>,
    merged: mpsc::Sender<Bytes>,
) {
    loop {
        let data = tokio::select! {
            biased;
            Some(data) = urgent.recv() => data,
            data = input.recv() => match data {
                Some(data) => data,
                None => return,
            },
        };
        if merged.send(data).await.is_err() {
            return;
        }
    }
}

#[cfg(windows)]
/// What the blocking terminal reader is doing, so draining can tell an idle terminal from a slow client.
#[derive(Default)]
struct Activity {
    waiting: AtomicBool,
    reads: AtomicU64,
}

#[cfg(windows)]
impl Activity {
    /// Waits for the terminal's end of output, or until one read has waited [`DRAIN`] without data.
    async fn drain(&self, mut eof: oneshot::Receiver<()>) {
        loop {
            let (waiting, reads) = (
                self.waiting.load(Ordering::SeqCst),
                self.reads.load(Ordering::SeqCst),
            );
            if tokio::time::timeout(DRAIN, &mut eof).await.is_ok() {
                return;
            }
            if waiting
                && self.waiting.load(Ordering::SeqCst)
                && self.reads.load(Ordering::SeqCst) == reads
            {
                return;
            }
        }
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

#[cfg(unix)]
#[allow(unsafe_code)]
mod tty {
    use std::io;
    use std::os::fd::BorrowedFd;

    use nix::errno::Errno;
    use nix::fcntl::{FcntlArg, OFlag, fcntl};
    use nix::sys::wait::{WaitStatus, waitpid};
    use nix::unistd::Pid;
    use portable_pty::MasterPty;
    use tokio::net::unix::pipe;

    /// Non-blocking halves of the terminal's master side, driven by the runtime.
    pub fn open(master: &dyn MasterPty) -> io::Result<(pipe::Receiver, pipe::Sender)> {
        let fd = master
            .as_raw_fd()
            .ok_or_else(|| io::Error::other("the terminal has no file descriptor"))?;
        // SAFETY: `fd` is owned by `master`, which is open for the duration of this call; it is only duplicated.
        let fd = unsafe { BorrowedFd::borrow_raw(fd) };
        let (read, write) = (fd.try_clone_to_owned()?, fd.try_clone_to_owned()?);
        // Duplicates share one open file, so this makes every handle on the terminal non-blocking. `master` itself
        // is only used for resizing from here on.
        let flags = OFlag::from_bits_retain(fcntl(&read, FcntlArg::F_GETFL)?);
        fcntl(&read, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
        Ok((
            pipe::Receiver::from_owned_fd_unchecked(read)?,
            pipe::Sender::from_owned_fd_unchecked(write)?,
        ))
    }

    /// Reaps `pid`, returning the exit code a shell would report: the status, or 128 plus the killing signal.
    pub fn wait(pid: u32) -> io::Result<i32> {
        let pid = Pid::from_raw(i32::try_from(pid).map_err(io::Error::other)?);
        loop {
            match waitpid(pid, None) {
                Ok(WaitStatus::Exited(_, code)) => return Ok(code),
                Ok(WaitStatus::Signaled(_, signal, _)) => return Ok(128 + signal as i32),
                Ok(_) | Err(Errno::EINTR) => {}
                Err(err) => return Err(err.into()),
            }
        }
    }
}

#[cfg(windows)]
#[allow(unsafe_code)]
mod job {
    use std::io;
    use std::os::windows::io::RawHandle;

    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        SetInformationJobObject, TerminateJobObject,
    };

    /// A Job Object holding a command and every process it starts. Its processes are killed when it is dropped,
    /// unless released first.
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
                return Err(io::Error::last_os_error());
            }
            let job = Self(handle);
            job.limit(JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE)?;
            Ok(job)
        }

        /// Lets the job's processes outlive it.
        pub fn release(&self) {
            if let Err(err) = self.limit(0) {
                tracing::warn!(%err, "processes left by the command will be killed with it");
            }
        }

        fn limit(&self, flags: u32) -> io::Result<()> {
            // SAFETY: an all-zero JOBOBJECT_EXTENDED_LIMIT_INFORMATION is valid and sets no limits.
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
            info.BasicLimitInformation.LimitFlags = flags;
            let size = u32::try_from(size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>())
                .map_err(io::Error::other)?;
            // SAFETY: the handle is valid until drop, and `info` is the structure this class expects, of `size`.
            let ok = unsafe {
                SetInformationJobObject(
                    self.0,
                    JobObjectExtendedLimitInformation,
                    (&raw const info).cast(),
                    size,
                )
            };
            if ok == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    async fn run_pty(script: &str, input: &[u8]) -> io::Result<(String, i32)> {
        let exec = Exec {
            argv: vec!["sh".to_owned(), "-c".to_owned(), script.to_owned()],
            cwd: String::new(),
            tty: true,
            cols: 80,
            rows: 24,
        };
        let (process, mut output) = Process::spawn(&Shell::new("/bin/sh"), &exec, Path::new("/"))?;
        if let Some(sender) = process.input() {
            let _ = sender.send(Bytes::copy_from_slice(input)).await;
        }
        let mut text = String::new();
        while let Some(event) = output.next().await {
            match event {
                Event::Stdout(data) => text.push_str(&String::from_utf8_lossy(&data)),
                Event::Stderr(_) => {}
                Event::Exit(code) => return Ok((text, code)),
                Event::Lost(err) => return Err(io::Error::other(err)),
            }
        }
        Err(io::ErrorKind::UnexpectedEof.into())
    }

    #[tokio::test]
    async fn terminal_runs_relay_io_and_report_exit_codes_like_a_shell() -> io::Result<()> {
        let (text, code) = run_pty("read line; echo got $line; exit 3", b"hi\r").await?;
        assert!(text.ends_with("got hi\r\n"), "{text:?}");
        assert_eq!(code, 3);

        let (_, code) = run_pty("kill -TERM $$", b"").await?;
        assert_eq!(code, 128 + 15);
        Ok(())
    }
}
