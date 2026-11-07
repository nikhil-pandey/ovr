//! The client half of a stream: start a run by syncing the snapshot, or attach to one, then relay terminal I/O
//! until it exits. Every offset is tracked, so a stream that breaks can be resumed on a new one.

use std::convert::Infallible;

use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use ovr_proto::frame::{self, CHUNK, INPUT_WINDOW, Reader, Writer};
use ovr_proto::msg::{
    Attach, Attached, Consumed, Exec, Exit, Failure, Resize, RunInfo, RunList, Runs, Welcome,
};
use ovr_proto::{Frame, FrameKind};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, watch};

use crate::Error;
use crate::snapshot::Delivery;

/// Exit code reported when a second interrupt stops the run, as a shell does for `SIGINT`.
pub const INTERRUPTED: i32 = 130;

/// Local terminal events forwarded to the remote command, ahead of any pending input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Resize {
        cols: u16,
        rows: u16,
    },
    Interrupt,
    /// Leave the run running: the terminal is going away.
    Detach,
}

/// Where a run's input comes from and its output goes, for as many streams as it takes.
pub struct Io<O, E> {
    /// Local stdin; it ends when the sender is dropped.
    pub stdin: mpsc::Receiver<Bytes>,
    pub signals: mpsc::Receiver<Signal>,
    pub stdout: O,
    pub stderr: E,
}

/// How far a client has got with a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    pub run: u64,
    pub tty: bool,
    /// Output written so far, per stream.
    pub out: u64,
    pub err: u64,
    /// The command was interrupted once; another interrupt stops the run.
    pub interrupted: bool,
}

impl Position {
    #[must_use]
    pub fn new(attached: Attached) -> Self {
        Self {
            run: attached.run,
            tty: attached.tty,
            out: attached.out,
            err: attached.err,
            interrupted: false,
        }
    }

    /// Where a new stream resumes.
    #[must_use]
    pub fn resume(&self) -> Attach {
        Attach {
            run: self.run,
            out: self.out,
            err: self.err,
        }
    }

    /// Moves to where the host resumed, returning how many bytes of output were dropped in between.
    pub fn resumed(&mut self, attached: Attached) -> u64 {
        let dropped = attached.out.saturating_sub(self.out) + attached.err.saturating_sub(self.err);
        self.out = attached.out;
        self.err = attached.err;
        dropped
    }
}

/// Whether a client drives the run or only watches it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Input, resizes, and interrupts reach the command; a second interrupt stops it.
    Drive,
    /// Output only; an interrupt stops watching.
    Watch,
}

/// How a relay ended, short of an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum End {
    /// The run ended, or this client stopped it or stopped watching, with this exit code.
    Exit(i32),
    /// The client is going away and leaves the run running.
    Detached,
}

/// Both halves of a stream.
pub struct Stream<S> {
    frames: Reader<S>,
    writer: Writer<S>,
}

impl<S: AsyncRead + AsyncWrite> Stream<S> {
    fn new(stream: S) -> Self {
        let (frames, writer) = frame::split(stream);
        Self { frames, writer }
    }

    /// Sends one frame, such as the end of input for a run left in the background.
    ///
    /// # Errors
    ///
    /// Returns an error when the stream is broken.
    pub async fn send(&mut self, frame: Frame) -> Result<(), Error> {
        Ok(self.writer.send(frame).await?)
    }
}

/// Syncs `delivery`, starts `exec`, and returns once the run has started, writing the host's warnings to `stderr`.
///
/// # Errors
///
/// Returns an error when syncing fails, the command cannot be started, or the connection drops.
pub async fn start<S, E>(
    stream: S,
    delivery: &Delivery<'_>,
    exec: &Exec,
    stderr: &mut E,
) -> Result<(Stream<S>, Position), Error>
where
    S: AsyncRead + AsyncWrite,
    E: AsyncWrite + Unpin,
{
    let mut stream = Stream::new(stream);
    let mut synced = false;
    let started = async {
        sync(&mut stream, delivery, exec).await?;
        loop {
            let frame = next(&mut stream.frames).await?;
            match frame.kind {
                FrameKind::Synced => {
                    synced = true;
                    delivery.synced().await?;
                }
                FrameKind::Err => {
                    stderr.write_all(&frame.payload).await?;
                    stderr.flush().await?;
                }
                FrameKind::Attached => return Ok(Position::new(frame.parse()?)),
                kind => return Err(ovr_proto::Error::Unexpected(kind).into()),
            }
        }
    }
    .await;
    match started {
        Ok(position) => Ok((stream, position)),
        Err(err) => {
            if !synced {
                delivery.forget().await;
            }
            Err(err)
        }
    }
}

/// Resumes run `attach.run` from the given offsets.
///
/// # Errors
///
/// Returns an error when the host has no such run or the connection drops.
pub async fn attach<S: AsyncRead + AsyncWrite>(
    stream: S,
    attach: Attach,
) -> Result<(Stream<S>, Attached), Error> {
    let mut stream = Stream::new(stream);
    stream
        .send(Frame::json(FrameKind::Attach, &attach)?)
        .await?;
    let attached = next(&mut stream.frames)
        .await?
        .expect(FrameKind::Attached)?;
    Ok((stream, attached))
}

/// `initiator`'s runs on the host.
///
/// # Errors
///
/// Returns an error when the host cannot list them.
pub async fn list<S: AsyncRead + AsyncWrite>(
    stream: S,
    initiator: &str,
) -> Result<Vec<RunInfo>, Error> {
    let mut stream = Stream::new(stream);
    let runs = Runs {
        initiator: initiator.to_owned(),
    };
    stream.send(Frame::json(FrameKind::Runs, &runs)?).await?;
    let list: RunList = next(&mut stream.frames).await?.expect(FrameKind::RunList)?;
    Ok(list.runs)
}

/// Kills `run`'s whole process tree and waits for it to end.
///
/// # Errors
///
/// Returns an error when there is no such run or the connection drops.
pub async fn kill<S: AsyncRead + AsyncWrite>(stream: S, run: u64) -> Result<(), Error> {
    let from_end = Attach {
        run,
        out: u64::MAX,
        err: u64::MAX,
    };
    let (mut stream, _) = attach(stream, from_end).await?;
    stream.send(Frame::empty(FrameKind::Kill)).await?;
    loop {
        match next(&mut stream.frames).await?.kind {
            FrameKind::Exit => return Ok(()),
            FrameKind::Out | FrameKind::Err | FrameKind::Consumed => {}
            kind => return Err(ovr_proto::Error::Unexpected(kind).into()),
        }
    }
}

/// Sends `HELLO`, a speculative thin pack, and `EXEC` without waiting, then repeats the pack in full if the host
/// lacks the base.
async fn sync<S: AsyncRead + AsyncWrite>(
    stream: &mut Stream<S>,
    delivery: &Delivery<'_>,
    exec: &Exec,
) -> Result<(), Error> {
    stream
        .send(Frame::json(FrameKind::Hello, &delivery.hello())?)
        .await?;
    let mut welcome = send_pack(stream, delivery, delivery.base.as_deref(), true).await?;
    stream.send(Frame::json(FrameKind::Exec, exec)?).await?;
    let welcome = match welcome.take() {
        Some(welcome) => welcome,
        None => next(&mut stream.frames).await?.expect(FrameKind::Welcome)?,
    };
    if welcome == Welcome::NeedFull {
        send_pack(stream, delivery, None, false).await?;
        stream.send(Frame::json(FrameKind::Exec, exec)?).await?;
    }
    Ok(())
}

/// Streams a pack followed by the empty end-of-pack frame. While `watch` is set, also listens for `WELCOME` and
/// cuts the pack short when the host needs a full one.
async fn send_pack<S: AsyncRead + AsyncWrite>(
    stream: &mut Stream<S>,
    delivery: &Delivery<'_>,
    base: Option<&str>,
    mut watch: bool,
) -> Result<Option<Welcome>, Error> {
    let mut welcome = None;
    if let Some(mut pack) = delivery.pack(base).await? {
        let mut buf = BytesMut::with_capacity(CHUNK);
        let complete = loop {
            buf.reserve(CHUNK);
            tokio::select! {
                read = pack.stdout.read_buf(&mut buf) => match read? {
                    0 => break true,
                    _ => stream.writer.send(Frame::new(FrameKind::Pack, buf.split().freeze())).await?,
                },
                reply = next(&mut stream.frames), if watch => {
                    let reply = reply?.expect(FrameKind::Welcome)?;
                    welcome = Some(reply);
                    watch = false;
                    if reply == Welcome::NeedFull {
                        break false;
                    }
                }
            }
        };
        if complete {
            pack.finish().await?;
        }
    }
    stream.send(Frame::empty(FrameKind::Pack)).await?;
    Ok(welcome)
}

/// Receives output, forwards signals, and sends input concurrently, so neither a slow local terminal nor a command
/// that ignores its input keeps a second Ctrl-C from stopping the run. Returns [`Error::Disconnected`] when the
/// stream breaks; `position` then says where to resume.
///
/// # Errors
///
/// Returns an error when the host reports a failure, the stream breaks, or the output cannot be written; the run is
/// killed in the last case, as `SIGPIPE` would kill a local command.
pub async fn relay<S, O, E>(
    stream: &mut Stream<S>,
    io: &mut Io<O, E>,
    position: &mut Position,
    mode: Mode,
) -> Result<End, Error>
where
    S: AsyncRead + AsyncWrite,
    O: AsyncWrite + Unpin,
    E: AsyncWrite + Unpin,
{
    let Stream { frames, writer } = stream;
    let Io {
        stdin,
        signals,
        stdout,
        stderr,
    } = io;
    let Position {
        out,
        err,
        interrupted,
        ..
    } = position;
    let (consumed_tx, mut consumed) = watch::channel(0);
    let (control_tx, mut control) = mpsc::unbounded_channel();

    let receive = async {
        loop {
            let frame = next(frames).await?;
            match frame.kind {
                FrameKind::Out => {
                    write(stdout, &frame.payload).await?;
                    *out += frame.payload.len() as u64;
                }
                FrameKind::Err => {
                    write(stderr, &frame.payload).await?;
                    *err += frame.payload.len() as u64;
                }
                FrameKind::Consumed => {
                    consumed_tx.send_replace(frame.parse::<Consumed>()?.bytes);
                }
                FrameKind::Exit => return Ok(Stop::Exit(frame.parse::<Exit>()?.code)),
                kind => return Err(ovr_proto::Error::Unexpected(kind).into()),
            }
        }
    };
    let forward_signals = async {
        while let Some(signal) = signals.recv().await {
            let frame = match (signal, mode) {
                (Signal::Detach, _) => return Ok(Stop::Detach),
                (Signal::Interrupt, Mode::Watch) => return Ok(Stop::Exit(INTERRUPTED)),
                (Signal::Interrupt, Mode::Drive) if *interrupted => return Ok(Stop::Kill),
                (Signal::Interrupt, Mode::Drive) => {
                    *interrupted = true;
                    Frame::empty(FrameKind::Interrupt)
                }
                (Signal::Resize { cols, rows }, Mode::Drive) => {
                    Frame::json(FrameKind::Resize, &Resize { cols, rows })?
                }
                (Signal::Resize { .. }, Mode::Watch) => continue,
            };
            let _ = control_tx.send(frame);
        }
        std::future::pending().await
    };
    let send = async {
        let mut sent = 0;
        let mut stdin_open = mode == Mode::Drive;
        loop {
            // The keeper holds what the command has not consumed yet; this bounds how much.
            let room = sent < *consumed.borrow_and_update() + INPUT_WINDOW;
            let frame = tokio::select! {
                biased;
                Some(frame) = control.recv() => frame,
                data = stdin.recv(), if stdin_open && room => {
                    if let Some(data) = data {
                        sent += data.len() as u64;
                        Frame::new(FrameKind::In, data)
                    } else {
                        stdin_open = false;
                        Frame::empty(FrameKind::In)
                    }
                }
                Ok(()) = consumed.changed(), if stdin_open && !room => continue,
                else => break,
            };
            if writer.send(frame).await.is_err() {
                break;
            }
        }
        // Nothing more can be sent; the host's last frames, or the broken stream, end the relay.
        std::future::pending::<Infallible>().await
    };
    let stop = tokio::select! {
        stop = receive => stop,
        stop = forward_signals => stop,
        never = send => match never {},
    };
    match stop {
        Ok(Stop::Exit(code)) => Ok(End::Exit(code)),
        Ok(Stop::Detach) => Ok(End::Detached),
        Ok(Stop::Kill) => {
            let _ = writer.send(Frame::empty(FrameKind::Kill)).await;
            Ok(End::Exit(INTERRUPTED))
        }
        Err(Error::Output(err)) => {
            let _ = writer.send(Frame::empty(FrameKind::Kill)).await;
            Err(Error::Output(err))
        }
        Err(err) => Err(err),
    }
}

enum Stop {
    Exit(i32),
    Detach,
    Kill,
}

async fn write<W: AsyncWrite + Unpin>(out: &mut W, data: &[u8]) -> Result<(), Error> {
    async {
        out.write_all(data).await?;
        out.flush().await
    }
    .await
    .map_err(Error::Output)
}

/// The next frame from the host, turning `ERROR` frames into errors and a broken stream into
/// [`Error::Disconnected`].
async fn next<S: AsyncRead>(frames: &mut Reader<S>) -> Result<Frame, Error> {
    let frame = match frames.next().await {
        Some(Ok(frame)) => frame,
        None | Some(Err(ovr_proto::Error::Io(_))) => return Err(Error::Disconnected),
        Some(Err(err)) => return Err(err.into()),
    };
    if frame.kind == FrameKind::Error {
        return Err(Error::Remote(frame.parse::<Failure>()?.message));
    }
    Ok(frame)
}

#[cfg(all(test, unix))]
mod tests {
    use std::path::Path;

    use ovrd::agent::{Config, Shell};

    use super::*;
    use crate::snapshot::Snapshot;

    struct Run {
        code: i32,
        stdout: String,
        stderr: String,
    }

    async fn ovr(
        repo: &Path,
        cwd: &str,
        host_root: &Path,
        script: &str,
        stdin: &[u8],
    ) -> Result<Run, Error> {
        ovr_with(repo, cwd, host_root, script, stdin, mpsc::channel(1).1).await
    }

    async fn ovr_with(
        repo: &Path,
        cwd: &str,
        host_root: &Path,
        script: &str,
        stdin: &[u8],
        signals: mpsc::Receiver<Signal>,
    ) -> Result<Run, Error> {
        let snapshot = Snapshot::take(&repo.join(cwd)).await?;
        let delivery = snapshot.deliver_to("box", "me").await?;
        let exec = Exec {
            argv: vec!["sh".to_owned(), "-c".to_owned(), script.to_owned()],
            cwd: snapshot.prefix.clone(),
            tty: false,
            cols: 80,
            rows: 24,
        };
        let config = config(host_root);
        let (client, agent) = tokio::io::duplex(1 << 16);
        let agent = tokio::spawn(async move { ovrd::agent::session::serve(agent, &config).await });

        let (stdin_tx, stdin_rx) = mpsc::channel(4);
        let stdin = Bytes::copy_from_slice(stdin);
        tokio::spawn(async move {
            for at in (0..stdin.len()).step_by(CHUNK) {
                let chunk = stdin.slice(at..stdin.len().min(at + CHUNK));
                if stdin_tx.send(chunk).await.is_err() {
                    return;
                }
            }
        });
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let (mut stream, mut position) = start(client, &delivery, &exec, &mut stderr).await?;
        let mut io = Io {
            stdin: stdin_rx,
            signals,
            stdout: &mut stdout,
            stderr: &mut stderr,
        };
        let end = relay(&mut stream, &mut io, &mut position, Mode::Drive).await?;
        drop(stream);
        agent.await.map_err(|_| Error::Disconnected)?;
        let End::Exit(code) = end else {
            return Err(Error::Disconnected);
        };
        Ok(Run {
            code,
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        })
    }

    fn config(host_root: &Path) -> Config {
        Config {
            root: host_root.to_owned(),
            runs: host_root.join(".runs"),
            shell: Shell::new("/bin/sh"),
            keeper: None,
        }
    }

    fn repo() -> Result<tempfile::TempDir, Error> {
        let dir = tempfile::tempdir()?;
        let status = std::process::Command::new("git")
            .args(["init", "-q"])
            .current_dir(dir.path())
            .status()?;
        assert!(status.success());
        std::fs::write(dir.path().join(".gitignore"), "target/\n")?;
        std::fs::create_dir_all(dir.path().join("src"))?;
        std::fs::create_dir_all(dir.path().join("target"))?;
        std::fs::write(dir.path().join("src/main.rs"), "v1\n")?;
        std::fs::write(dir.path().join("src/gone.rs"), "bye\n")?;
        std::fs::write(dir.path().join("target/big"), "artifact\n")?;
        Ok(dir)
    }

    #[tokio::test]
    async fn syncs_incrementally_and_relays_the_command() -> Result<(), Error> {
        let repo = repo()?;
        let host = tempfile::tempdir()?;
        let name = repo
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_owned();

        let first = ovr(
            repo.path(),
            "src",
            host.path(),
            "pwd; cat main.rs; ls ../target; exit 3",
            b"",
        )
        .await?;
        assert_eq!(first.code, 3);
        assert!(
            first.stdout.contains(&format!("/{name}-")) && first.stdout.ends_with("/src\nv1\n"),
            "{}",
            first.stdout
        );
        assert!(
            first.stderr.contains("No such file"),
            "ignored files must not be sent: {}",
            first.stderr
        );

        std::fs::write(repo.path().join("src/main.rs"), "v2\n")?;
        std::fs::remove_file(repo.path().join("src/gone.rs"))?;
        let second = ovr(
            repo.path(),
            "",
            host.path(),
            "cat src/main.rs; ls src; cat",
            b"typed",
        )
        .await?;
        assert_eq!(
            (second.code, second.stdout.as_str()),
            (0, "v2\nmain.rs\ntyped")
        );
        Ok(())
    }

    #[tokio::test]
    async fn restores_tracked_files_the_host_changed() -> Result<(), Error> {
        let repo = repo()?;
        let host = tempfile::tempdir()?;
        ovr(
            repo.path(),
            "",
            host.path(),
            "echo edited > src/main.rs; rm src/gone.rs",
            b"",
        )
        .await?;

        let run = ovr(
            repo.path(),
            "",
            host.path(),
            "cat src/main.rs src/gone.rs; git cat-file -t HEAD",
            b"",
        )
        .await?;
        assert_eq!((run.code, run.stdout.as_str()), (0, "v1\nbye\ncommit\n"));
        Ok(())
    }

    #[tokio::test]
    async fn resends_everything_when_the_host_lost_its_checkout() -> Result<(), Error> {
        let repo = repo()?;
        let host = tempfile::tempdir()?;
        ovr(repo.path(), "", host.path(), "true", b"").await?;
        std::fs::remove_dir_all(host.path())?;

        std::fs::write(repo.path().join("src/main.rs"), "v2\n")?;
        let run = ovr(
            repo.path(),
            "",
            host.path(),
            "cat src/main.rs src/gone.rs",
            b"",
        )
        .await?;
        assert_eq!((run.code, run.stdout.as_str()), (0, "v2\nbye\n"));
        Ok(())
    }

    #[tokio::test]
    async fn reports_remote_failures_without_recording_the_sync() -> Result<(), Error> {
        let repo = repo()?;
        let host = tempfile::tempdir()?;
        // A file where the checkout root should be.
        let blocked = host.path().join("blocker");
        std::fs::write(&blocked, "")?;

        let err = ovr(repo.path(), "", &blocked, "true", b"").await.err();
        assert!(matches!(err, Some(Error::Remote(_))), "{err:?}");
        let snapshot = Snapshot::take(repo.path()).await?;
        assert_eq!(snapshot.deliver_to("box", "me").await?.base, None);
        Ok(())
    }

    #[tokio::test]
    async fn input_beyond_the_window_flows_as_the_command_reads_it() -> Result<(), Error> {
        let repo = repo()?;
        let host = tempfile::tempdir()?;
        let input = vec![b'x'; 3 * usize::try_from(INPUT_WINDOW).unwrap_or_default()];

        let slow = ovr(repo.path(), "", host.path(), "sleep 1; wc -c", &input).await?;
        assert_eq!(slow.stdout.trim(), input.len().to_string());
        let ignored = ovr(repo.path(), "", host.path(), "echo done", &input).await?;
        assert_eq!((ignored.code, ignored.stdout.as_str()), (0, "done\n"));
        Ok(())
    }

    #[tokio::test]
    async fn interrupts_reach_the_command_and_a_second_stops_the_run() -> Result<(), Error> {
        let repo = repo()?;
        let host = tempfile::tempdir()?;
        let interrupt = |count| {
            let (tx, rx) = mpsc::channel(2);
            tokio::spawn(async move {
                for _ in 0..count {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    let _ = tx.send(Signal::Interrupt).await;
                }
                tx.closed().await;
            });
            rx
        };

        let run = ovr_with(repo.path(), "", host.path(), "sleep 30", b"", interrupt(1)).await?;
        assert_eq!(run.code, 128 + 2, "SIGINT");

        let marker = host.path().join("survived");
        let script = format!("trap '' INT; sleep 2; touch {}", marker.display());
        let run = ovr_with(repo.path(), "", host.path(), &script, b"", interrupt(2)).await?;
        assert_eq!(run.code, INTERRUPTED);
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert!(!marker.exists(), "a stopped run kills its command");
        Ok(())
    }
}
