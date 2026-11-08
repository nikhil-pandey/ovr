//! The client half of a stream: start a run by syncing the snapshot or without syncing, or attach to one, then relay
//! terminal I/O until it exits. Every offset is tracked, so a stream that breaks can be resumed on a new one.

use std::convert::Infallible;
use std::future::poll_fn;
use std::pin::Pin;
use std::task::ready;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use futures::{Sink, SinkExt, StreamExt};
use ovr_proto::frame::{self, CHUNK, INPUT_WINDOW, Reader, Writer};
use ovr_proto::msg::{
    Attach, Attached, Clean, Cleaned, Consumed, Enter, Exec, Exit, Failure, Resize, RunInfo,
    RunList, Runs, Welcome,
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
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(checkout = %delivery.checkout, seq = delivery.snapshot.seq)
)]
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
    let started = async {
        let mut phase = Instant::now();
        let recovered = sync(&mut stream, delivery, exec).await?;
        loop {
            let frame = next(&mut stream.frames).await?;
            match frame.kind {
                FrameKind::Synced => {
                    tracing::debug!(elapsed = ?phase.elapsed(), "received SYNCED");
                    phase = Instant::now();
                    delivery.synced(recovered.as_deref()).await?;
                    tracing::debug!(elapsed = ?phase.elapsed(), "recorded the acknowledged bases");
                    phase = Instant::now();
                }
                FrameKind::Err => warn(stderr, &frame.payload).await?,
                FrameKind::Attached => {
                    let attached: Attached = frame.parse()?;
                    tracing::debug!(elapsed = ?phase.elapsed(), run = attached.run, "received ATTACHED");
                    return Ok(Position::new(attached));
                }
                kind => return Err(ovr_proto::Error::Unexpected(kind).into()),
            }
        }
    }
    .await;
    match started {
        Ok(position) => Ok((stream, position)),
        // The bases stay: the host may have applied the snapshot anyway, and the next run's HELLO finds out.
        Err(err) => Err(reason(&mut stream.frames, err).await),
    }
}

/// Starts `exec` without syncing, in the checkout `enter` names as the host has it, or in the host's base directory,
/// and returns once the run has started, writing the host's warnings to `stderr`. One round trip: `EXEC` follows
/// `ENTER` at once.
///
/// # Errors
///
/// Returns an error when the command cannot be started or the connection drops.
pub async fn enter<S, E>(
    stream: S,
    enter: &Enter,
    exec: &Exec,
    stderr: &mut E,
) -> Result<(Stream<S>, Position), Error>
where
    S: AsyncRead + AsyncWrite,
    E: AsyncWrite + Unpin,
{
    let mut stream = Stream::new(stream);
    let started = async {
        stream
            .writer
            .feed(Frame::json(FrameKind::Enter, enter)?)
            .await?;
        stream.send(Frame::json(FrameKind::Exec, exec)?).await?;
        loop {
            let frame = next(&mut stream.frames).await?;
            match frame.kind {
                FrameKind::Err => warn(stderr, &frame.payload).await?,
                FrameKind::Attached => return Ok(Position::new(frame.parse()?)),
                kind => return Err(ovr_proto::Error::Unexpected(kind).into()),
            }
        }
    }
    .await;
    match started {
        Ok(position) => Ok((stream, position)),
        Err(err) => Err(reason(&mut stream.frames, err).await),
    }
}

/// Writes a warning from the host before the command's output.
async fn warn<E: AsyncWrite + Unpin>(stderr: &mut E, warning: &[u8]) -> Result<(), Error> {
    stderr.write_all(warning).await?;
    stderr.flush().await?;
    Ok(())
}

/// A host that fails stops reading and closes the stream right after saying why, so a write that failed meanwhile
/// is reported as the host's reason when it gave one.
async fn reason<S: AsyncRead>(frames: &mut Reader<S>, err: Error) -> Error {
    if !matches!(err, Error::Protocol(ovr_proto::Error::Io(_))) {
        return err;
    }
    loop {
        match next(frames).await {
            Ok(_) => {}
            Err(remote @ Error::Remote(_)) => return remote,
            Err(_) => return err,
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

/// Removes `initiator`'s checkouts and finished runs from the host, and agents of other versions.
///
/// # Errors
///
/// Returns an error when the host cannot clean up or the connection drops.
pub async fn clean<S: AsyncRead + AsyncWrite>(
    stream: S,
    initiator: &str,
) -> Result<Cleaned, Error> {
    let mut stream = Stream::new(stream);
    let clean = Clean {
        initiator: initiator.to_owned(),
    };
    stream.send(Frame::json(FrameKind::Clean, &clean)?).await?;
    Ok(next(&mut stream.frames).await?.expect(FrameKind::Cleaned)?)
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

/// Sends `HELLO`, a speculative thin pack, and `EXEC` without waiting, then, if the host lacks a base, a pack
/// against the snapshot it has instead, or a full one when this repository no longer has that. Everything after
/// `HELLO` is sent while listening to the host, since a host that fails mid-sync says why and stops reading:
/// waiting for it to read would hang.
///
/// Returns what the replacement pack was built against, if the host lacked a base.
async fn sync<S: AsyncRead + AsyncWrite>(
    stream: &mut Stream<S>,
    delivery: &Delivery<'_>,
    exec: &Exec,
) -> Result<Option<Vec<String>>, Error> {
    stream
        .send(Frame::json(FrameKind::Hello, &delivery.hello())?)
        .await?;
    let mut welcome = None;
    send_pack(stream, delivery, &delivery.bases, &mut welcome).await?;
    send_heeding(stream, Frame::json(FrameKind::Exec, exec)?, &mut welcome).await?;
    let welcome = match welcome {
        Some(welcome) => welcome,
        None => next(&mut stream.frames).await?.expect(FrameKind::Welcome)?,
    };
    let Welcome::Rebase { have } = &welcome else {
        return Ok(None);
    };
    let bases = match have {
        Some(have) if delivery.has(have).await => vec![have.clone()],
        _ => Vec::new(),
    };
    tracing::debug!(?have, ?bases, "the host lacks a base");
    let mut welcome = Some(welcome);
    send_pack(stream, delivery, &bases, &mut welcome).await?;
    send_heeding(stream, Frame::json(FrameKind::Exec, exec)?, &mut welcome).await?;
    Ok(Some(bases))
}

/// Streams a pack followed by the empty end-of-pack frame. A pack sent before the host's `WELCOME` is cut short
/// once that says the host lacks its base.
async fn send_pack<S: AsyncRead + AsyncWrite>(
    stream: &mut Stream<S>,
    delivery: &Delivery<'_>,
    bases: &[String],
    welcome: &mut Option<Welcome>,
) -> Result<(), Error> {
    let speculative = welcome.is_none();
    let sending = Instant::now();
    let mut bytes = 0;
    let mut cut = false;
    if let Some(mut pack) = delivery.pack(bases).await? {
        let mut buf = BytesMut::with_capacity(CHUNK);
        let complete = loop {
            if speculative && matches!(welcome, Some(Welcome::Rebase { .. })) {
                break false;
            }
            buf.reserve(CHUNK);
            tokio::select! {
                read = pack.stdout.read_buf(&mut buf) => {
                    let read = read?;
                    if read == 0 {
                        break true;
                    }
                    bytes += read;
                    let frame = Frame::new(FrameKind::Pack, buf.split().freeze());
                    send_heeding(stream, frame, welcome).await?;
                }
                heard = heed(&mut stream.frames, welcome) => heard?,
            }
        };
        if complete {
            pack.finish().await?;
        }
        cut = !complete;
    }
    send_heeding(stream, Frame::empty(FrameKind::Pack), welcome).await?;
    tracing::debug!(elapsed = ?sending.elapsed(), bytes, full = bases.is_empty(), cut, "sent the pack");
    Ok(())
}

/// Sends `frame` while heeding the host, without ever dropping a frame half sent.
async fn send_heeding<S: AsyncRead + AsyncWrite>(
    stream: &mut Stream<S>,
    frame: Frame,
    welcome: &mut Option<Welcome>,
) -> Result<(), Error> {
    let Stream { frames, writer } = stream;
    let mut frame = Some(frame);
    let mut writer = Pin::new(writer);
    loop {
        let sent = poll_fn(|cx| {
            if frame.is_some() {
                ready!(writer.as_mut().poll_ready(cx))?;
                if let Some(frame) = frame.take() {
                    writer.as_mut().start_send(frame)?;
                }
            }
            writer.as_mut().poll_flush(cx)
        });
        tokio::select! {
            biased;
            sent = sent => return Ok(sent?),
            heard = heed(frames, welcome) => heard?,
        }
    }
}

/// Takes the host's one reply while sending, its `WELCOME`; anything else, such as `ERROR`, fails the sync.
async fn heed<S: AsyncRead>(
    frames: &mut Reader<S>,
    welcome: &mut Option<Welcome>,
) -> Result<(), Error> {
    let frame = next(frames).await?;
    if welcome.is_some() {
        return Err(ovr_proto::Error::Unexpected(frame.kind).into());
    }
    *welcome = Some(frame.expect(FrameKind::Welcome)?);
    Ok(())
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use ovr_proto::msg::RunState;
    use ovrd::agent::{Config, Shell};
    use tokio::io::DuplexStream;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::snapshot::{Repository, Snapshot};

    struct Run {
        /// The host's number for the run.
        number: u64,
        code: i32,
        stdout: String,
        stderr: String,
        /// Bytes of the pack the host indexed.
        sent: u64,
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
        let snapshot = Snapshot::take_at(&repo.join(cwd)).await?;
        run_snapshot(&snapshot, host_root, script, stdin, signals).await
    }

    async fn run_snapshot(
        snapshot: &Snapshot,
        host_root: &Path,
        script: &str,
        stdin: &[u8],
        signals: mpsc::Receiver<Signal>,
    ) -> Result<Run, Error> {
        let delivery = snapshot.deliver_to("box", "me").await?;
        let (client, agent, sent) = counted(host_root);

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
        let mut stderr = Vec::new();
        let exec = exec(snapshot, script);
        let (stream, position) = start(client, &delivery, &exec, &mut stderr).await?;
        let mut run = to_end(stream, position, stdin_rx, signals, stderr, agent).await?;
        run.sent = sent.load(Ordering::Relaxed);
        Ok(run)
    }

    /// Relays a started run until it exits, after `stderr` holds what the host said before the command's output.
    async fn to_end(
        mut stream: Stream<DuplexStream>,
        mut position: Position,
        stdin: mpsc::Receiver<Bytes>,
        signals: mpsc::Receiver<Signal>,
        mut stderr: Vec<u8>,
        agent: JoinHandle<()>,
    ) -> Result<Run, Error> {
        let mut stdout = Vec::new();
        let mut io = Io {
            stdin,
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
            number: position.run,
            code,
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
            sent: 0,
        })
    }

    /// A stream to an agent serving `host_root` in this process, keepers included.
    fn agent(host_root: &Path) -> (DuplexStream, JoinHandle<()>) {
        let config = config(host_root);
        let (client, agent) = tokio::io::duplex(1 << 16);
        let agent = tokio::spawn(async move { ovrd::agent::session::serve(agent, &config).await });
        (client, agent)
    }

    /// Like [`agent`], also counting the bytes of the pack the agent indexes: the last one before an `EXEC`.
    fn counted(host_root: &Path) -> (DuplexStream, JoinHandle<()>, Arc<AtomicU64>) {
        let (agent_end, agent) = agent(host_root);
        let (client, proxy) = tokio::io::duplex(1 << 16);
        let (mut from_client, mut to_client) = frame::split(proxy);
        let (mut from_agent, mut to_agent) = frame::split(agent_end);
        let sent = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&sent);
        tokio::spawn(async move {
            let mut pack = 0;
            while let Some(Ok(frame)) = from_client.next().await {
                match frame.kind {
                    FrameKind::Pack => pack += frame.payload.len() as u64,
                    FrameKind::Exec => counter.store(std::mem::take(&mut pack), Ordering::Relaxed),
                    _ => {}
                }
                if to_agent.send(frame).await.is_err() {
                    break;
                }
            }
            let _ = to_agent.close().await;
        });
        tokio::spawn(async move {
            while let Some(Ok(frame)) = from_agent.next().await {
                if to_client.send(frame).await.is_err() {
                    break;
                }
            }
            let _ = to_client.close().await;
        });
        (client, agent, sent)
    }

    fn exec(snapshot: &Snapshot, script: &str) -> Exec {
        Exec {
            argv: vec!["sh".to_owned(), "-c".to_owned(), script.to_owned()],
            cwd: snapshot.repository.prefix.clone(),
            tty: false,
            cols: 80,
            rows: 24,
        }
    }

    /// Starts `script` without relaying it.
    async fn begin(
        repo: &Path,
        host_root: &Path,
        script: &str,
    ) -> Result<(Stream<DuplexStream>, Position, JoinHandle<()>), Error> {
        let snapshot = Snapshot::take_at(repo).await?;
        let delivery = snapshot.deliver_to("box", "me").await?;
        let (client, agent) = agent(host_root);
        let exec = exec(&snapshot, script);
        let (stream, position) = start(client, &delivery, &exec, &mut tokio::io::sink()).await?;
        Ok((stream, position, agent))
    }

    /// Relays a run without input until it ends or the stream breaks, returning that and the output.
    async fn follow(
        stream: &mut Stream<DuplexStream>,
        position: &mut Position,
        mode: Mode,
    ) -> (Result<End, Error>, String) {
        let mut stdout = Vec::new();
        let mut io = Io {
            stdin: mpsc::channel(1).1,
            signals: mpsc::channel(1).1,
            stdout: &mut stdout,
            stderr: tokio::io::sink(),
        };
        let end = relay(stream, &mut io, position, mode).await;
        (end, String::from_utf8_lossy(&stdout).into_owned())
    }

    async fn wait_for(path: &Path) -> Result<(), Error> {
        for _ in 0..400 {
            if path.exists() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(Error::Host(format!("{} never appeared", path.display())))
    }

    fn config(host_root: &Path) -> Config {
        Config {
            root: host_root.to_owned(),
            runs: host_root.join(".runs"),
            agents: host_root.join(".agents"),
            base: host_root.join(".base"),
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
    async fn a_failed_sync_keeps_the_confirmed_base() -> Result<(), Error> {
        let repo = repo()?;
        let host = tempfile::tempdir()?;
        ovr(repo.path(), "", host.path(), "true", b"").await?;
        let confirmed = Snapshot::take_at(repo.path()).await?.commit;
        // A file where the checkout root should be.
        let blocked = host.path().join("blocker");
        std::fs::write(&blocked, "")?;

        std::fs::write(repo.path().join("src/main.rs"), "v2\n")?;
        let err = ovr(repo.path(), "", &blocked, "true", b"").await.err();
        assert!(matches!(err, Some(Error::Remote(_))), "{err:?}");
        let snapshot = Snapshot::take_at(repo.path()).await?;
        let bases = snapshot.deliver_to("box", "me").await?.bases;
        assert_eq!(bases, [confirmed]);

        // A delivery already in flight must not erase a later delivery's acknowledgement when it fails.
        let failed = snapshot.deliver_to("box", "me").await?;
        std::fs::write(repo.path().join("src/main.rs"), "v3\n")?;
        ovr(repo.path(), "", host.path(), "true", b"").await?;
        let newer = Snapshot::take_at(repo.path()).await?;
        let (client, agent) = agent(&blocked);
        assert!(
            start(
                client,
                &failed,
                &exec(&snapshot, "true"),
                &mut tokio::io::sink()
            )
            .await
            .is_err()
        );
        agent.await.map_err(|_| Error::Disconnected)?;
        assert_eq!(
            newer.deliver_to("box", "me").await?.bases.first(),
            Some(&newer.commit)
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_host_missing_the_base_gets_a_pack_against_what_it_has() -> Result<(), Error> {
        let repo = repo()?;
        std::fs::write(repo.path().join("noise"), noise(1 << 20))?;
        let host = tempfile::tempdir()?;
        let full = ovr(repo.path(), "", host.path(), "true", b"").await?.sent;
        let checkout = std::fs::read_dir(host.path().join("me"))?
            .next()
            .ok_or_else(|| Error::Host("no checkout".to_owned()))??
            .path();

        // A base the host never received, as when it collected the base after a lost SYNCED.
        let main = repo.path().join("src/main.rs");
        std::fs::write(&main, "elsewhere\n")?;
        let unknown = Snapshot::take_at(repo.path()).await?.commit;
        let host_ref = git(repo.path(), &["for-each-ref", "--format=%(refname)"]).await?;
        let lose_base = ["update-ref", host_ref.as_str(), unknown.as_str()];

        // The host already has this very snapshot: nothing is sent.
        std::fs::write(&main, "v1\n")?;
        git(repo.path(), &lose_base).await?;
        let run = ovr(repo.path(), "", host.path(), "cat src/main.rs", b"").await?;
        assert_eq!((run.stdout.as_str(), run.sent), ("v1\n", 0));

        std::fs::write(&main, "v2\n")?;
        git(repo.path(), &lose_base).await?;
        let run = ovr(repo.path(), "", host.path(), "cat src/main.rs", b"").await?;
        assert_eq!(run.stdout, "v2\n");
        let thin = run.sent;
        assert!(
            thin * 10 < full,
            "{thin} bytes against what the host has, {full} in full"
        );

        // What the host has is gone from this repository: everything is sent again.
        let tree = git(&checkout, &["rev-parse", "HEAD^{tree}"]).await?;
        let pruned = git(&checkout, &["commit-tree", "-m", "pruned", &tree]).await?;
        std::fs::write(checkout.join(".git/ovr-applied"), format!("0 {pruned}"))?;
        std::fs::write(&main, "v3\n")?;
        git(repo.path(), &lose_base).await?;
        let run = ovr(repo.path(), "", host.path(), "cat src/main.rs", b"").await?;
        assert_eq!(run.stdout, "v3\n");
        assert!(run.sent >= full, "a full pack is sent");

        // Missing, malformed, and collected applied state all require a full replacement pack.
        for (index, state) in [
            None,
            Some("not a record".to_owned()),
            Some("0 HEAD".to_owned()),
            Some(format!("0 {}", "0".repeat(40))),
        ]
        .into_iter()
        .enumerate()
        {
            let applied = checkout.join(".git/ovr-applied");
            if let Some(state) = state {
                std::fs::write(applied, state)?;
            } else {
                std::fs::remove_file(applied)?;
            }
            let content = format!("fallback {index}\n");
            std::fs::write(&main, &content)?;
            git(repo.path(), &lose_base).await?;
            let run = ovr(repo.path(), "", host.path(), "cat src/main.rs", b"").await?;
            assert_eq!(run.stdout, content);
            assert!(run.sent >= full, "a full pack is sent");
        }
        Ok(())
    }

    #[tokio::test]
    async fn returning_to_a_retained_state_sends_nothing() -> Result<(), Error> {
        let repo = repo()?;
        let host = tempfile::tempdir()?;
        let big = repo.path().join("big");
        let streams = noise(2 << 20);
        let (a, b) = streams.split_at(1 << 20);
        std::fs::write(&big, a)?;
        let first = ovr(repo.path(), "", host.path(), "true", b"").await?.sent;
        let checkout = std::fs::read_dir(host.path().join("me"))?
            .next()
            .ok_or_else(|| Error::Host("no checkout".to_owned()))??
            .path();
        std::fs::write(&big, b)?;
        let second = ovr(repo.path(), "", host.path(), "true", b"").await?.sent;
        assert!(second >= 1 << 20, "b is new to the host");

        std::fs::write(&big, a)?;
        let run = ovr(repo.path(), "", host.path(), "wc -c < big", b"").await?;
        assert_eq!(run.stdout.trim(), (1 << 20).to_string());
        assert_eq!(run.sent, 0, "a is still a base on both sides");
        let snapshot = Snapshot::take_at(repo.path()).await?;
        let kept = ovr_proto::retained::read(&checkout, "refs/ovr/kept/").await?;
        assert_eq!(kept, snapshot.deliver_to("box", "me").await?.bases);
        assert_eq!(kept.len(), 2);

        // A new tree using both retained states should send neither state's large blob.
        std::fs::write(repo.path().join("other"), b)?;
        let run = ovr(repo.path(), "", host.path(), "cat big other | wc -c", b"").await?;
        assert_eq!(run.stdout.trim(), (2 << 20).to_string());
        assert!(
            run.sent < first / 10,
            "the pack excludes objects from both bases"
        );
        Ok(())
    }

    /// Runs `script` without syncing, from `cwd` in `repo`, or outside a repository without one.
    async fn unsynced(
        repo: Option<&Path>,
        cwd: &str,
        host_root: &Path,
        script: &str,
    ) -> Result<Run, Error> {
        let (checkout, prefix) = match repo {
            Some(repo) => {
                let repository = Repository::locate(&repo.join(cwd))
                    .await?
                    .ok_or_else(|| Error::Host("not a repository".to_owned()))?;
                (Some(repository.checkout("me")), repository.prefix)
            }
            None => (None, String::new()),
        };
        let request = Enter {
            initiator: "me".to_owned(),
            checkout,
        };
        let exec = Exec {
            argv: vec!["sh".to_owned(), "-c".to_owned(), script.to_owned()],
            cwd: prefix,
            tty: false,
            cols: 80,
            rows: 24,
        };
        let (client, agent) = agent(host_root);
        let mut stderr = Vec::new();
        let (stream, position) = enter(client, &request, &exec, &mut stderr).await?;
        let (stdin, signals) = (mpsc::channel(1).1, mpsc::channel(1).1);
        to_end(stream, position, stdin, signals, stderr, agent).await
    }

    /// The physical path of `dir`, as `pwd -P` prints it: temporary directories can be under a symlink, as on macOS.
    fn physical(dir: &Path) -> Result<String, Error> {
        Ok(std::fs::canonicalize(dir)?.to_string_lossy().into_owned())
    }

    #[tokio::test]
    async fn without_syncing_runs_in_the_checkout_as_the_host_has_it() -> Result<(), Error> {
        let repo = repo()?;
        let host = tempfile::tempdir()?;
        let script = "echo built > src/main.rs";
        ovr(repo.path(), "", host.path(), script, b"").await?;
        let checkout = std::fs::read_dir(host.path().join("me"))?
            .next()
            .ok_or_else(|| Error::Host("no checkout".to_owned()))??
            .path();
        let applied = std::fs::read(checkout.join(".git/ovr-applied"))?;

        std::fs::write(repo.path().join("src/main.rs"), "local edit\n")?;
        std::fs::write(repo.path().join("src/new.rs"), "never sent\n")?;
        let script = "pwd -P; cat main.rs; ls";
        let run = unsynced(Some(repo.path()), "src", host.path(), script).await?;
        assert_eq!(
            (run.code, run.stdout),
            (
                0,
                format!("{}/src\nbuilt\ngone.rs\nmain.rs\n", physical(&checkout)?)
            ),
            "{}",
            run.stderr
        );
        assert!(!run.stderr.contains("checkout"), "{}", run.stderr);
        assert_eq!(
            std::fs::read(checkout.join(".git/ovr-applied"))?,
            applied,
            "nothing is applied"
        );
        let runs = list(agent(host.path()).0, "me").await?;
        let listed = runs.iter().find(|info| info.run == run.number);
        assert_eq!(
            listed.and_then(|info| info.checkout.clone()),
            checkout
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
        );

        std::fs::create_dir(repo.path().join("docs"))?;
        std::fs::write(repo.path().join("docs/new.md"), "new\n")?;
        let missing = unsynced(Some(repo.path()), "docs", host.path(), "true").await;
        assert!(
            matches!(&missing, Err(Error::Remote(message))
                if message == "docs is not in the host's checkout as it is; run without -n to sync it first"),
            "{:?}",
            missing.map(|run| run.stdout)
        );
        Ok(())
    }

    #[tokio::test]
    async fn without_a_checkout_runs_in_the_base_directory() -> Result<(), Error> {
        let repo = repo()?;
        let host = tempfile::tempdir()?;
        let base = host.path().join(".base");
        std::fs::create_dir_all(&base)?;
        // The base directory may be a repository of the host user's; nothing must collect garbage there.
        git(&base, &["init", "-q"]).await?;
        let here = format!("{}\n", physical(&base)?);

        let inside = unsynced(Some(repo.path()), "src", host.path(), "pwd -P").await?;
        assert_eq!((inside.code, inside.stdout.as_str()), (0, here.as_str()));
        let name = Repository::locate(repo.path())
            .await?
            .map(|repository| repository.checkout("me"))
            .unwrap_or_default();
        let warning = format!(
            "ovr: this host has no checkout of {name} yet; running in {}, not in src\n",
            base.display()
        );
        assert!(inside.stderr.starts_with(&warning), "{}", inside.stderr);
        assert!(
            !host.path().join("me").join(&name).exists(),
            "no checkout is created"
        );

        let outside = unsynced(None, "", host.path(), "pwd -P").await?;
        assert_eq!((outside.code, outside.stdout.as_str()), (0, here.as_str()));
        assert!(!outside.stderr.contains("checkout"), "{}", outside.stderr);

        let runs = list(agent(host.path()).0, "me").await?;
        let checkouts: Vec<_> = runs
            .iter()
            .filter(|info| [inside.number, outside.number].contains(&info.run))
            .map(|info| info.checkout.clone())
            .collect();
        assert_eq!(checkouts, [None, None]);
        // A keeper lets go of its run only after it would have collected garbage.
        for run in [inside.number, outside.number] {
            let lock = host.path().join(".runs").join(run.to_string()).join("lock");
            let mut released = false;
            for _ in 0..400 {
                if ovr_proto::lock::Lock::try_exclusive(&lock)?.is_some() {
                    released = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            assert!(released, "the keeper of run {run} never finished");
        }
        assert!(
            !base.join(".git/ovr-gc.lock").exists(),
            "the keeper left the base directory alone"
        );
        Ok(())
    }

    async fn git(dir: &Path, args: &[&str]) -> Result<String, Error> {
        let identity = [
            "GIT_AUTHOR_NAME",
            "GIT_AUTHOR_EMAIL",
            "GIT_COMMITTER_NAME",
            "GIT_COMMITTER_EMAIL",
        ]
        .map(|key| (key, std::ffi::OsStr::new("ovr")));
        let out = ovr_proto::git::run_env(dir, args, &identity).await?;
        Ok(out.trim().to_owned())
    }

    /// Incompressible bytes, so a pack carrying them outgrows every buffer and cannot shrink.
    fn noise(len: usize) -> Vec<u8> {
        let mut state = 1u64;
        (0..len)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                (state >> 56) as u8
            })
            .collect()
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

    #[tokio::test]
    async fn a_broken_stream_resumes_exactly_where_it_left_off() -> Result<(), Error> {
        let repo = repo()?;
        let host = tempfile::tempdir()?;
        let go = host.path().join("go");
        let script = format!(
            "echo one; while [ ! -f {} ]; do sleep 0.05; done; echo two; exit 4",
            go.display()
        );
        let (mut stream, mut position, first_agent) =
            begin(repo.path(), host.path(), &script).await?;

        let (stdout, mut seen) = tokio::io::duplex(64);
        let mut io = Io {
            stdin: mpsc::channel(1).1,
            signals: mpsc::channel(1).1,
            stdout,
            stderr: tokio::io::sink(),
        };
        let break_after_first_line = async {
            let mut first = [0; 4];
            seen.read_exact(&mut first).await?;
            first_agent.abort();
            Ok::<_, Error>(first)
        };
        let (broken, first) = tokio::join!(
            relay(&mut stream, &mut io, &mut position, Mode::Drive),
            break_after_first_line
        );
        assert!(matches!(broken, Err(Error::Disconnected)), "{broken:?}");
        assert_eq!(&first?, b"one\n");
        assert_eq!(position.out, 4);

        std::fs::write(&go, "")?;
        let (mut stream, attached) = attach(agent(host.path()).0, position.resume()).await?;
        assert_eq!(position.resumed(attached), 0, "nothing was dropped");
        let (end, rest) = follow(&mut stream, &mut position, Mode::Drive).await;
        assert_eq!((end?, rest.as_str()), (End::Exit(4), "two\n"));
        Ok(())
    }

    #[tokio::test]
    async fn runs_share_the_checkout_side_by_side() -> Result<(), Error> {
        let repo = repo()?;
        let host = tempfile::tempdir()?;
        let marker = host.path().join("marker");
        let script = format!(
            "while [ ! -f {} ]; do sleep 0.05; done; echo released",
            marker.display()
        );
        let (mut waiting, mut position, _agent) = begin(repo.path(), host.path(), &script).await?;

        let script = format!("touch {}; cat src/main.rs", marker.display());
        let other = tokio::time::timeout(
            Duration::from_secs(20),
            ovr(repo.path(), "", host.path(), &script, b""),
        )
        .await
        .map_err(|_| Error::Host("the second run waited for the first".to_owned()))??;
        assert_eq!((other.code, other.stdout.as_str()), (0, "v1\n"));
        let (end, out) = follow(&mut waiting, &mut position, Mode::Drive).await;
        assert_eq!((end?, out.as_str()), (End::Exit(0), "released\n"));
        Ok(())
    }

    #[tokio::test]
    async fn a_late_snapshot_never_rolls_the_checkout_back() -> Result<(), Error> {
        let repo = repo()?;
        let host = tempfile::tempdir()?;
        let main = repo.path().join("src/main.rs");
        let first = Snapshot::take_at(repo.path()).await?;
        std::fs::write(&main, "v2\n")?;
        let second = Snapshot::take_at(repo.path()).await?;
        std::fs::write(&main, "v1\n")?;
        let third = Snapshot::take_at(repo.path()).await?;
        assert_eq!(third.commit, first.commit, "same tree, later snapshot");

        // The same tree arriving late must not lower the bar for the snapshot taken between them.
        for late in [&third, &first, &second] {
            let none = mpsc::channel(1).1;
            let run = run_snapshot(late, host.path(), "cat src/main.rs", b"", none).await?;
            assert_eq!(run.stdout, "v1\n");
        }

        // A recovery waiting for its replacement pack must not block a newer sync, or lose its offered base
        // when that newer run finishes and triggers collection.
        let delivery = first.deliver_to("box", "me").await?;
        let checkout = host.path().join("me").join(&delivery.checkout);
        git(&checkout, &["config", "gc.autoPackLimit", "1"]).await?;
        git(&checkout, &["config", "gc.pruneExpire", "now"]).await?;
        let (client, agent) = agent(host.path());
        let mut stream = Stream::new(client);
        let mut hello = delivery.hello();
        hello.bases = vec!["0".repeat(40)];
        stream.send(Frame::json(FrameKind::Hello, &hello)?).await?;
        let welcome: Welcome = next(&mut stream.frames).await?.expect(FrameKind::Welcome)?;
        assert_eq!(
            welcome,
            Welcome::Rebase {
                have: Some(first.commit.clone())
            }
        );
        std::fs::write(&main, "v4\n")?;
        let newer = tokio::time::timeout(
            Duration::from_secs(20),
            ovr(repo.path(), "", host.path(), "cat src/main.rs", b""),
        )
        .await
        .map_err(|_| Error::Host("recovery blocked a concurrent sync".to_owned()))??;
        assert_eq!(newer.stdout, "v4\n");
        // Only the recovery's hold on the objects keeps its base now.
        ovr_proto::retained::forget(&checkout, "refs/ovr/kept/").await?;
        let checkout = ovrd::agent::checkout::Checkout::at(checkout);
        checkout.collect_garbage().await;
        {
            let _objects = checkout.hold_objects().await?;
            assert!(checkout.has_commit(&first.commit).await);
        }

        let request = exec(&first, "cat src/main.rs");
        for _ in 0..2 {
            stream.send(Frame::empty(FrameKind::Pack)).await?;
            stream.send(Frame::json(FrameKind::Exec, &request)?).await?;
        }
        assert_eq!(next(&mut stream.frames).await?.kind, FrameKind::Synced);
        let attached = next(&mut stream.frames)
            .await?
            .expect(FrameKind::Attached)?;
        let (end, out) = follow(&mut stream, &mut Position::new(attached), Mode::Drive).await;
        assert_eq!((end?, out.as_str()), (End::Exit(0), "v4\n"));
        drop(stream);
        agent.await.map_err(|_| Error::Disconnected)?;
        ovr_proto::retained::forget(checkout.dir(), "refs/ovr/kept/").await?;
        checkout.collect_garbage().await;
        let _objects = checkout.hold_objects().await?;
        assert!(!checkout.has_commit(&first.commit).await);
        Ok(())
    }

    #[tokio::test]
    async fn runs_are_killed_listed_and_replayed_from_other_streams() -> Result<(), Error> {
        let repo = repo()?;
        let host = tempfile::tempdir()?;
        let started = host.path().join("started");
        let script = format!("echo hi; touch {}; sleep 30", started.display());
        let (stream, position, _agent) = begin(repo.path(), host.path(), &script).await?;
        drop(stream);
        wait_for(&started).await?;

        kill(agent(host.path()).0, position.run).await?;
        let runs = list(agent(host.path()).0, "me").await?;
        let states: Vec<_> = runs.iter().map(|info| (info.run, &info.state)).collect();
        assert_eq!(
            states,
            [(position.run, &RunState::Exited { code: 128 + 9 })]
        );
        assert_eq!(list(agent(host.path()).0, "someone else").await?, []);

        let from_start = Attach {
            run: position.run,
            out: 0,
            err: 0,
        };
        let (mut stream, attached) = attach(agent(host.path()).0, from_start).await?;
        let mut replayed = Position::new(attached);
        let (end, out) = follow(&mut stream, &mut replayed, Mode::Watch).await;
        assert_eq!((end?, out.as_str()), (End::Exit(128 + 9), "hi\n"));
        Ok(())
    }

    #[tokio::test]
    async fn the_end_of_input_reaches_the_command_while_others_watch() -> Result<(), Error> {
        let repo = repo()?;
        let host = tempfile::tempdir()?;
        let (mut driver, mut position, _agent) =
            begin(repo.path(), host.path(), "cat; echo eof").await?;
        let (_watcher, _) = attach(agent(host.path()).0, position.resume()).await?;

        let (stdin_tx, stdin) = mpsc::channel(1);
        stdin_tx
            .send(Bytes::from_static(b"data\n"))
            .await
            .map_err(|_| Error::Disconnected)?;
        drop(stdin_tx);
        let mut stdout = Vec::new();
        let mut io = Io {
            stdin,
            signals: mpsc::channel(1).1,
            stdout: &mut stdout,
            stderr: tokio::io::sink(),
        };
        let end = tokio::time::timeout(
            Duration::from_secs(20),
            relay(&mut driver, &mut io, &mut position, Mode::Drive),
        )
        .await
        .map_err(|_| Error::Host("the command never saw the end of its input".to_owned()))??;
        assert_eq!(
            (end, stdout.as_slice()),
            (End::Exit(0), &b"data\neof\n"[..])
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_host_that_fails_mid_pack_is_heard_instead_of_waited_on() -> Result<(), Error> {
        let repo = repo()?;
        let confirmed = Snapshot::take_at(repo.path()).await?;
        confirmed
            .deliver_to("box", "me")
            .await?
            .synced(None)
            .await?;
        std::fs::write(repo.path().join("noise"), noise(2 << 20))?;
        let snapshot = Snapshot::take_at(repo.path()).await?;
        let delivery = snapshot.deliver_to("box", "me").await?;

        // A host that fails after the first pack frame, says why, and stops reading without closing the stream.
        let (client, host) = tokio::io::duplex(1 << 16);
        let host = tokio::spawn(async move {
            let (mut frames, mut writer) = frame::split(host);
            frames.next().await;
            writer
                .send(Frame::json(FrameKind::Welcome, &Welcome::Ok)?)
                .await?;
            frames.next().await;
            let failure = Failure {
                message: "disk full".to_owned(),
            };
            writer
                .send(Frame::json(FrameKind::Error, &failure)?)
                .await?;
            std::future::pending::<()>().await;
            Ok::<_, ovr_proto::Error>((frames, writer))
        });

        let exec = exec(&snapshot, "true");
        let started = tokio::time::timeout(
            Duration::from_secs(10),
            start(client, &delivery, &exec, &mut tokio::io::sink()),
        )
        .await;
        host.abort();
        assert!(
            matches!(started, Ok(Err(Error::Remote(ref message))) if message == "disk full"),
            "{:?}",
            started.map(|started| started.map(|_| ()))
        );
        assert_eq!(
            snapshot.deliver_to("box", "me").await?.bases,
            [confirmed.commit]
        );
        Ok(())
    }
}
