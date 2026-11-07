//! The run keeper: `ovrd keep DIR`, one detached process per run that owns the command, its terminal, and its
//! output log, so that the run outlives every stream, connection, and agent. It serves clients on the run's
//! endpoint with the same frames a run uses after `EXEC`, starting from `ATTACH`.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use ovr_proto::frame::{self, INPUT_WINDOW, MAX_LEN, Reader, Writer};
use ovr_proto::msg::{Attach, Attached, Consumed, Exit, Failure, Resize, RunState};
use ovr_proto::{Frame, FrameKind, git, ipc};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use super::checkout::Checkout;
use super::log::{self, Progress, Stream, Tail};
use super::process::{Event, Process};
use super::runs::{Ended, Run};
use super::{Config, Shell};
use crate::Error;

/// How long the agent waits for a keeper to report that its command started.
const START_TIMEOUT: Duration = Duration::from_secs(10);

/// Starts the keeper for `run` and waits until its command has started, or failed to.
///
/// # Errors
///
/// Returns an error when the keeper or the command cannot be started.
pub async fn start(config: &Config, run: &Run) -> Result<(), Error> {
    if let Some(program) = &config.keeper {
        spawn(program, run)?;
    } else {
        let (run, shell) = (run.clone(), config.shell.clone());
        tokio::spawn(async move {
            if let Err(err) = keep(run, shell).await {
                tracing::warn!(%err, "the keeper failed");
            }
        });
    }
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        match run.ended()? {
            Some(Ended::Failed { message }) => return Err(Error::Invalid(message)),
            Some(_) => return Ok(()),
            None if run.dir.join("pid").is_file() => return Ok(()),
            None if Instant::now() > deadline => {
                return Err(Error::Invalid(format!(
                    "the run keeper did not start; {} may say why",
                    run.dir.join("keeper.log").display()
                )));
            }
            None => tokio::time::sleep(Duration::from_millis(5)).await,
        }
    }
}

/// Starts `program keep DIR` detached from the agent, so that the agent's end, or its SSH session's, leaves it be.
fn spawn(program: &std::path::Path, run: &Run) -> Result<(), Error> {
    let log = std::fs::File::create(run.dir.join("keeper.log"))?;
    let mut command = tokio::process::Command::new(program);
    command
        .arg("keep")
        .arg(&run.dir)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(log);
    #[cfg(unix)]
    let mut child = command.spawn()?;
    #[cfg(windows)]
    let mut child = {
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        const CREATE_BREAKAWAY_FROM_JOB: u32 = 0x0100_0000;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        let flags = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW;
        // Leave the agent's job, e.g. the Scheduled Task's, where the job allows it.
        command.creation_flags(flags | CREATE_BREAKAWAY_FROM_JOB);
        match command.spawn() {
            Ok(child) => child,
            Err(err) => {
                tracing::warn!(%err, "the run cannot leave the agent's job; ending that job ends the run");
                command.creation_flags(flags).spawn()?
            }
        }
    };
    // Reaps the keeper should it exit while this agent is still running.
    tokio::spawn(async move {
        let _ = child.wait().await;
    });
    Ok(())
}

/// `ovrd keep DIR`: keeps the run in `DIR` in a session of its own.
///
/// # Errors
///
/// Returns an error when the run cannot be kept.
pub async fn main(dir: PathBuf) -> Result<(), Error> {
    #[cfg(unix)]
    nix::unistd::setsid().map_err(std::io::Error::from)?;
    keep(Run::at(dir)?, Shell::detect()).await
}

/// Connects to `run`'s keeper.
///
/// # Errors
///
/// Returns an error when no keeper is listening.
pub async fn connect(run: &Run) -> std::io::Result<ipc::ClientStream> {
    ipc::connect_to(&run.endpoint()).await
}

struct Kept {
    run: Run,
    tty: bool,
    process: Mutex<Option<Process>>,
    progress: watch::Receiver<Progress>,
}

impl Kept {
    fn with<T>(&self, f: impl FnOnce(&mut Process) -> T) -> Option<T> {
        self.process
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
            .map(f)
    }

    /// Kills the command's whole process tree; its end is still recorded as usual.
    fn kill(&self) {
        drop(
            self.process
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take(),
        );
    }
}

async fn keep(run: Run, shell: Shell) -> Result<(), Error> {
    let _lock = git::lock(run.lock_path()).await?;
    // Recorded as lost while this keeper was starting.
    if run.ended()?.is_some() {
        return Ok(());
    }
    let meta = run.meta()?;
    let mut listener = ipc::Listener::bind_at(run.endpoint())?;
    let (process, mut output) = match Process::spawn(&shell, &meta.exec, &meta.cwd) {
        Ok(spawned) => spawned,
        Err(err) => {
            let program = meta.exec.argv.first().map_or("shell", |p| p);
            let message = format!("cannot start {program}: {err}");
            run.end(&Ended::Failed { message })?;
            return Ok(());
        }
    };
    run.started(process.id())?;
    let mut out = log::Writer::create(&run.dir, Stream::Out, log::SEGMENT)?;
    let mut err = log::Writer::create(&run.dir, Stream::Err, log::SEGMENT)?;
    let (progress_tx, progress) = watch::channel(Progress::default());
    let kept = Arc::new(Kept {
        run: run.clone(),
        tty: meta.exec.tty,
        process: Mutex::new(Some(process)),
        progress,
    });

    let record = async {
        let mut warned = false;
        loop {
            let (writer, data) = match output.next().await {
                Some(Event::Stdout(data)) => (&mut out, data),
                Some(Event::Stderr(data)) => (&mut err, data),
                Some(Event::Exit(code)) => return Ended::Exited { code },
                Some(Event::Lost(err)) => {
                    let message = format!("lost track of the command: {err}");
                    return Ended::Failed { message };
                }
                None => {
                    let message = "the command ended without a status".to_owned();
                    return Ended::Failed { message };
                }
            };
            match writer.append(&data) {
                Ok(()) => progress_tx.send_modify(|progress| {
                    progress.out = out.len();
                    progress.err = err.len();
                }),
                Err(err) if !warned => {
                    tracing::warn!(%err, "cannot log output; dropping it");
                    warned = true;
                }
                Err(_) => {}
            }
        }
    };
    tokio::pin!(record);
    let mut clients = JoinSet::new();
    let ended = loop {
        tokio::select! {
            ended = &mut record => break ended,
            accepted = listener.accept() => match accepted {
                Ok(stream) => {
                    clients.spawn(serve(stream, Arc::clone(&kept)));
                }
                Err(err) => tracing::warn!(%err, "accept failed"),
            },
            Some(_) = clients.join_next(), if !clients.is_empty() => {}
        }
    };
    // Recorded before clients see the end, so that anyone attaching from now on reads it from disk.
    run.end(&ended)?;
    progress_tx.send_modify(|progress| progress.ended = true);
    drop(listener);
    Checkout::at(meta.dir).collect_garbage();
    while clients.join_next().await.is_some() {}
    Ok(())
}

/// Serves one attached client until the run ends or the client goes away; neither stops the other.
async fn serve<S: AsyncRead + AsyncWrite>(stream: S, kept: Arc<Kept>) {
    let (mut frames, mut writer) = frame::split(stream);
    if let Err(err) = attach(&mut writer, &mut frames, &kept).await {
        tracing::debug!(%err, "client left");
    }
}

async fn attach<S: AsyncRead + AsyncWrite>(
    writer: &mut Writer<S>,
    frames: &mut Reader<S>,
    kept: &Kept,
) -> Result<(), Error> {
    let request: Attach = next(frames).await?.expect(FrameKind::Attach)?;
    let dir = &kept.run.dir;
    let mut out = Tail::open(dir, Stream::Out, request.out, kept.progress.clone())?;
    let mut err = Tail::open(dir, Stream::Err, request.err, kept.progress.clone())?;
    let attached = Attached {
        run: kept.run.id,
        tty: kept.tty,
        out: out.pos(),
        err: err.pos(),
    };
    writer
        .send(Frame::json(FrameKind::Attached, &attached)?)
        .await?;

    let mut stdin = Stdin::default();
    let mut consumed = stdin.consumed.subscribe();
    let send_output = async {
        send_log(writer, &mut out, &mut err, Some(&mut consumed)).await?;
        writer.send(last_frame(&kept.run)?).await?;
        Ok(())
    };
    let take_input = async {
        loop {
            if stdin.eof && stdin.chunks.is_empty() {
                kept.with(Process::close_input);
            }
            // Only while delivering: a held sender keeps the command's input open after another client ends it.
            let input = if stdin.chunks.is_empty() {
                None
            } else {
                kept.with(|process| process.input().cloned()).flatten()
            };
            tokio::select! {
                permit = reserve(input.as_ref()), if !stdin.chunks.is_empty() => match permit {
                    Some(permit) => permit.send(stdin.pop()),
                    None => stdin.discard(),
                },
                frame = next(frames) => match stdin.receive(frame?)? {
                    Some(Signal::Interrupt) => {
                        kept.with(|process| process.interrupt());
                    }
                    Some(Signal::Resize(Resize { cols, rows })) => {
                        kept.with(|process| process.resize(cols, rows));
                    }
                    Some(Signal::Kill) => kept.kill(),
                    None => {}
                },
            }
        }
    };
    tokio::select! {
        sent = send_output => sent,
        Err::<Infallible, Error>(err) = take_input => Err(err),
    }
}

/// Sends both streams of the log from the tails' positions to their end, reporting consumed input on the way.
///
/// # Errors
///
/// Returns an error when the log cannot be read or the client is gone.
pub async fn send_log<S: AsyncRead + AsyncWrite>(
    writer: &mut Writer<S>,
    out: &mut Tail,
    err: &mut Tail,
    mut consumed: Option<&mut watch::Receiver<u64>>,
) -> Result<(), Error> {
    let (mut out_done, mut err_done) = (false, false);
    while !(out_done && err_done) {
        tokio::select! {
            chunk = out.next(), if !out_done => match chunk? {
                Some(data) => writer.send(Frame::new(FrameKind::Out, data)).await?,
                None => out_done = true,
            },
            chunk = err.next(), if !err_done => match chunk? {
                Some(data) => writer.send(Frame::new(FrameKind::Err, data)).await?,
                None => err_done = true,
            },
            Some(bytes) = changed(consumed.as_deref_mut()) => {
                writer.send(Frame::json(FrameKind::Consumed, &Consumed { bytes })?).await?;
            }
        }
    }
    Ok(())
}

async fn changed(consumed: Option<&mut watch::Receiver<u64>>) -> Option<u64> {
    let consumed = consumed?;
    consumed.changed().await.ok()?;
    Some(*consumed.borrow_and_update())
}

/// `EXIT` or `ERROR` for a run whose output has all been sent.
///
/// # Errors
///
/// Returns an error when the run's record cannot be read.
pub fn last_frame(run: &Run) -> Result<Frame, Error> {
    let message = match run.state()? {
        RunState::Exited { code } => return Ok(Frame::json(FrameKind::Exit, &Exit { code })?),
        RunState::Failed { message } => message,
        RunState::Lost => format!(
            "run {} was lost: its keeper ended without recording how the command did",
            run.id
        ),
        RunState::Running => format!("run {} is not reachable right now; try again", run.id),
    };
    Ok(Frame::json(FrameKind::Error, &Failure { message })?)
}

/// Input for the command that it has not taken yet. The client stops sending once [`INPUT_WINDOW`] is unconsumed,
/// so holding it here costs bounded memory and frames can always be read.
#[derive(Default)]
struct Stdin {
    chunks: VecDeque<Bytes>,
    held: u64,
    consumed: watch::Sender<u64>,
    eof: bool,
    /// The command closed its input; anything more is discarded.
    closed: bool,
}

enum Signal {
    Interrupt,
    Resize(Resize),
    Kill,
}

impl Stdin {
    /// Holds input from `frame`, or returns the signal it carries.
    fn receive(&mut self, frame: Frame) -> Result<Option<Signal>, Error> {
        match frame.kind {
            FrameKind::In if frame.payload.is_empty() => self.eof = true,
            FrameKind::In if self.closed => self.consume(frame.payload.len()),
            FrameKind::In => {
                self.held += frame.payload.len() as u64;
                if self.held > INPUT_WINDOW + MAX_LEN as u64 {
                    return Err(Error::Invalid(
                        "the client sent more input than it may".to_owned(),
                    ));
                }
                self.chunks.push_back(frame.payload);
            }
            FrameKind::Resize => return Ok(Some(Signal::Resize(frame.parse()?))),
            FrameKind::Interrupt => return Ok(Some(Signal::Interrupt)),
            FrameKind::Kill => return Ok(Some(Signal::Kill)),
            kind => return Err(ovr_proto::Error::Unexpected(kind).into()),
        }
        Ok(None)
    }

    /// The next chunk, counted as consumed now that the command is taking it.
    fn pop(&mut self) -> Bytes {
        let data = self.chunks.pop_front().unwrap_or_default();
        self.held -= data.len() as u64;
        self.consume(data.len());
        data
    }

    /// The command closed its input, so what is held can never be delivered.
    fn discard(&mut self) {
        self.closed = true;
        while !self.chunks.is_empty() {
            self.pop();
        }
    }

    fn consume(&self, bytes: usize) {
        self.consumed.send_modify(|total| *total += bytes as u64);
    }
}

async fn reserve(input: Option<&mpsc::Sender<Bytes>>) -> Option<mpsc::Permit<'_, Bytes>> {
    input?.reserve().await.ok()
}

async fn next<S: AsyncRead>(frames: &mut Reader<S>) -> Result<Frame, Error> {
    Ok(frames.next().await.ok_or(Error::Disconnected)??)
}
