//! The agent half of a run; see the client half in `ovr`'s `session` module for the sequence.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::path::Path;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use ovr_proto::frame::{self, INPUT_WINDOW, MAX_LEN, Reader, Writer};
use ovr_proto::msg::{Consumed, Exec, Exit, Failure, Hello, Resize, Welcome};
use ovr_proto::{Frame, FrameKind};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};

use super::Config;
use super::checkout::Checkout;
use super::process::{Event, Process};
use crate::Error;

/// Exit code of a run interrupted before its command started, as a shell reports for `SIGINT`.
const INTERRUPTED: i32 = 130;

/// Serves one run on `stream`, reporting failures to the client as `ERROR`.
pub async fn serve<S>(stream: S, config: &Config)
where
    S: AsyncRead + AsyncWrite,
{
    let (mut frames, mut writer) = frame::split(stream);
    let last = match run(&mut writer, &mut frames, config).await {
        Ok(code) => Frame::json(FrameKind::Exit, &Exit { code }),
        Err(Error::Disconnected) => return,
        Err(err) => {
            tracing::warn!(%err, "run failed");
            let message = err.to_string();
            Frame::json(FrameKind::Error, &Failure { message })
        }
    };
    let sent = match last {
        Ok(last) => writer.send(last).await,
        Err(err) => Err(err),
    };
    match sent {
        Ok(()) => {
            let _ = writer.close().await;
        }
        Err(err) => tracing::info!(%err, "cannot report the end of a run"),
    }
}

async fn run<S: AsyncRead + AsyncWrite>(
    writer: &mut Writer<S>,
    frames: &mut Reader<S>,
    config: &Config,
) -> Result<i32, Error> {
    let hello: Hello = next(frames).await?.expect(FrameKind::Hello)?;
    let checkout = Checkout::open(&config.root, &hello.repo).await?;
    let welcome = match &hello.base {
        Some(base) if !checkout.has_commit(base).await => Welcome::NeedFull,
        _ => Welcome::Ok,
    };
    writer
        .send(Frame::json(FrameKind::Welcome, &welcome)?)
        .await?;
    if welcome == Welcome::NeedFull {
        // The speculative pack is useless without its base; skip it and its EXEC.
        while next(frames).await?.kind != FrameKind::Exec {}
    }
    receive_pack(frames, &checkout).await?;
    let exec: Exec = next(frames).await?.expect(FrameKind::Exec)?;

    // Frames keep being read while queued behind another run, so Ctrl-C or a vanished client ends the wait.
    let mut stdin = Stdin::default();
    let lock = checkout.lock();
    tokio::pin!(lock);
    let _lock = loop {
        tokio::select! {
            lock = &mut lock => break lock?,
            frame = next(frames) => match stdin.receive(frame?)? {
                Some(Signal::Interrupt) => return Ok(INTERRUPTED),
                Some(Signal::Resize(size)) => stdin.size = Some(size),
                None => {}
            },
        }
    };
    let warning = checkout.apply(&hello.snapshot).await?;
    writer.send(Frame::empty(FrameKind::Synced)).await?;
    if let Some(warning) = warning {
        writer
            .send(Frame::new(FrameKind::Err, format!("ovr: {warning}\n")))
            .await?;
    }
    let cwd = checkout.cwd(&exec.cwd)?;
    let code = execute(writer, frames, config, &exec, &cwd, stdin).await?;
    checkout.collect_garbage();
    Ok(code)
}

/// Indexes pack frames as they arrive, until the empty end-of-pack frame.
async fn receive_pack<S: AsyncRead>(
    frames: &mut Reader<S>,
    checkout: &Checkout,
) -> Result<(), Error> {
    let mut indexer = None;
    loop {
        let frame = next(frames).await?;
        if frame.kind != FrameKind::Pack {
            return Err(ovr_proto::Error::Unexpected(frame.kind).into());
        }
        if frame.payload.is_empty() {
            break;
        }
        let indexer = match &mut indexer {
            Some(indexer) => indexer,
            None => indexer.insert(checkout.index_pack()?),
        };
        if indexer.write(&frame.payload).await.is_err() {
            // Git stopped reading; its exit status explains why.
            break;
        }
    }
    match indexer {
        Some(indexer) => indexer.finish().await,
        None => Ok(()),
    }
}

/// Relays output and input concurrently, so that a blocked client terminal never stops interrupts from arriving and a
/// command ignoring its input never stops output from leaving.
async fn execute<S: AsyncRead + AsyncWrite>(
    writer: &mut Writer<S>,
    frames: &mut Reader<S>,
    config: &Config,
    exec: &Exec,
    cwd: &Path,
    mut stdin: Stdin,
) -> Result<i32, Error> {
    let (mut process, mut output) = Process::spawn(&config.shell, exec, cwd).map_err(|err| {
        Error::Invalid(format!(
            "cannot start {}: {err}",
            exec.argv.first().map_or("shell", |p| p)
        ))
    })?;
    if let Some(Resize { cols, rows }) = stdin.size.take() {
        process.resize(cols, rows);
    }
    let mut consumed = stdin.consumed.subscribe();

    let send_output = async {
        loop {
            tokio::select! {
                event = output.next() => match event {
                    Some(Event::Stdout(data)) => writer.send(Frame::new(FrameKind::Out, data)).await?,
                    Some(Event::Stderr(data)) => writer.send(Frame::new(FrameKind::Err, data)).await?,
                    Some(Event::Exit(code)) => return Ok(code),
                    Some(Event::Lost(err)) => {
                        return Err(Error::Invalid(format!("lost track of the command: {err}")));
                    }
                    None => return Err(Error::Invalid("the command ended without a status".to_owned())),
                },
                Ok(()) = consumed.changed() => {
                    let bytes = *consumed.borrow_and_update();
                    writer.send(Frame::json(FrameKind::Consumed, &Consumed { bytes })?).await?;
                }
            }
        }
    };
    let take_input = async {
        loop {
            if stdin.eof && stdin.chunks.is_empty() {
                process.close_input();
            }
            let input = process.input().cloned();
            tokio::select! {
                permit = reserve(input.as_ref()), if !stdin.chunks.is_empty() => match permit {
                    Some(permit) => permit.send(stdin.pop()),
                    None => stdin.discard(),
                },
                frame = next(frames) => match stdin.receive(frame?)? {
                    Some(Signal::Interrupt) => process.interrupt(),
                    Some(Signal::Resize(Resize { cols, rows })) => process.resize(cols, rows),
                    None => {}
                },
            }
        }
    };
    tokio::select! {
        code = send_output => code,
        Err::<Infallible, Error>(err) = take_input => Err(err),
    }
}

/// Input for the command that it has not taken yet. The client stops sending once [`INPUT_WINDOW`] is unconsumed, so holding it here costs bounded memory and frames can always be read.
#[derive(Default)]
struct Stdin {
    chunks: VecDeque<Bytes>,
    held: u64,
    consumed: watch::Sender<u64>,
    eof: bool,
    /// The command closed its input; anything more is discarded.
    closed: bool,
    /// The latest terminal size, received before the command started.
    size: Option<Resize>,
}

enum Signal {
    Interrupt,
    Resize(Resize),
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
