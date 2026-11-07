//! The agent half of a run; see the client half in `ovr`'s `session` module for the sequence.

use std::collections::VecDeque;
use std::path::Path;

use ovr_proto::FrameKind;
use ovr_proto::frame::{self, Frame};
use ovr_proto::msg::{Exec, Exit, Failure, Hello, Resize, Welcome};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use super::Config;
use super::checkout::Checkout;
use super::process::{Event, Process};
use crate::Error;

type Frames = mpsc::Receiver<Result<Frame, ovr_proto::Error>>;

/// Input a command has not consumed yet that is held here while frames keep being read, so that interrupts and a
/// vanished client are still noticed. Beyond this, frames stop being read and the client is pushed back.
const INPUT_BACKLOG: usize = 1024 * 1024;

/// Serves one run on `stream`, reporting failures to the client as `ERROR`.
pub async fn serve<S>(stream: S, config: &Config)
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut frames = frame::spawn_reader(reader);
    let result = run(&mut writer, &mut frames, config).await;
    let sent = match result {
        Ok(code) => frame::send(&mut writer, FrameKind::Exit, &Exit { code }).await,
        Err(Error::Disconnected) => Ok(()),
        Err(err) => {
            tracing::warn!(%err, "run failed");
            let message = err.to_string();
            frame::send(&mut writer, FrameKind::Error, &Failure { message }).await
        }
    };
    if sent.is_ok() {
        let _ = writer.shutdown().await;
    }
}

async fn run<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frames: &mut Frames,
    config: &Config,
) -> Result<i32, Error> {
    let hello: Hello = next(frames).await?.expect(FrameKind::Hello)?;
    let checkout = Checkout::open(&config.root, &hello.repo).await?;
    let welcome = match &hello.base {
        Some(base) if !checkout.has_commit(base).await => Welcome::NeedFull,
        _ => Welcome::Ok,
    };
    frame::send(writer, FrameKind::Welcome, &welcome).await?;
    if welcome == Welcome::NeedFull {
        // The speculative pack is useless without its base; skip it and its EXEC.
        while next(frames).await?.kind != FrameKind::Exec {}
    }
    receive_pack(frames, &checkout).await?;
    let exec: Exec = next(frames).await?.expect(FrameKind::Exec)?;

    let _lock = checkout.lock().await?;
    let warning = checkout.apply(&hello.snapshot).await?;
    frame::write(writer, FrameKind::Synced, &[]).await?;
    if let Some(warning) = warning {
        frame::write(
            writer,
            FrameKind::Err,
            format!("ovr: {warning}\n").as_bytes(),
        )
        .await?;
    }
    let cwd = checkout.cwd(&exec.cwd)?;
    let code = execute(writer, frames, config, &exec, &cwd).await?;
    checkout.collect_garbage();
    Ok(code)
}

/// Indexes pack frames as they arrive, until the empty end-of-pack frame.
async fn receive_pack(frames: &mut Frames, checkout: &Checkout) -> Result<(), Error> {
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

async fn execute<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frames: &mut Frames,
    config: &Config,
    exec: &Exec,
    cwd: &Path,
) -> Result<i32, Error> {
    let mut process = Process::spawn(&config.shell, exec, cwd).map_err(|err| {
        Error::Invalid(format!(
            "cannot start {}: {err}",
            exec.argv.first().map_or("shell", |p| p)
        ))
    })?;
    let mut backlog = Backlog::default();
    loop {
        if backlog.eof && backlog.chunks.is_empty() {
            process.close_input();
        }
        let input = process.input().cloned();
        tokio::select! {
            event = process.next() => match event {
                Some(Event::Stdout(data)) => frame::write(writer, FrameKind::Out, &data).await?,
                Some(Event::Stderr(data)) => frame::write(writer, FrameKind::Err, &data).await?,
                Some(Event::Exit(code)) => return Ok(code),
                Some(Event::Lost(err)) => return Err(Error::Invalid(format!("lost track of the command: {err}"))),
                None => return Err(Error::Invalid("the command ended without a status".to_owned())),
            },
            permit = reserve(input.as_ref()), if !backlog.chunks.is_empty() => match permit {
                Some(permit) => permit.send(backlog.pop()),
                // The command closed its input; whatever is left can never be delivered.
                None => backlog = Backlog { eof: backlog.eof, ..Backlog::default() },
            },
            frame = next(frames), if backlog.bytes < INPUT_BACKLOG => {
                let frame = frame?;
                match frame.kind {
                    FrameKind::In if frame.payload.is_empty() => backlog.eof = true,
                    FrameKind::In => backlog.push(frame.payload),
                    FrameKind::Resize => {
                        let Resize { cols, rows } = frame.parse()?;
                        process.resize(cols, rows);
                    }
                    FrameKind::Interrupt => process.interrupt(),
                    kind => return Err(ovr_proto::Error::Unexpected(kind).into()),
                }
            }
        }
    }
}

#[derive(Default)]
struct Backlog {
    chunks: VecDeque<Vec<u8>>,
    bytes: usize,
    eof: bool,
}

impl Backlog {
    fn push(&mut self, data: Vec<u8>) {
        self.bytes += data.len();
        self.chunks.push_back(data);
    }

    fn pop(&mut self) -> Vec<u8> {
        let data = self.chunks.pop_front().unwrap_or_default();
        self.bytes -= data.len();
        data
    }
}

async fn reserve(input: Option<&mpsc::Sender<Vec<u8>>>) -> Option<mpsc::Permit<'_, Vec<u8>>> {
    input?.reserve().await.ok()
}

async fn next(frames: &mut Frames) -> Result<Frame, Error> {
    match frames.recv().await {
        Some(frame) => Ok(frame?),
        None => Err(Error::Disconnected),
    }
}
