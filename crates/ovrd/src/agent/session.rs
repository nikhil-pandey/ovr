//! The agent half of a stream; see the client half in `ovr`'s `session` module for the sequence.
//!
//! A stream starts a run (`HELLO`), resumes one (`ATTACH`), or lists runs (`RUNS`). Once a run has started, the
//! agent only relays frames between the client and the run's keeper, so the stream's end never ends the run.

use futures::{SinkExt, StreamExt};
use ovr_proto::frame::{self, Reader, Writer};
use ovr_proto::msg::{Attach, Attached, Exec, Failure, Hello, RunList, Runs, Welcome};
use ovr_proto::{Frame, FrameKind};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;

use super::Config;
use super::checkout::Checkout;
use super::keeper;
use super::log::{Progress, Stream, Tail};
use super::runs::{self, Meta, Run, Store};
use crate::Error;

/// Serves one stream on `stream`, reporting failures to the client as `ERROR`.
pub async fn serve<S>(stream: S, config: &Config)
where
    S: AsyncRead + AsyncWrite,
{
    let (mut frames, mut writer) = frame::split(stream);
    let result = dispatch(&mut writer, &mut frames, config).await;
    let failure = match result {
        Ok(()) => None,
        Err(Error::Disconnected) => return,
        Err(err) => {
            tracing::warn!(%err, "stream failed");
            Some(err.to_string())
        }
    };
    if let Some(message) = failure {
        match Frame::json(FrameKind::Error, &Failure { message }) {
            Ok(frame) => {
                if let Err(err) = writer.send(frame).await {
                    tracing::info!(%err, "cannot report a failure");
                }
            }
            Err(err) => tracing::warn!(%err, "cannot encode a failure"),
        }
    }
    let _ = writer.close().await;
}

async fn dispatch<S: AsyncRead + AsyncWrite>(
    writer: &mut Writer<S>,
    frames: &mut Reader<S>,
    config: &Config,
) -> Result<(), Error> {
    let first = next(frames).await?;
    let store = Store::new(config.runs.clone());
    match first.kind {
        FrameKind::Hello => {
            let run = start(writer, frames, config, &store, first.parse()?).await?;
            let attach = Attach {
                run: run.id,
                out: 0,
                err: 0,
            };
            attach_to(writer, frames, &run, attach).await
        }
        FrameKind::Attach => {
            let attach: Attach = first.parse()?;
            attach_to(writer, frames, &store.open(attach.run)?, attach).await
        }
        FrameKind::Runs => {
            let Runs { initiator } = first.parse()?;
            let runs = store.list(&initiator)?;
            writer
                .send(Frame::json(FrameKind::RunList, &RunList { runs })?)
                .await?;
            Ok(())
        }
        kind => Err(ovr_proto::Error::Unexpected(kind).into()),
    }
}

/// Syncs the checkout, records the run, and starts its keeper.
async fn start<S: AsyncRead + AsyncWrite>(
    writer: &mut Writer<S>,
    frames: &mut Reader<S>,
    config: &Config,
    store: &Store,
    hello: Hello,
) -> Result<Run, Error> {
    let checkout = Checkout::open(&config.root, &hello.checkout).await?;
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

    let warning = checkout.apply(&hello.snapshot, hello.seq).await?;
    writer.send(Frame::empty(FrameKind::Synced)).await?;
    if let Some(warning) = warning {
        writer
            .send(Frame::new(FrameKind::Err, format!("ovr: {warning}\n")))
            .await?;
    }
    let meta = Meta {
        initiator: hello.initiator,
        checkout: hello.checkout,
        dir: checkout.dir().to_owned(),
        cwd: checkout.cwd(&exec.cwd)?,
        exec,
        started: runs::now(),
    };
    let run = store.create(&meta).await?;
    keeper::start(config, &run).await?;
    Ok(run)
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

/// Relays between the client and the run's keeper or, once the run has ended, replays its log from disk.
async fn attach_to<S: AsyncRead + AsyncWrite>(
    writer: &mut Writer<S>,
    frames: &mut Reader<S>,
    run: &Run,
    attach: Attach,
) -> Result<(), Error> {
    if run.ended()?.is_none()
        && let Ok(stream) = keeper::connect(run).await
    {
        let (mut kept_frames, mut kept_writer) = frame::split(stream);
        let attached = async {
            kept_writer
                .send(Frame::json(FrameKind::Attach, &attach)?)
                .await?;
            kept_frames
                .next()
                .await
                .ok_or(Error::Disconnected)?
                .map_err(Error::from)
        };
        // A keeper that is just finishing may close without answering; the log on disk then has everything.
        if let Ok(attached) = attached.await {
            writer.send(attached).await?;
            return relay(writer, frames, &mut kept_writer, &mut kept_frames).await;
        }
    }
    replay(writer, run, attach).await
}

/// Copies frames both ways until the keeper is done, which ends the stream, or the client goes away, which leaves
/// the run be.
async fn relay<C, K>(
    writer: &mut Writer<C>,
    frames: &mut Reader<C>,
    kept_writer: &mut Writer<K>,
    kept_frames: &mut Reader<K>,
) -> Result<(), Error>
where
    C: AsyncRead + AsyncWrite,
    K: AsyncRead + AsyncWrite,
{
    let up = async {
        loop {
            let frame = next(frames).await?;
            if kept_writer.send(frame).await.is_err() {
                // The keeper is done; its last frames, or its end, finish the relay.
                return std::future::pending().await;
            }
        }
    };
    let down = async {
        while let Some(frame) = kept_frames.next().await {
            writer.send(frame?).await?;
        }
        Ok::<(), Error>(())
    };
    tokio::select! {
        result = up => result,
        result = down => result,
    }
}

/// Sends the log of a run that has no keeper from the requested offsets, then how it ended.
async fn replay<S: AsyncRead + AsyncWrite>(
    writer: &mut Writer<S>,
    run: &Run,
    attach: Attach,
) -> Result<(), Error> {
    let last = keeper::last_frame(run)?;
    let (_ended, progress) = watch::channel(Progress::finished(&run.dir)?);
    let mut out = Tail::open(&run.dir, Stream::Out, attach.out, progress.clone())?;
    let mut err = Tail::open(&run.dir, Stream::Err, attach.err, progress)?;
    let attached = Attached {
        run: run.id,
        tty: run.meta()?.exec.tty,
        out: out.pos(),
        err: err.pos(),
    };
    writer
        .send(Frame::json(FrameKind::Attached, &attached)?)
        .await?;
    keeper::send_log(writer, &mut out, &mut err, None).await?;
    writer.send(last).await?;
    Ok(())
}

async fn next<S: AsyncRead>(frames: &mut Reader<S>) -> Result<Frame, Error> {
    Ok(frames.next().await.ok_or(Error::Disconnected)??)
}
