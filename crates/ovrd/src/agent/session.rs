//! The agent half of a stream; see the client half in `ovr`'s `session` module for the sequence.
//!
//! A stream starts a run (`HELLO`), resumes one (`ATTACH`), lists runs (`RUNS`), or cleans up (`CLEAN`). Once a run has started, the
//! agent only relays frames between the client and the run's keeper, so the stream's end never ends the run.

use std::time::Instant;

use futures::{SinkExt, StreamExt};
use ovr_proto::frame::{self, Reader, Writer};
use ovr_proto::msg::{Attach, Attached, Clean, Exec, Failure, Hello, RunList, Runs, Welcome};
use ovr_proto::{Frame, FrameKind};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;

use super::Config;
use super::checkout::{self, Checkout};
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
    let Some(message) = failure else {
        let _ = writer.close().await;
        return;
    };
    match Frame::json(FrameKind::Error, &Failure { message }) {
        Ok(frame) => {
            if let Err(err) = writer.send(frame).await {
                tracing::info!(%err, "cannot report a failure");
            }
        }
        Err(err) => tracing::warn!(%err, "cannot encode a failure"),
    }
    let _ = writer.close().await;
    // A client still sending a pack would otherwise wait forever for room to write, and never read why.
    while let Some(Ok(_)) = frames.next().await {}
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
            let (run, _claim) = start(writer, frames, config, &store, first.parse()?).await?;
            let attach = Attach {
                run: run.id,
                out: 0,
                err: 0,
            };
            attach_to(writer, frames, &run, attach).await
        }
        FrameKind::Attach => {
            let attach: Attach = first.parse()?;
            let run = store.open(attach.run)?;
            let meta = run.meta()?;
            let _claim = checkout::claim(&config.root, &meta.initiator, &meta.checkout).await?;
            attach_to(writer, frames, &run, attach).await
        }
        FrameKind::Runs => {
            let Runs { initiator } = first.parse()?;
            let runs = store.list(&initiator)?;
            writer
                .send(Frame::json(FrameKind::RunList, &RunList { runs })?)
                .await?;
            Ok(())
        }
        FrameKind::Clean => {
            let Clean { initiator } = first.parse()?;
            let config = config.clone();
            let cleaned =
                tokio::task::spawn_blocking(move || super::clean::clean(&config, Some(&initiator)))
                    .await
                    .map_err(std::io::Error::from)??;
            writer
                .send(Frame::json(FrameKind::Cleaned, &cleaned)?)
                .await?;
            Ok(())
        }
        kind => Err(ovr_proto::Error::Unexpected(kind).into()),
    }
}

/// Syncs the checkout, records the run, and starts its keeper; the checkout stays claimed until the claim is dropped.
async fn start<S: AsyncRead + AsyncWrite>(
    writer: &mut Writer<S>,
    frames: &mut Reader<S>,
    config: &Config,
    store: &Store,
    hello: Hello,
) -> Result<(Run, Option<std::fs::File>), Error> {
    let checkout = Checkout::open(&config.root, &hello.initiator, &hello.checkout).await?;
    let objects = checkout.hold_objects().await?;
    let welcome = if checkout.has_commits(&hello.bases).await {
        Welcome::Ok
    } else {
        Welcome::Rebase {
            have: checkout.newest().await?,
        }
    };
    writer
        .send(Frame::json(FrameKind::Welcome, &welcome)?)
        .await?;
    if welcome != Welcome::Ok {
        // The speculative pack is useless without its base; skip it and its EXEC.
        while next(frames).await?.kind != FrameKind::Exec {}
    }
    let received = Instant::now();
    let bytes = receive_pack(frames, &checkout).await?;
    let exec: Exec = next(frames).await?.expect(FrameKind::Exec)?;

    let applying = Instant::now();
    let warning = checkout.apply(&hello.snapshot, hello.seq).await?;
    drop(objects);
    writer.send(Frame::empty(FrameKind::Synced)).await?;
    tracing::info!(
        checkout = hello.checkout,
        bytes,
        receive_ms = (applying - received).as_millis(),
        apply_ms = applying.elapsed().as_millis(),
        "synced"
    );
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
    Ok((run, checkout.into_claim()))
}

/// Indexes pack frames as they arrive, until the empty end-of-pack frame, and returns the pack's size.
async fn receive_pack<S: AsyncRead>(
    frames: &mut Reader<S>,
    checkout: &Checkout,
) -> Result<u64, Error> {
    let mut indexer = None;
    let mut bytes = 0;
    loop {
        let frame = next(frames).await?;
        if frame.kind != FrameKind::Pack {
            return Err(ovr_proto::Error::Unexpected(frame.kind).into());
        }
        if frame.payload.is_empty() {
            break;
        }
        bytes += frame.payload.len() as u64;
        let indexer = match &mut indexer {
            Some(indexer) => indexer,
            None => indexer.insert(checkout.index_pack()?),
        };
        if indexer.write(&frame.payload).await.is_err() {
            // Git stopped reading; its exit status explains why.
            break;
        }
    }
    if let Some(indexer) = indexer {
        indexer.finish().await?;
    }
    Ok(bytes)
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

#[cfg(all(test, unix))]
mod tests {
    use std::time::Duration;

    use super::super::Shell;
    use super::*;
    use crate::mux::{Incoming, Mux};

    #[tokio::test]
    async fn a_failed_sync_lets_the_client_finish_sending_and_hear_why()
    -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let config = Config {
            root: dir.path().join("checkouts"),
            runs: dir.path().join("runs"),
            agents: dir.path().join("agents"),
            shell: Shell::new("/bin/sh"),
            keeper: None,
        };
        // Through the mux, whose flow control stalls a writer the other end stopped reading.
        let (initiator, host) = tokio::io::duplex(1 << 16);
        let mux = Mux::open_on(initiator);
        let mut incoming = Incoming::accept_on(host);
        let host = tokio::spawn(async move {
            if let Some(channel) = incoming.accept().await {
                serve(channel, &config).await;
            }
            // The connection outlives the stream, as with a real host.
            std::future::pending::<Incoming>().await
        });

        let (mut frames, mut writer) = frame::split(mux.open().await?);
        let hello = Hello {
            checkout: "repo-key".to_owned(),
            seq: 1,
            bases: Vec::new(),
            snapshot: "0".repeat(40),
            initiator: "me".to_owned(),
        };
        writer.send(Frame::json(FrameKind::Hello, &hello)?).await?;
        // Not a pack: git gives up on the first frame, well before the rest is sent.
        let sent = tokio::time::timeout(Duration::from_secs(10), async {
            for _ in 0..64 {
                writer
                    .send(Frame::new(FrameKind::Pack, vec![0xff; 64 * 1024]))
                    .await?;
            }
            writer.send(Frame::empty(FrameKind::Pack)).await
        })
        .await;
        assert!(matches!(sent, Ok(Ok(()))), "{sent:?}");

        let welcome: Welcome = next(&mut frames).await?.expect(FrameKind::Welcome)?;
        assert_eq!(welcome, Welcome::Ok);
        let failure: Failure = next(&mut frames).await?.expect(FrameKind::Error)?;
        assert!(failure.message.contains("pack"), "{}", failure.message);
        host.abort();
        Ok(())
    }
}
