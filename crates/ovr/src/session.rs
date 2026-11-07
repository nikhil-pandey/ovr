//! The client half of a run: sync the snapshot, start the command, and relay terminal I/O until it exits.

use std::convert::Infallible;

use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use ovr_proto::frame::{self, CHUNK, INPUT_WINDOW, Reader, Writer};
use ovr_proto::msg::{Consumed, Exec, Exit, Failure, Resize, Welcome};
use ovr_proto::{Frame, FrameKind};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, watch};

use crate::Error;
use crate::snapshot::Snapshot;

/// Exit code reported when a second interrupt abandons the run, as a shell does for `SIGINT`.
pub const INTERRUPTED: i32 = 130;

/// Local terminal events forwarded to the remote command, ahead of any pending input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Resize { cols: u16, rows: u16 },
    Interrupt,
}

/// Where a run's input comes from and its output goes.
pub struct Io<O, E> {
    /// Local stdin; it ends when the sender is dropped.
    pub stdin: mpsc::Receiver<Bytes>,
    pub signals: mpsc::Receiver<Signal>,
    pub stdout: O,
    pub stderr: E,
}

/// Runs `exec` against `snapshot` over an agent session and returns the remote exit code.
///
/// # Errors
///
/// Returns an error when syncing fails, the agent reports a failure, or the connection drops.
pub async fn run<S, O, E>(
    stream: S,
    snapshot: &Snapshot,
    exec: &Exec,
    io: Io<O, E>,
) -> Result<i32, Error>
where
    S: AsyncRead + AsyncWrite,
    O: AsyncWrite + Unpin,
    E: AsyncWrite + Unpin,
{
    let (mut frames, mut writer) = frame::split(stream);
    let mut synced = false;
    let result = async {
        sync(&mut writer, &mut frames, snapshot, exec).await?;
        relay(&mut writer, &mut frames, snapshot, &mut synced, io).await
    }
    .await;
    if result.is_err() && !synced {
        snapshot.forget().await;
    }
    result
}

/// Sends `HELLO`, a speculative thin pack, and `EXEC` without waiting, then repeats the pack in full if the host
/// lacks the base.
async fn sync<S: AsyncRead + AsyncWrite>(
    writer: &mut Writer<S>,
    frames: &mut Reader<S>,
    snapshot: &Snapshot,
    exec: &Exec,
) -> Result<(), Error> {
    writer
        .send(Frame::json(FrameKind::Hello, &snapshot.hello())?)
        .await?;
    let mut welcome = send_pack(writer, frames, snapshot, snapshot.base.as_deref(), true).await?;
    writer.send(Frame::json(FrameKind::Exec, exec)?).await?;
    let welcome = match welcome.take() {
        Some(welcome) => welcome,
        None => next(frames).await?.expect(FrameKind::Welcome)?,
    };
    if welcome == Welcome::NeedFull {
        send_pack(writer, frames, snapshot, None, false).await?;
        writer.send(Frame::json(FrameKind::Exec, exec)?).await?;
    }
    Ok(())
}

/// Streams a pack followed by the empty end-of-pack frame. While `watch` is set, also listens for `WELCOME` and
/// cuts the pack short when the host needs a full one.
async fn send_pack<S: AsyncRead + AsyncWrite>(
    writer: &mut Writer<S>,
    frames: &mut Reader<S>,
    snapshot: &Snapshot,
    base: Option<&str>,
    mut watch: bool,
) -> Result<Option<Welcome>, Error> {
    let mut welcome = None;
    if let Some(mut pack) = snapshot.pack(base).await? {
        let mut buf = BytesMut::with_capacity(CHUNK);
        let complete = loop {
            buf.reserve(CHUNK);
            tokio::select! {
                read = pack.stdout.read_buf(&mut buf) => match read? {
                    0 => break true,
                    _ => writer.send(Frame::new(FrameKind::Pack, buf.split().freeze())).await?,
                },
                reply = next(frames), if watch => {
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
    writer.send(Frame::empty(FrameKind::Pack)).await?;
    Ok(welcome)
}

/// Receives output, forwards signals, and sends input concurrently, so neither a slow local terminal nor a command
/// that ignores its input keeps a second Ctrl-C from abandoning the run.
async fn relay<S, O, E>(
    writer: &mut Writer<S>,
    frames: &mut Reader<S>,
    snapshot: &Snapshot,
    synced: &mut bool,
    io: Io<O, E>,
) -> Result<i32, Error>
where
    S: AsyncRead + AsyncWrite,
    O: AsyncWrite + Unpin,
    E: AsyncWrite + Unpin,
{
    let Io {
        mut stdin,
        mut signals,
        mut stdout,
        mut stderr,
    } = io;
    let (consumed_tx, mut consumed) = watch::channel(0);
    let (control_tx, mut control) = mpsc::unbounded_channel();

    let receive = async {
        loop {
            let frame = next(frames).await?;
            match frame.kind {
                FrameKind::Synced => {
                    *synced = true;
                    snapshot.synced().await?;
                }
                FrameKind::Out => {
                    stdout.write_all(&frame.payload).await?;
                    stdout.flush().await?;
                }
                FrameKind::Err => {
                    stderr.write_all(&frame.payload).await?;
                    stderr.flush().await?;
                }
                FrameKind::Consumed => {
                    consumed_tx.send_replace(frame.parse::<Consumed>()?.bytes);
                }
                FrameKind::Exit => return Ok(frame.parse::<Exit>()?.code),
                kind => return Err(ovr_proto::Error::Unexpected(kind).into()),
            }
        }
    };
    let forward_signals = async {
        let mut interrupted = false;
        while let Some(signal) = signals.recv().await {
            let frame = match signal {
                Signal::Resize { cols, rows } => {
                    Frame::json(FrameKind::Resize, &Resize { cols, rows })?
                }
                Signal::Interrupt if interrupted => return Ok(INTERRUPTED),
                Signal::Interrupt => {
                    interrupted = true;
                    Frame::empty(FrameKind::Interrupt)
                }
            };
            let _ = control_tx.send(frame);
        }
        std::future::pending().await
    };
    let send = async {
        let mut sent = 0;
        let mut stdin_open = true;
        loop {
            // The agent holds what the command has not consumed yet; this bounds how much.
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
        // Nothing more can be sent; the agent's last frames, or the lost connection, end the run.
        std::future::pending::<Infallible>().await
    };
    tokio::select! {
        result = receive => result,
        result = forward_signals => result,
        never = send => match never {},
    }
}

/// The next frame from the agent, turning `ERROR` frames and a dropped connection into errors.
async fn next<S: AsyncRead>(frames: &mut Reader<S>) -> Result<Frame, Error> {
    let frame = frames.next().await.ok_or(Error::Disconnected)??;
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
        let snapshot = Snapshot::take(&repo.join(cwd), "box").await?;
        let exec = Exec {
            argv: vec!["sh".to_owned(), "-c".to_owned(), script.to_owned()],
            cwd: snapshot.prefix.clone(),
            tty: false,
            cols: 80,
            rows: 24,
        };
        let config = Config {
            root: host_root.to_owned(),
            shell: Shell::new("/bin/sh"),
        };
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
        let io = Io {
            stdin: stdin_rx,
            signals,
            stdout: &mut stdout,
            stderr: &mut stderr,
        };
        let code = run(client, &snapshot, &exec, io).await?;
        agent.await.map_err(|_| Error::Disconnected)?;
        Ok(Run {
            code,
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        })
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
            first.stdout.ends_with(&format!("{name}/src\nv1\n")),
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
        let snapshot = Snapshot::take(repo.path(), "box").await?;
        assert_eq!(snapshot.base, None);
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
    async fn interrupts_reach_the_command_and_a_second_abandons_the_run() -> Result<(), Error> {
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
        assert!(!marker.exists(), "an abandoned run kills its command");
        Ok(())
    }
}
