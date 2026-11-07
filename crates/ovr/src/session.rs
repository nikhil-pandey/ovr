//! The client half of a run: sync the snapshot, start the command, and relay terminal I/O until it exits.

use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use ovr_proto::frame::{self, CHUNK, Reader, Writer};
use ovr_proto::msg::{Exec, Exit, Failure, Resize, Welcome};
use ovr_proto::{Frame, FrameKind};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::Error;
use crate::snapshot::Snapshot;

/// Exit code reported when a second interrupt abandons the run, as a shell does for `SIGINT`.
pub const INTERRUPTED: i32 = 130;

/// Local terminal events forwarded to the remote command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    Data(bytes::Bytes),
    /// Local stdin ended.
    Eof,
    Resize {
        cols: u16,
        rows: u16,
    },
    /// Ctrl-C outside a terminal; inside one it arrives as input.
    Interrupt,
}

/// Where a run's input comes from and its output goes.
pub struct Io<O, E> {
    pub input: mpsc::Receiver<Input>,
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

async fn relay<S, O, E>(
    writer: &mut Writer<S>,
    frames: &mut Reader<S>,
    snapshot: &Snapshot,
    synced: &mut bool,
    mut io: Io<O, E>,
) -> Result<i32, Error>
where
    S: AsyncRead + AsyncWrite,
    O: AsyncWrite + Unpin,
    E: AsyncWrite + Unpin,
{
    let mut input_open = true;
    let mut interrupted = false;
    loop {
        tokio::select! {
            frame = next(frames) => {
                let frame = frame?;
                match frame.kind {
                    FrameKind::Synced => {
                        *synced = true;
                        snapshot.synced().await?;
                    }
                    FrameKind::Out => {
                        io.stdout.write_all(&frame.payload).await?;
                        io.stdout.flush().await?;
                    }
                    FrameKind::Err => {
                        io.stderr.write_all(&frame.payload).await?;
                        io.stderr.flush().await?;
                    }
                    FrameKind::Exit => return Ok(frame.parse::<Exit>()?.code),
                    kind => return Err(ovr_proto::Error::Unexpected(kind).into()),
                }
            }
            input = io.input.recv(), if input_open => {
                let frame = match input {
                    Some(Input::Data(data)) => Frame::new(FrameKind::In, data),
                    Some(Input::Resize { cols, rows }) => Frame::json(FrameKind::Resize, &Resize { cols, rows })?,
                    Some(Input::Interrupt) if interrupted => return Ok(INTERRUPTED),
                    Some(Input::Interrupt) => {
                        interrupted = true;
                        Frame::empty(FrameKind::Interrupt)
                    }
                    // Interrupts may still follow the end of stdin.
                    Some(Input::Eof) => Frame::empty(FrameKind::In),
                    None => {
                        input_open = false;
                        Frame::empty(FrameKind::In)
                    }
                };
                writer.send(frame).await?;
            },
        }
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

        let (input_tx, input) = mpsc::channel(4);
        if !stdin.is_empty() {
            input_tx
                .send(Input::Data(stdin.to_vec().into()))
                .await
                .map_err(|_| Error::Disconnected)?;
        }
        drop(input_tx);
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let io = Io {
            input,
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
}
