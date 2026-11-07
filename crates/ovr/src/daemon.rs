//! Talking to the local `ovrd`, starting it when needed.

use std::path::PathBuf;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use ovr_proto::frame::{self, Resumed};
use ovr_proto::ipc::{self, ClientStream};
use ovr_proto::msg::{Op, Request, Response};
use ovr_proto::{Frame, FrameKind, VERSION};

use crate::Error;

/// Sends `op` to `ovrd` and returns its response with the connection, which an `Open` turns into the run's stream.
///
/// A running `ovrd` of another version answers `Restart` and exits; the request is then retried against a fresh
/// one.
///
/// # Errors
///
/// Returns an error when `ovrd` cannot be started or reached.
pub async fn request(op: Op) -> Result<(Resumed<ClientStream>, Response), Error> {
    let program = ovrd_path();
    let request = Request {
        version: VERSION.to_owned(),
        op,
    };
    let request = Frame::json(FrameKind::Request, &request)?;
    let mut last = Error::Disconnected;
    for _ in 0..3 {
        let stream = ipc::connect_or_spawn(&program)
            .await
            .map_err(|source| Error::Daemon {
                source,
                help: daemon_help(),
            })?;
        let mut stream = frame::framed(stream);
        stream.send(request.clone()).await?;
        match stream.next().await {
            Some(Ok(frame)) => match frame.expect::<Response>(FrameKind::Response)? {
                Response::Restart => {}
                response => return Ok((frame::resume(stream), response)),
            },
            None => last = Error::Disconnected,
            Some(Err(err)) => last = err.into(),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(last)
}

/// `ovrd` next to this executable, as every installer places it, or else from `PATH`.
fn ovrd_path() -> PathBuf {
    let name = format!("ovrd{}", std::env::consts::EXE_SUFFIX);
    std::env::current_exe()
        .ok()
        .map(|exe| exe.with_file_name(&name))
        .filter(|path| path.is_file())
        .unwrap_or_else(|| PathBuf::from(name))
}

fn daemon_help() -> String {
    let log = ovr_proto::paths::log_dir().map_or_else(
        |_| "ovrd.log".to_owned(),
        |dir| dir.join("ovrd.log").display().to_string(),
    );
    format!("is ovrd installed next to ovr? {log} has details")
}
