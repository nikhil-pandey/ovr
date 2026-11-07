//! Talking to the local `ovrd`, starting it when needed.

use std::path::PathBuf;
use std::time::Duration;

use ovr_proto::ipc::{self, ClientStream};
use ovr_proto::msg::{Op, Request, Response};
use ovr_proto::{FrameKind, VERSION, frame};

use crate::Error;

/// Sends `op` to `ovrd` and returns its response with the connection, which an `Open` turns into the run's stream.
///
/// A running `ovrd` of another version answers `Restart` and exits; the request is then retried against a fresh
/// one.
///
/// # Errors
///
/// Returns an error when `ovrd` cannot be started or reached.
pub async fn request(op: Op) -> Result<(ClientStream, Response), Error> {
    let program = ovrd_path();
    let request = Request {
        version: VERSION.to_owned(),
        op,
    };
    let mut last = Error::Disconnected;
    for _ in 0..3 {
        let mut stream = ipc::connect_or_spawn(&program)
            .await
            .map_err(Error::Daemon)?;
        frame::send(&mut stream, FrameKind::Request, &request).await?;
        match frame::read(&mut stream).await {
            Ok(Some(frame)) => match frame.expect::<Response>(FrameKind::Response)? {
                Response::Restart => {}
                response => return Ok((stream, response)),
            },
            Ok(None) => last = Error::Disconnected,
            Err(err) => last = err.into(),
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
