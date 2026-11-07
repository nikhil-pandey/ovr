//! Framing: a five-byte [`Header`] (`kind:u8, len:u32le`) followed by `len` payload bytes.

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::Error;

/// Largest payload a data frame carries. Readers split data into chunks of this size.
pub const CHUNK: usize = 64 * 1024;

/// Largest payload any frame may carry; bounds memory for control frames such as a long `EXEC` argv.
pub const MAX_LEN: u32 = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    // Client to agent.
    Hello = 1,
    Exec = 2,
    Resize = 3,
    Interrupt = 4,
    Pack = 5,
    In = 6,

    // Agent to client.
    Welcome = 16,
    Synced = 17,
    Out = 18,
    Err = 19,
    Exit = 20,
    Error = 21,

    // `ovr` to the local `ovrd`.
    Request = 32,
    Response = 33,

    // Host to laptop `ovrd`, before multiplexing starts.
    Banner = 48,
    Accept = 49,
    Pair = 50,
    PairOk = 51,
}

impl TryFrom<u8> for FrameKind {
    type Error = Error;

    fn try_from(byte: u8) -> Result<Self, Error> {
        use FrameKind::{
            Accept, Banner, Err, Error as Fail, Exec, Exit, Hello, In, Interrupt, Out, Pack, Pair,
            PairOk, Request, Resize, Response, Synced, Welcome,
        };
        Ok(match byte {
            1 => Hello,
            2 => Exec,
            3 => Resize,
            4 => Interrupt,
            5 => Pack,
            6 => In,
            16 => Welcome,
            17 => Synced,
            18 => Out,
            19 => Err,
            20 => Exit,
            21 => Fail,
            32 => Request,
            33 => Response,
            48 => Banner,
            49 => Accept,
            50 => Pair,
            51 => PairOk,
            other => return Result::Err(Error::UnknownFrame(other)),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub kind: FrameKind,
    pub len: u32,
}

impl Header {
    pub const SIZE: usize = 5;

    #[must_use]
    pub fn encode(self) -> [u8; Self::SIZE] {
        let [a, b, c, d] = self.len.to_le_bytes();
        [self.kind as u8, a, b, c, d]
    }

    /// # Errors
    ///
    /// Returns an error for an unknown frame kind or a payload longer than [`MAX_LEN`].
    pub fn decode(bytes: [u8; Self::SIZE]) -> Result<Self, Error> {
        let [kind, a, b, c, d] = bytes;
        let len = u32::from_le_bytes([a, b, c, d]);
        if len > MAX_LEN {
            return Err(Error::TooLarge(len));
        }
        Ok(Self {
            kind: FrameKind::try_from(kind)?,
            len,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub kind: FrameKind,
    pub payload: Vec<u8>,
}

impl Frame {
    /// Decodes the JSON payload of a control frame.
    ///
    /// # Errors
    ///
    /// Returns an error when the payload is not the expected message.
    pub fn parse<T: DeserializeOwned>(&self) -> Result<T, Error> {
        Ok(serde_json::from_slice(&self.payload)?)
    }

    /// Like [`Frame::parse`], but first checks that the frame is of `kind`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Unexpected`] for any other kind, or an error for a malformed payload.
    pub fn expect<T: DeserializeOwned>(&self, kind: FrameKind) -> Result<T, Error> {
        if self.kind != kind {
            return Err(Error::Unexpected(self.kind));
        }
        self.parse()
    }
}

/// Reads one frame, or `None` when the stream ends cleanly between frames.
///
/// Not cancel-safe: a partially read frame is lost if the future is dropped. Read from a dedicated task when the
/// caller needs to `select!`.
///
/// # Errors
///
/// Returns an error on I/O failure, a truncated frame, or an invalid header.
pub async fn read<R: AsyncRead + Unpin + ?Sized>(reader: &mut R) -> Result<Option<Frame>, Error> {
    let mut header = [0; Header::SIZE];
    let mut filled = 0;
    while filled < header.len() {
        match reader.read(&mut header[filled..]).await? {
            0 if filled == 0 => return Ok(None),
            0 => return Err(Error::Io(std::io::ErrorKind::UnexpectedEof.into())),
            n => filled += n,
        }
    }
    let header = Header::decode(header)?;
    let mut payload = vec![0; header.len as usize];
    reader.read_exact(&mut payload).await?;
    Ok(Some(Frame {
        kind: header.kind,
        payload,
    }))
}

/// Writes one frame with a raw payload.
///
/// # Errors
///
/// Returns an error on I/O failure or a payload longer than [`MAX_LEN`].
pub async fn write<W: AsyncWrite + Unpin + ?Sized>(
    writer: &mut W,
    kind: FrameKind,
    payload: &[u8],
) -> std::io::Result<()> {
    let len = u32::try_from(payload.len())
        .ok()
        .filter(|len| *len <= MAX_LEN)
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "frame payload too large")
        })?;
    let mut buf = Vec::with_capacity(Header::SIZE + payload.len());
    buf.extend_from_slice(&Header { kind, len }.encode());
    buf.extend_from_slice(payload);
    writer.write_all(&buf).await
}

/// Writes one control frame carrying `message` as JSON.
///
/// # Errors
///
/// Returns an error on I/O failure.
pub async fn send<W: AsyncWrite + Unpin + ?Sized, T: Serialize>(
    writer: &mut W,
    kind: FrameKind,
    message: &T,
) -> std::io::Result<()> {
    let payload = serde_json::to_vec(message).map_err(std::io::Error::other)?;
    write(writer, kind, &payload).await
}

/// Reads frames on a background task so that callers can `select!` on them safely. The channel ends after the
/// stream ends cleanly; a failure is delivered as the last item.
pub fn spawn_reader<R>(mut reader: R) -> tokio::sync::mpsc::Receiver<Result<Frame, Error>>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    tokio::spawn(async move {
        loop {
            match read(&mut reader).await {
                Ok(Some(frame)) => {
                    if tx.send(Ok(frame)).await.is_err() {
                        return;
                    }
                }
                Ok(None) => return,
                Err(err) => {
                    let _ = tx.send(Err(err)).await;
                    return;
                }
            }
        }
    });
    rx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::Exit;

    #[tokio::test]
    async fn round_trips_frames_and_reports_clean_eof() {
        let mut buf = Vec::new();
        send(&mut buf, FrameKind::Exit, &Exit { code: -3 })
            .await
            .ok();
        write(&mut buf, FrameKind::Out, b"hello").await.ok();
        write(&mut buf, FrameKind::Pack, b"").await.ok();

        let mut reader = buf.as_slice();
        let exit = read(&mut reader).await.ok().flatten();
        assert_eq!(
            exit.as_ref()
                .and_then(|f| f.expect::<Exit>(FrameKind::Exit).ok())
                .map(|e| e.code),
            Some(-3)
        );
        let out = read(&mut reader).await.ok().flatten();
        assert_eq!(
            out.map(|f| (f.kind, f.payload)),
            Some((FrameKind::Out, b"hello".to_vec()))
        );
        let end = read(&mut reader).await.ok().flatten();
        assert_eq!(end.map(|f| f.payload.len()), Some(0));
        assert!(matches!(read(&mut reader).await, Ok(None)));
    }

    #[tokio::test]
    async fn rejects_oversized_unknown_and_truncated_frames() {
        let oversized = Header {
            kind: FrameKind::Out,
            len: MAX_LEN,
        }
        .encode();
        let mut too_large = oversized;
        too_large[1..].copy_from_slice(&(MAX_LEN + 1).to_le_bytes());
        assert!(matches!(
            read(&mut too_large.as_slice()).await,
            Err(Error::TooLarge(_))
        ));
        assert!(matches!(
            read(&mut [99, 0, 0, 0, 0].as_slice()).await,
            Err(Error::UnknownFrame(99))
        ));
        assert!(matches!(
            read(&mut [18, 4, 0].as_slice()).await,
            Err(Error::Io(_))
        ));
        assert!(matches!(
            read(&mut [18, 4, 0, 0, 0, b'x'].as_slice()).await,
            Err(Error::Io(_))
        ));
    }
}
