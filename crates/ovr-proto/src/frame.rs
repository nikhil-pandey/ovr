//! Framing: `len:u32le` covering the rest, then `kind:u8`, then the payload.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadHalf, WriteHalf};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::Error;

/// Largest payload a data frame carries. Readers split data into chunks of this size.
pub const CHUNK: usize = 64 * 1024;

/// Largest payload any frame may carry; bounds memory for control frames such as a long `EXEC` argv.
pub const MAX_LEN: usize = 1024 * 1024;

/// A client sends input only while less than this is unconsumed, as reported by `CONSUMED`, so the agent holds at
/// most this plus one frame on behalf of a command that is not reading, and can always keep reading frames.
pub const INPUT_WINDOW: u64 = 1024 * 1024;

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
    Attach = 7,
    Kill = 8,
    Runs = 9,

    // Agent to client.
    Welcome = 16,
    Synced = 17,
    Out = 18,
    Err = 19,
    Exit = 20,
    Error = 21,
    Consumed = 22,
    Attached = 23,
    RunList = 24,

    // `ovr` to the local `ovrd`.
    Request = 32,
    Response = 33,

    // Host to initiator `ovrd`, before multiplexing starts.
    Banner = 48,
    Accept = 49,
    Pair = 50,
    PairOk = 51,
}

impl TryFrom<u8> for FrameKind {
    type Error = Error;

    fn try_from(byte: u8) -> Result<Self, Error> {
        use FrameKind::{
            Accept, Attach, Attached, Banner, Consumed, Err, Error as Fail, Exec, Exit, Hello, In,
            Interrupt, Kill, Out, Pack, Pair, PairOk, Request, Resize, Response, RunList, Runs,
            Synced, Welcome,
        };
        Ok(match byte {
            1 => Hello,
            2 => Exec,
            3 => Resize,
            4 => Interrupt,
            5 => Pack,
            6 => In,
            7 => Attach,
            8 => Kill,
            9 => Runs,
            16 => Welcome,
            17 => Synced,
            18 => Out,
            19 => Err,
            20 => Exit,
            21 => Fail,
            22 => Consumed,
            23 => Attached,
            24 => RunList,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub kind: FrameKind,
    pub payload: Bytes,
}

impl Frame {
    #[must_use]
    pub fn new(kind: FrameKind, payload: impl Into<Bytes>) -> Self {
        Self {
            kind,
            payload: payload.into(),
        }
    }

    #[must_use]
    pub fn empty(kind: FrameKind) -> Self {
        Self::new(kind, Bytes::new())
    }

    /// A control frame carrying `message` as JSON.
    ///
    /// # Errors
    ///
    /// Returns an error when `message` cannot be serialized.
    pub fn json<T: Serialize>(kind: FrameKind, message: &T) -> Result<Self, Error> {
        Ok(Self::new(kind, serde_json::to_vec(message)?))
    }

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

/// Encodes and decodes [`Frame`]s for `tokio_util::codec`.
#[derive(Debug)]
pub struct Codec(LengthDelimitedCodec);

impl Default for Codec {
    fn default() -> Self {
        Self(
            LengthDelimitedCodec::builder()
                .little_endian()
                .length_field_type::<u32>()
                .max_frame_length(MAX_LEN + 1)
                .new_codec(),
        )
    }
}

impl Decoder for Codec {
    type Item = Frame;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, Error> {
        let Some(mut body) = self.0.decode(src)? else {
            return Ok(None);
        };
        if !body.has_remaining() {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "empty frame").into());
        }
        let kind = FrameKind::try_from(body.get_u8())?;
        Ok(Some(Frame {
            kind,
            payload: body.freeze(),
        }))
    }
}

impl Encoder<Frame> for Codec {
    type Error = Error;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), Error> {
        let len = frame.payload.len();
        if len > MAX_LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("frame of {len} bytes exceeds the {MAX_LEN}-byte limit"),
            )
            .into());
        }
        dst.reserve(5 + len);
        #[allow(clippy::cast_possible_truncation)] // bounded by MAX_LEN
        dst.put_u32_le(len as u32 + 1);
        dst.put_u8(frame.kind as u8);
        dst.extend_from_slice(&frame.payload);
        Ok(())
    }
}

/// The receiving half of a framed stream. Reading the next frame is cancel-safe.
pub type Reader<S> = FramedRead<ReadHalf<S>, Codec>;

/// The sending half of a framed stream.
pub type Writer<S> = FramedWrite<WriteHalf<S>, Codec>;

/// Splits `stream` into frame halves.
pub fn split<S: AsyncRead + AsyncWrite>(stream: S) -> (Reader<S>, Writer<S>) {
    let (read, write) = tokio::io::split(stream);
    (
        FramedRead::new(read, Codec::default()),
        FramedWrite::new(write, Codec::default()),
    )
}

/// Frames exchanged on a stream before it carries another protocol, such as a handshake before multiplexing.
pub type Framed<S> = tokio_util::codec::Framed<S, Codec>;

#[must_use]
pub fn framed<S: AsyncRead + AsyncWrite>(stream: S) -> Framed<S> {
    Framed::new(stream, Codec::default())
}

/// A stream after its frame exchange; reads start with whatever the exchange had already buffered.
pub type Resumed<S> =
    tokio::io::Join<tokio::io::Chain<std::io::Cursor<Bytes>, ReadHalf<S>>, WriteHalf<S>>;

/// Hands the raw stream back after a frame exchange, without losing bytes the peer sent right behind it.
#[must_use]
pub fn resume<S: AsyncRead + AsyncWrite>(framed: Framed<S>) -> Resumed<S> {
    let parts = framed.into_parts();
    let (read, write) = tokio::io::split(parts.io);
    let buffered = std::io::Cursor::new(parts.read_buf.freeze());
    tokio::io::join(AsyncReadExt::chain(buffered, read), write)
}

#[cfg(test)]
mod tests {
    use futures::{SinkExt, StreamExt};
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::msg::Exit;

    fn decode_all(bytes: &[u8]) -> Vec<Result<Frame, Error>> {
        let mut codec = Codec::default();
        let mut buf = BytesMut::from(bytes);
        let mut frames = Vec::new();
        loop {
            match codec.decode_eof(&mut buf) {
                Ok(Some(frame)) => frames.push(Ok(frame)),
                Ok(None) => return frames,
                Err(err) => {
                    frames.push(Err(err));
                    return frames;
                }
            }
        }
    }

    #[test]
    fn round_trips_frames() -> Result<(), Error> {
        let mut codec = Codec::default();
        let mut buf = BytesMut::new();
        codec.encode(Frame::json(FrameKind::Exit, &Exit { code: -3 })?, &mut buf)?;
        codec.encode(Frame::new(FrameKind::Out, &b"hello"[..]), &mut buf)?;
        codec.encode(Frame::empty(FrameKind::Pack), &mut buf)?;

        let frames: Vec<Frame> = decode_all(&buf).into_iter().collect::<Result<_, _>>()?;
        assert_eq!(frames[0].expect::<Exit>(FrameKind::Exit)?.code, -3);
        assert_eq!(frames[1], Frame::new(FrameKind::Out, &b"hello"[..]));
        assert_eq!(frames[2], Frame::empty(FrameKind::Pack));
        assert_eq!(frames.len(), 3);
        Ok(())
    }

    #[test]
    fn rejects_oversized_unknown_empty_and_truncated_frames() {
        let too_long = u32::try_from(MAX_LEN + 2).unwrap_or(u32::MAX).to_le_bytes();
        for bytes in [
            &too_long[..],
            &[1, 0, 0, 0, 99],
            &[0, 0, 0, 0],
            &[5, 0, 0, 0, 18, b'x'],
        ] {
            assert!(
                matches!(decode_all(bytes).last(), Some(Err(_))),
                "{bytes:?} must not decode"
            );
        }
        assert!(matches!(
            decode_all(&[1, 0, 0, 0, 99]).last(),
            Some(Err(Error::UnknownFrame(99)))
        ));
        let oversized = Frame::new(FrameKind::Out, vec![0; MAX_LEN + 1]);
        assert!(
            Codec::default()
                .encode(oversized, &mut BytesMut::new())
                .is_err()
        );
    }

    #[tokio::test]
    async fn resume_keeps_bytes_sent_behind_a_handshake() -> Result<(), Error> {
        let (initiator, host) = tokio::io::duplex(1024);
        let mut initiator = framed(initiator);
        initiator.send(Frame::empty(FrameKind::Accept)).await?;
        let mut initiator = resume(initiator);
        initiator.write_all(b"multiplexed").await?;
        initiator.shutdown().await?;

        let mut host = framed(host);
        let accept = host.next().await.transpose()?;
        assert_eq!(accept.map(|frame| frame.kind), Some(FrameKind::Accept));
        let mut rest = Vec::new();
        resume(host).read_to_end(&mut rest).await?;
        assert_eq!(rest, b"multiplexed");
        Ok(())
    }
}
