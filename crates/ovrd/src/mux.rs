//! Multiplexes independent byte streams ("channels") over one host connection, so that any number of runs share a
//! single SSH session or dial-in TLS connection.
//!
//! Frames are `channel:u32le, kind:u8, len:u32le, payload`. Each channel is exposed as a [`DuplexStream`]; shutting
//! down its write half sends `CLOSE`, which the peer sees as end-of-stream. Every channel has its own credit window,
//! so a slow reader on one channel never stalls the others or the connection. Both ends ping; a connection that has
//! been silent for [`IDLE_TIMEOUT`] is closed.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter, DuplexStream};
use tokio::sync::{Semaphore, mpsc, watch};

/// Largest payload of a `DATA` frame.
const CHUNK: usize = 64 * 1024;

/// Bytes a sender may have in flight on one channel before the receiver's consumer catches up.
const WINDOW: u32 = 4 * 1024 * 1024;

const HEADER: usize = 9;
const PING_INTERVAL: Duration = Duration::from_secs(15);
const IDLE_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Kind {
    Open = 1,
    Data = 2,
    Close = 3,
    Window = 4,
    Ping = 5,
    Pong = 6,
}

impl Kind {
    fn from_u8(byte: u8) -> Option<Self> {
        Some(match byte {
            1 => Self::Open,
            2 => Self::Data,
            3 => Self::Close,
            4 => Self::Window,
            5 => Self::Ping,
            6 => Self::Pong,
            _ => return None,
        })
    }
}

/// Which side of the connection this is. Only the opener opens channels; the roles also keep channel ids disjoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The laptop's `ovrd`.
    Opener,
    /// A host's agent.
    Acceptor,
}

struct Outgoing {
    channel: u32,
    kind: Kind,
    payload: Vec<u8>,
}

struct Channel {
    /// Data from the peer, waiting to be written to the local stream. Dropped when the peer closes.
    deliver: Option<mpsc::UnboundedSender<Vec<u8>>>,
    /// Bytes we may still send to the peer.
    credit: Arc<Semaphore>,
    local_closed: bool,
}

struct Shared {
    out: mpsc::Sender<Outgoing>,
    channels: Mutex<HashMap<u32, Channel>>,
    next_id: AtomicU32,
    closed: watch::Sender<bool>,
}

/// A handle to a multiplexed connection. Cloning shares the connection.
#[derive(Clone)]
pub struct Mux(Arc<Shared>);

/// Channels opened by the peer.
pub struct Incoming(mpsc::Receiver<DuplexStream>);

impl Incoming {
    /// The next channel opened by the peer, or `None` once the connection is closed.
    pub async fn accept(&mut self) -> Option<DuplexStream> {
        self.0.recv().await
    }
}

impl Mux {
    /// Starts multiplexing over `stream`. The connection runs on background tasks until either side closes it.
    pub fn new<S>(stream: S, role: Role) -> (Self, Incoming)
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (out_tx, out_rx) = mpsc::channel(64);
        let (incoming_tx, incoming_rx) = mpsc::channel(16);
        let shared = Arc::new(Shared {
            out: out_tx,
            channels: Mutex::default(),
            next_id: AtomicU32::new(match role {
                Role::Opener => 1,
                Role::Acceptor => 2,
            }),
            closed: watch::Sender::new(false),
        });
        let (reader, writer) = tokio::io::split(stream);
        tokio::spawn(write_loop(Arc::clone(&shared), writer, out_rx));
        tokio::spawn(read_loop(Arc::clone(&shared), reader, incoming_tx));
        tokio::spawn(ping_loop(Arc::clone(&shared)));
        (Self(shared), Incoming(incoming_rx))
    }

    /// Opens a channel to the peer.
    ///
    /// # Errors
    ///
    /// Returns an error when the connection is closed.
    pub async fn open(&self) -> io::Result<DuplexStream> {
        if self.is_closed() {
            return Err(io::ErrorKind::ConnectionAborted.into());
        }
        let id = self.0.next_id.fetch_add(2, Ordering::Relaxed);
        let stream = attach(&self.0, id);
        self.0.send(id, Kind::Open, Vec::new()).await?;
        Ok(stream)
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        *self.0.closed.borrow()
    }

    /// Resolves once the connection is closed.
    pub async fn closed(&self) {
        let mut closed = self.0.closed.subscribe();
        let _ = closed.wait_for(|closed| *closed).await;
    }

    /// Closes the connection. Open channels see end-of-stream.
    pub fn close(&self) {
        self.0.shutdown();
    }
}

impl Shared {
    async fn send(&self, channel: u32, kind: Kind, payload: Vec<u8>) -> io::Result<()> {
        self.out
            .send(Outgoing {
                channel,
                kind,
                payload,
            })
            .await
            .map_err(|_| io::ErrorKind::ConnectionAborted.into())
    }

    fn channels(&self) -> MutexGuard<'_, HashMap<u32, Channel>> {
        self.channels.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn shutdown(&self) {
        self.closed.send_replace(true);
        let channels = std::mem::take(&mut *self.channels());
        for channel in channels.values() {
            channel.credit.close();
        }
    }

    fn local_closed(&self, id: u32) {
        let mut channels = self.channels();
        if let Some(channel) = channels.get_mut(&id) {
            channel.local_closed = true;
            if channel.deliver.is_none() {
                channels.remove(&id);
            }
        }
    }

    fn remote_closed(&self, id: u32) {
        let mut channels = self.channels();
        if let Some(channel) = channels.get_mut(&id) {
            channel.deliver = None;
            if channel.local_closed {
                channels.remove(&id);
            }
        }
    }
}

/// Registers channel `id` and starts the tasks that move its bytes; returns the caller's end.
fn attach(shared: &Arc<Shared>, id: u32) -> DuplexStream {
    let (user, ours) = tokio::io::duplex(2 * CHUNK);
    let (mut from_user, mut to_user) = tokio::io::split(ours);
    let credit = Arc::new(Semaphore::new(WINDOW as usize));
    let (deliver_tx, mut deliver_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    shared.channels().insert(
        id,
        Channel {
            deliver: Some(deliver_tx),
            credit: Arc::clone(&credit),
            local_closed: false,
        },
    );

    let pump = Arc::clone(shared);
    tokio::spawn(async move {
        let mut buf = vec![0; CHUNK];
        loop {
            let n = match from_user.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            #[allow(clippy::cast_possible_truncation)] // n <= CHUNK
            let Ok(permit) = credit.acquire_many(n as u32).await else {
                return;
            };
            permit.forget();
            if pump.send(id, Kind::Data, buf[..n].to_vec()).await.is_err() {
                return;
            }
        }
        let _ = pump.send(id, Kind::Close, Vec::new()).await;
        pump.local_closed(id);
    });

    let deliver = Arc::clone(shared);
    tokio::spawn(async move {
        let mut open = true;
        while let Some(data) = deliver_rx.recv().await {
            // A consumer that went away still returns credit, so the peer never stalls on a dead channel.
            open = open && to_user.write_all(&data).await.is_ok();
            #[allow(clippy::cast_possible_truncation)] // data.len() <= CHUNK
            let window = (data.len() as u32).to_le_bytes().to_vec();
            if deliver.send(id, Kind::Window, window).await.is_err() {
                break;
            }
        }
        let _ = to_user.shutdown().await;
    });

    user
}

async fn read_loop<R>(shared: Arc<Shared>, mut reader: R, incoming: mpsc::Sender<DuplexStream>)
where
    R: AsyncRead + Unpin,
{
    let mut closed = shared.closed.subscribe();
    loop {
        let frame = tokio::select! {
            frame = tokio::time::timeout(IDLE_TIMEOUT, read_frame(&mut reader)) => frame,
            _ = closed.wait_for(|closed| *closed) => break,
        };
        let (id, kind, payload) = match frame {
            Ok(Ok(Some(frame))) => frame,
            Ok(Ok(None)) => break,
            Ok(Err(err)) => {
                tracing::debug!(%err, "connection failed");
                break;
            }
            Err(_) => {
                tracing::warn!(
                    "connection silent for {}s; closing it",
                    IDLE_TIMEOUT.as_secs()
                );
                break;
            }
        };
        // The reader never awaits the writer, so a full outgoing queue cannot deadlock two peers.
        match kind {
            Kind::Open => {
                let stream = attach(&shared, id);
                if incoming.try_send(stream).is_err() {
                    shared.remote_closed(id);
                    let _ = shared.out.try_send(Outgoing {
                        channel: id,
                        kind: Kind::Close,
                        payload: Vec::new(),
                    });
                }
            }
            Kind::Data => {
                if let Some(deliver) = shared.channels().get(&id).and_then(|c| c.deliver.as_ref()) {
                    let _ = deliver.send(payload);
                }
            }
            Kind::Close => shared.remote_closed(id),
            Kind::Window => {
                let grant = <[u8; 4]>::try_from(payload.as_slice()).map(u32::from_le_bytes);
                if let (Ok(grant), Some(channel)) = (grant, shared.channels().get(&id)) {
                    channel.credit.add_permits(grant as usize);
                }
            }
            Kind::Ping => {
                let _ = shared.out.try_send(Outgoing {
                    channel: 0,
                    kind: Kind::Pong,
                    payload: Vec::new(),
                });
            }
            Kind::Pong => {}
        }
    }
    shared.shutdown();
}

async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> io::Result<Option<(u32, Kind, Vec<u8>)>> {
    let mut header = [0; HEADER];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }
    let [c0, c1, c2, c3, kind, l0, l1, l2, l3] = header;
    let id = u32::from_le_bytes([c0, c1, c2, c3]);
    let len = u32::from_le_bytes([l0, l1, l2, l3]) as usize;
    let kind = Kind::from_u8(kind)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unknown mux frame"))?;
    if len > CHUNK {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "oversized mux frame",
        ));
    }
    let mut payload = vec![0; len];
    reader.read_exact(&mut payload).await?;
    Ok(Some((id, kind, payload)))
}

async fn write_loop<W>(shared: Arc<Shared>, writer: W, mut out: mpsc::Receiver<Outgoing>)
where
    W: AsyncWrite + Unpin,
{
    let mut writer = BufWriter::with_capacity(4 * CHUNK, writer);
    let mut closed = shared.closed.subscribe();
    loop {
        let first = tokio::select! {
            next = out.recv() => next,
            _ = closed.wait_for(|closed| *closed) => None,
        };
        let Some(first) = first else { break };
        let mut result = write_one(&mut writer, first).await;
        // Coalesce whatever else is queued into the same flush.
        while result.is_ok() {
            match out.try_recv() {
                Ok(next) => result = write_one(&mut writer, next).await,
                Err(_) => break,
            }
        }
        if let Err(err) = result.and(writer.flush().await) {
            tracing::debug!(%err, "connection write failed");
            break;
        }
    }
    shared.shutdown();
    let _ = writer.shutdown().await;
}

async fn write_one<W: AsyncWrite + Unpin>(writer: &mut W, frame: Outgoing) -> io::Result<()> {
    #[allow(clippy::cast_possible_truncation)] // payloads are at most CHUNK
    let len = frame.payload.len() as u32;
    let mut header = [0; HEADER];
    header[..4].copy_from_slice(&frame.channel.to_le_bytes());
    header[4] = frame.kind as u8;
    header[5..].copy_from_slice(&len.to_le_bytes());
    writer.write_all(&header).await?;
    writer.write_all(&frame.payload).await
}

async fn ping_loop(shared: Arc<Shared>) {
    let mut closed = shared.closed.subscribe();
    let mut interval = tokio::time::interval(PING_INTERVAL);
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = closed.wait_for(|closed| *closed) => return,
        }
        if shared.send(0, Kind::Ping, Vec::new()).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn echo_server(mut incoming: Incoming) {
        while let Some(stream) = incoming.accept().await {
            tokio::spawn(async move {
                let (mut rd, mut wr) = tokio::io::split(stream);
                let _ = tokio::io::copy(&mut rd, &mut wr).await;
                let _ = wr.shutdown().await;
            });
        }
    }

    async fn round_trip(mux: Mux, data: Vec<u8>) -> io::Result<Vec<u8>> {
        let stream = mux.open().await?;
        let (mut rd, mut wr) = tokio::io::split(stream);
        let writer = tokio::spawn(async move {
            wr.write_all(&data).await?;
            wr.shutdown().await
        });
        let mut echoed = Vec::new();
        rd.read_to_end(&mut echoed).await?;
        writer.await.map_err(io::Error::other)??;
        Ok(echoed)
    }

    #[tokio::test]
    async fn channels_are_independent_and_close_cleanly() -> io::Result<()> {
        let (a, b) = tokio::io::duplex(16 * 1024);
        let (laptop, _) = Mux::new(a, Role::Opener);
        let (_host, incoming) = Mux::new(b, Role::Acceptor);
        tokio::spawn(echo_server(incoming));

        // Several times the window, so the transfer only completes if credit flows back.
        let big: Vec<u8> = (0..=250).cycle().take(3 * WINDOW as usize).collect();
        let (big_echo, small_echo) = tokio::join!(
            round_trip(laptop.clone(), big.clone()),
            round_trip(laptop.clone(), b"ping".to_vec())
        );
        assert_eq!(small_echo?, b"ping");
        assert_eq!(big_echo?, big);
        Ok(())
    }

    #[tokio::test]
    async fn dropping_the_transport_ends_open_channels() -> io::Result<()> {
        let (a, b) = tokio::io::duplex(1024);
        let (laptop, _) = Mux::new(a, Role::Opener);
        let (host, mut incoming) = Mux::new(b, Role::Acceptor);
        let mut stream = laptop.open().await?;
        let accepted = incoming.accept().await;
        assert!(accepted.is_some());

        host.close();
        laptop.closed().await;
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).await?;
        assert_eq!(rest.len(), 0);
        assert!(laptop.open().await.is_err());
        Ok(())
    }
}
