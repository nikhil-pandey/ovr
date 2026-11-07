//! Multiplexes runs over one host connection with yamux, so that any number of runs share a single SSH session or
//! dial-in TLS connection. The initiator opens a stream per run; the host accepts them.
//!
//! yamux sends no keepalives of its own. SSH connections use `ServerAliveInterval`; dial-in connections, which may
//! cross a tunnel or proxy where TCP keepalive only probes the nearest hop, carry a heartbeat: the accepting side
//! opens one extra channel, both sides write a byte to it every [`HEARTBEAT`], and either side closes the
//! connection when it has read nothing at all for [`DEAD_AFTER`].

use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

/// How often each side of a dial-in connection proves it is there.
pub const HEARTBEAT: Duration = Duration::from_secs(15);

/// How long a dial-in connection may stay silent before it is considered gone.
pub const DEAD_AFTER: Duration = Duration::from_secs(45);

/// One run's byte stream. Shutting down its write half is the peer's end-of-stream.
pub type Channel = Compat<yamux::Stream>;

enum Command {
    Open(oneshot::Sender<yamux::Stream>),
    Close,
}

/// The initiator's handle on a host connection; clones share it. The connection closes when every clone is dropped.
#[derive(Clone)]
pub struct Mux {
    commands: mpsc::UnboundedSender<Command>,
    closed: watch::Receiver<bool>,
}

impl Mux {
    /// Starts multiplexing over `io` as the side that opens channels.
    pub fn open_on<T>(io: T) -> Self
    where
        T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (commands, closed) = start(io, yamux::Mode::Client, None, false);
        Self { commands, closed }
    }

    /// Like [`Mux::open_on`], with heartbeats, for a peer using [`Incoming::accept_with_heartbeat`].
    pub fn open_with_heartbeat<T>(io: T) -> Self
    where
        T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (commands, closed) = start(io, yamux::Mode::Client, None, true);
        Self { commands, closed }
    }

    /// Opens a channel to the peer.
    ///
    /// # Errors
    ///
    /// Returns an error when the connection is closed.
    pub async fn open(&self) -> io::Result<Channel> {
        let (reply, stream) = oneshot::channel();
        self.commands
            .send(Command::Open(reply))
            .map_err(|_| io::ErrorKind::ConnectionAborted)?;
        let stream = stream.await.map_err(|_| io::ErrorKind::ConnectionAborted)?;
        Ok(stream.compat())
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    /// Resolves once the connection is closed.
    pub async fn closed(&self) {
        let mut closed = self.closed.clone();
        let _ = closed.wait_for(|closed| *closed).await;
    }

    /// Closes the connection. Open channels see end-of-stream.
    pub fn close(&self) {
        let _ = self.commands.send(Command::Close);
    }
}

/// Channels the initiator opens on a host connection. Dropping this closes the connection.
pub struct Incoming {
    streams: mpsc::UnboundedReceiver<yamux::Stream>,
    _commands: mpsc::UnboundedSender<Command>,
}

impl Incoming {
    /// Starts multiplexing over `io` as the side that accepts channels.
    pub fn accept_on<T>(io: T) -> Self
    where
        T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        Self::start_accepting(io, false)
    }

    /// Like [`Incoming::accept_on`], with heartbeats, for a peer using [`Mux::open_with_heartbeat`].
    pub fn accept_with_heartbeat<T>(io: T) -> Self
    where
        T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        Self::start_accepting(io, true)
    }

    fn start_accepting<T>(io: T, heartbeat: bool) -> Self
    where
        T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (inbound, streams) = mpsc::unbounded_channel();
        let (commands, _) = start(io, yamux::Mode::Server, Some(inbound), heartbeat);
        Self {
            streams,
            _commands: commands,
        }
    }

    /// The next channel, or `None` once the connection has ended.
    pub async fn accept(&mut self) -> Option<Channel> {
        self.streams
            .recv()
            .await
            .map(FuturesAsyncReadCompatExt::compat)
    }
}

/// Sets up a TCP connection for multiplexing: no Nagle delay, and keepalive probes that end the connection within
/// about a minute of the peer vanishing, e.g. when an initiator sleeps.
///
/// # Errors
///
/// Returns an error when the socket options cannot be set.
pub fn tune(tcp: &TcpStream) -> io::Result<()> {
    tcp.set_nodelay(true)?;
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(15))
        .with_interval(Duration::from_secs(5));
    socket2::SockRef::from(tcp).set_tcp_keepalive(&keepalive)
}

fn start<T>(
    io: T,
    mode: yamux::Mode,
    inbound: Option<mpsc::UnboundedSender<yamux::Stream>>,
    heartbeat: bool,
) -> (mpsc::UnboundedSender<Command>, watch::Receiver<bool>)
where
    T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (commands, requests) = mpsc::unbounded_channel();
    let (done, closed) = watch::channel(false);
    let io = Watched {
        io,
        last_read: Arc::new(Mutex::new(Instant::now())),
    };
    let mut first_inbound = None;
    if heartbeat {
        // The accepting side opens the heartbeat channel, so that the opening side, which accepts nothing else,
        // can tell it apart from the runs it opens.
        let (reply, channel) = oneshot::channel();
        if mode == yamux::Mode::Server {
            let _ = commands.send(Command::Open(reply));
        } else {
            first_inbound = Some(reply);
        }
        tokio::spawn(keep_alive(
            channel,
            Arc::clone(&io.last_read),
            commands.downgrade(),
            closed.clone(),
        ));
    }
    let connection = yamux::Connection::new(io.compat(), yamux::Config::default(), mode);
    tokio::spawn(async move {
        if let Err(err) = drive(connection, requests, inbound, first_inbound).await {
            tracing::debug!(%err, "connection ended");
        }
        done.send_replace(true);
    });
    (commands, closed)
}

/// Beats on the heartbeat channel and closes the connection once nothing has been read for [`DEAD_AFTER`]. Any bytes
/// count, so a connection busy with a large transfer is alive even if a heartbeat queues behind it.
async fn keep_alive(
    channel: oneshot::Receiver<yamux::Stream>,
    last_read: Arc<Mutex<Instant>>,
    commands: mpsc::WeakUnboundedSender<Command>,
    mut closed: watch::Receiver<bool>,
) {
    let beat = async {
        let Ok(channel) = channel.await else { return };
        let (mut reader, mut writer) = tokio::io::split(channel.compat());
        let drain = async {
            let mut buf = [0; 64];
            while reader.read(&mut buf).await.is_ok_and(|read| read > 0) {}
        };
        let send = async {
            let mut ticks = tokio::time::interval(HEARTBEAT);
            loop {
                ticks.tick().await;
                if writer.write_all(&[0]).await.is_err() || writer.flush().await.is_err() {
                    return;
                }
            }
        };
        // The peer closes its end only when the connection is going away.
        tokio::select! {
            () = drain => {}
            () = send => {}
        }
    };
    let watch = async {
        loop {
            let deadline = *last_read.lock().unwrap_or_else(PoisonError::into_inner) + DEAD_AFTER;
            if Instant::now() >= deadline {
                tracing::info!(
                    "no heartbeat for {}s; closing the connection",
                    DEAD_AFTER.as_secs()
                );
                return;
            }
            tokio::time::sleep_until(deadline).await;
        }
    };
    tokio::select! {
        () = beat => {}
        () = watch => {}
        _ = closed.wait_for(|closed| *closed) => return,
    }
    if let Some(commands) = commands.upgrade() {
        let _ = commands.send(Command::Close);
    }
}

/// The transport, noting when it last read anything.
struct Watched<T> {
    io: T,
    last_read: Arc<Mutex<Instant>>,
}

impl<T: AsyncRead + Unpin> AsyncRead for Watched<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.io).poll_read(cx, buf);
        if buf.filled().len() > before {
            *self
                .last_read
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Instant::now();
        }
        polled
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Watched<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

/// Owns the connection: serves open and close requests, hands out inbound streams, the first to `first_inbound` if
/// given, and moves every stream's bytes, until the connection ends.
async fn drive<T>(
    mut connection: yamux::Connection<T>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    inbound: Option<mpsc::UnboundedSender<yamux::Stream>>,
    mut first_inbound: Option<oneshot::Sender<yamux::Stream>>,
) -> Result<(), yamux::ConnectionError>
where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let mut waiting = VecDeque::new();
    let mut closing = false;
    std::future::poll_fn(|cx| {
        loop {
            if !closing {
                match commands.poll_recv(cx) {
                    Poll::Ready(Some(Command::Open(reply))) => {
                        waiting.push_back(reply);
                        continue;
                    }
                    Poll::Ready(Some(Command::Close) | None) => closing = true,
                    Poll::Pending => {}
                }
            }
            if closing {
                // Dropping an active connection, unlike `poll_close`, fails every stream, including writers waiting
                // for credit; the peer sees the transport end.
                return Poll::Ready(Ok(()));
            }
            if !waiting.is_empty() {
                match connection.poll_new_outbound(cx) {
                    Poll::Ready(Ok(stream)) => {
                        if let Some(reply) = waiting.pop_front() {
                            let _ = reply.send(stream);
                        }
                        continue;
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => {}
                }
            }
            match connection.poll_next_inbound(cx) {
                Poll::Ready(Some(Ok(stream))) => {
                    if let Some(first) = first_inbound.take() {
                        let _ = first.send(stream);
                    } else if let Some(inbound) = &inbound {
                        let _ = inbound.send(stream);
                    }
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn channels_are_independent_and_end_with_the_connection() -> io::Result<()> {
        let (initiator, host) = tokio::io::duplex(1 << 16);
        let mux = Mux::open_on(initiator);
        let mut incoming = Incoming::accept_on(host);

        // A channel nobody reads must not stall another.
        let mut stalled = mux.open().await?;
        stalled.write_all(b"x").await?;
        let _unread = incoming.accept().await.ok_or(io::ErrorKind::NotFound)?;
        let stalled = tokio::spawn(async move { stalled.write_all(&vec![0; 1 << 20]).await });

        let mut echo = mux.open().await?;
        echo.write_all(b"ping").await?;
        echo.shutdown().await?;
        let mut second = incoming.accept().await.ok_or(io::ErrorKind::NotFound)?;
        let mut got = Vec::new();
        second.read_to_end(&mut got).await?;
        assert_eq!(got, b"ping");

        mux.close();
        mux.closed().await;
        assert!(incoming.accept().await.is_none());
        assert!(mux.open().await.is_err());
        assert!(stalled.await.is_ok_and(|written| written.is_err()));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeats_keep_idle_connections_open_and_end_silent_ones() -> io::Result<()> {
        let (initiator, host) = tokio::io::duplex(1 << 16);
        let mux = Mux::open_with_heartbeat(initiator);
        let mut incoming = Incoming::accept_with_heartbeat(host);
        tokio::time::sleep(DEAD_AFTER * 4).await;
        assert!(!mux.is_closed(), "an idle but present host stays connected");
        let mut channel = mux.open().await?;
        channel.write_all(b"run").await?;
        let mut accepted = incoming.accept().await.ok_or(io::ErrorKind::NotFound)?;
        let mut got = [0; 3];
        accepted.read_exact(&mut got).await?;
        assert_eq!(
            &got, b"run",
            "the heartbeat channel is not handed out as a run"
        );

        // Peers that are connected but never answer, like one that vanished behind a tunnel.
        let (initiator, _silent_host) = tokio::io::duplex(1 << 16);
        let (_silent_initiator, host) = tokio::io::duplex(1 << 16);
        let started = Instant::now();
        let mux = Mux::open_with_heartbeat(initiator);
        let mut incoming = Incoming::accept_with_heartbeat(host);
        mux.closed().await;
        assert!(incoming.accept().await.is_none());
        assert!(started.elapsed() >= DEAD_AFTER);
        assert!(started.elapsed() < DEAD_AFTER + HEARTBEAT);
        Ok(())
    }
}
