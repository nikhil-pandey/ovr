//! Multiplexes runs over one host connection with yamux, so that any number of runs share a single SSH session or
//! dial-in TLS connection. The initiator opens a stream per run; the host accepts them.
//!
//! yamux sends no keepalives of its own: SSH connections use `ServerAliveInterval`, and dial-in connections use TCP
//! keepalive (see [`tune`]), so that a vanished peer still ends the connection.

use std::collections::VecDeque;
use std::io;
use std::task::Poll;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

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
        let (commands, closed) = start(io, yamux::Mode::Client, None);
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
        let (inbound, streams) = mpsc::unbounded_channel();
        let (commands, _) = start(io, yamux::Mode::Server, Some(inbound));
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
) -> (mpsc::UnboundedSender<Command>, watch::Receiver<bool>)
where
    T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (commands, requests) = mpsc::unbounded_channel();
    let (done, closed) = watch::channel(false);
    let connection = yamux::Connection::new(io.compat(), yamux::Config::default(), mode);
    tokio::spawn(async move {
        if let Err(err) = drive(connection, requests, inbound).await {
            tracing::debug!(%err, "connection ended");
        }
        done.send_replace(true);
    });
    (commands, closed)
}

/// Owns the connection: serves open and close requests, hands out inbound streams, and moves every stream's bytes,
/// until the connection ends.
async fn drive<T>(
    mut connection: yamux::Connection<T>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    inbound: Option<mpsc::UnboundedSender<yamux::Stream>>,
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
                    if let Some(inbound) = &inbound {
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
}
