//! UDP transport for fast-paced multiplayer games.
//!
//! [`Manager`] drives the sans-IO [`netlib_core::Manager`] with a non-blocking
//! [`std::net::UdpSocket`]. Call [`poll`](Manager::poll) at the start of a tick, handle
//! events, queue messages with [`send`](Manager::send) and call [`flush`](Manager::flush)
//! at the end of the tick. No threads are spawned and no callbacks are made.

use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::time::{Duration, Instant};

pub use netlib_core::{
    ChannelConfig, Config, ConfigError, ConnectError, DeliveryMode, DisconnectReason, Event, ManagerStats, PeerError,
    PeerId, PeerState, PeerStats, SendError, Time,
};

/// Errors of [`Manager::bind`].
#[derive(Debug)]
pub enum BindError {
    /// The socket could not be created.
    Io(io::Error),
    /// The configuration is invalid.
    Config(ConfigError),
    /// The OS random number generator failed.
    Random,
}

impl std::fmt::Display for BindError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BindError::Io(e) => write!(f, "socket error: {e}"),
            BindError::Config(e) => write!(f, "invalid config: {e}"),
            BindError::Random => write!(f, "OS random number generator failed"),
        }
    }
}

impl std::error::Error for BindError {}

/// Socket-level counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SocketStats {
    /// Datagrams that could not be sent (buffer full, network unreachable, ...).
    pub send_errors: u64,
    /// Receive errors that were skipped, such as ICMP port unreachable on Windows.
    pub recv_errors: u64,
}

/// A connection manager bound to a UDP socket.
pub struct Manager {
    core: netlib_core::Manager,
    socket: UdpSocket,
    epoch: Instant,
    buf: Vec<u8>,
    stats: SocketStats,
}

impl Manager {
    /// Binds a socket. Use port 0 for clients.
    pub fn bind(addr: impl ToSocketAddrs, config: Config) -> Result<Self, BindError> {
        let socket = UdpSocket::bind(addr).map_err(BindError::Io)?;
        socket.set_nonblocking(true).map_err(BindError::Io)?;
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).map_err(|_| BindError::Random)?;
        let mtu = config.mtu;
        let epoch = Instant::now();
        let core = netlib_core::Manager::new(config, secret, Time::ZERO).map_err(BindError::Config)?;
        Ok(Manager {
            core,
            socket,
            epoch,
            // One extra byte detects oversized datagrams, which the core drops.
            buf: vec![0; mtu + 1],
            stats: SocketStats::default(),
        })
    }

    /// Current time on the manager's clock.
    pub fn now(&self) -> Time {
        Time::from_micros(u64::try_from(self.epoch.elapsed().as_micros()).unwrap_or(u64::MAX))
    }

    /// The bound address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// The sans-IO core, for statistics and peer queries.
    pub fn core(&self) -> &netlib_core::Manager {
        &self.core
    }

    /// Socket-level counters.
    pub fn socket_stats(&self) -> &SocketStats {
        &self.stats
    }

    /// Receives all pending datagrams, runs timers and sends handshake packets.
    pub fn poll(&mut self) -> io::Result<()> {
        loop {
            match self.socket.recv_from(&mut self.buf) {
                Ok((n, from)) => {
                    let now = self.now();
                    self.core.handle_datagram(now, from, &self.buf[..n]);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                // Windows reports ICMP port unreachable from earlier sends as a receive error.
                Err(e) if e.kind() == io::ErrorKind::ConnectionReset => self.stats.recv_errors += 1,
                Err(e) => return Err(e),
            }
        }
        let now = self.now();
        self.core.update(now);
        self.transmit();
        Ok(())
    }

    /// Takes the next event. Its data borrows the manager until the next call.
    pub fn poll_event(&mut self) -> Option<Event<'_>> {
        self.core.poll_event()
    }

    /// Starts connecting to `addr`.
    pub fn connect(&mut self, addr: impl ToSocketAddrs, data: &[u8]) -> io::Result<Result<PeerId, ConnectError>> {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no address"))?;
        let now = self.now();
        let r = self.core.connect(now, addr, data);
        self.transmit();
        Ok(r)
    }

    /// Accepts a pending incoming connection.
    pub fn accept(&mut self, peer: PeerId) -> Result<(), PeerError> {
        let now = self.now();
        let r = self.core.accept(now, peer);
        self.transmit();
        r
    }

    /// Rejects a pending incoming connection.
    pub fn reject(&mut self, peer: PeerId, data: &[u8]) -> Result<(), PeerError> {
        let r = self.core.reject(peer, data);
        self.transmit();
        r
    }

    /// Closes a connection; the remote side is notified immediately.
    pub fn disconnect(&mut self, peer: PeerId, data: &[u8]) -> Result<(), PeerError> {
        let r = self.core.disconnect(peer, data);
        self.transmit();
        r
    }

    /// Queues a message for the next [`flush`](Self::flush).
    pub fn send(&mut self, peer: PeerId, channel: u8, data: &[u8]) -> Result<(), SendError> {
        self.core.send(peer, channel, data)
    }

    /// Packs queued messages into datagrams and sends them.
    pub fn flush(&mut self) {
        let now = self.now();
        self.core.flush(now);
        self.transmit();
    }

    /// Time until the core has timer work, capped at `max`. Useful for sleeping in tools.
    pub fn time_to_next_timeout(&self, max: Duration) -> Duration {
        match self.core.next_timeout() {
            Some(t) => (t - self.now()).min(max),
            None => max,
        }
    }

    fn transmit(&mut self) {
        while let Some((to, n)) = self.core.poll_transmit(&mut self.buf) {
            match self.socket.send_to(&self.buf[..n], to) {
                Ok(_) => {}
                // UDP gives no delivery guarantee; a datagram we cannot send now is lost.
                Err(_) => self.stats.send_errors += 1,
            }
        }
    }
}
