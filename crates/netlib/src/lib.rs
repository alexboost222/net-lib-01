//! UDP transport for fast-paced multiplayer games.
//!
//! [`Manager`] drives the sans-IO [`netlib_core::Manager`] with a [`std::net::UdpSocket`].
//! Call [`poll`](Manager::poll) at the start of a tick, handle events, queue messages with
//! [`send`](Manager::send) and call [`flush`](Manager::flush) at the end of the tick.
//! No callbacks are made.
//!
//! By default a background thread receives datagrams as soon as they arrive. This gives exact
//! arrival times (so RTT does not include how long a datagram waited for the next `poll`),
//! answers PINGs immediately and keeps the socket buffer drained during frame hitches.
//! Set [`DriverConfig::receive_thread`] to `false` to receive inside `poll` instead.

use std::collections::VecDeque;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub use netlib_core::{
    ChannelConfig, Config, ConfigError, ConnectError, DeliveryMode, DisconnectReason, Event, ManagerStats, PeerError,
    PeerId, PeerState, PeerStats, SendError, Time,
};

/// Errors of [`Manager::bind`].
#[derive(Debug)]
pub enum BindError {
    /// The socket or the receive thread could not be created.
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

/// Settings of the socket driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverConfig {
    /// Receive datagrams on a background thread as soon as they arrive.
    pub receive_thread: bool,
}

impl Default for DriverConfig {
    fn default() -> Self {
        DriverConfig { receive_thread: true }
    }
}

/// Socket-level counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SocketStats {
    /// Datagrams that could not be sent (buffer full, network unreachable, ...).
    pub send_errors: u64,
    /// Receive errors that were skipped, such as ICMP port unreachable on Windows.
    pub recv_errors: u64,
}

/// How often the receive thread wakes up to check for shutdown.
const RECV_TIMEOUT: Duration = Duration::from_millis(50);

struct Shared {
    core: netlib_core::Manager,
    tx_buf: Vec<u8>,
    stats: SocketStats,
}

struct Inner {
    shared: Mutex<Shared>,
    socket: UdpSocket,
    epoch: Instant,
    stop: AtomicBool,
}

impl Inner {
    fn now(&self) -> Time {
        Time::from_micros(u64::try_from(self.epoch.elapsed().as_micros()).unwrap_or(u64::MAX))
    }

    fn lock(&self) -> MutexGuard<'_, Shared> {
        // The core stays consistent even if a panic happened while it was locked.
        self.shared.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Locks the core and returns it together with the current time, which is taken under
    /// the lock so the core always sees monotonic time.
    fn lock_now(&self) -> (MutexGuard<'_, Shared>, Time) {
        let guard = self.lock();
        (guard, self.now())
    }
}

fn transmit(shared: &mut Shared, socket: &UdpSocket) {
    let Shared { core, tx_buf, stats } = shared;
    while let Some((to, n)) = core.poll_transmit(tx_buf) {
        // UDP gives no delivery guarantee; a datagram we cannot send now is lost.
        if socket.send_to(&tx_buf[..n], to).is_err() {
            stats.send_errors += 1;
        }
    }
}

fn receive_loop(inner: Arc<Inner>, mtu: usize) {
    let mut buf = vec![0u8; mtu + 1];
    while !inner.stop.load(Ordering::Relaxed) {
        match inner.socket.recv_from(&mut buf) {
            Ok((n, from)) => {
                let (mut shared, now) = inner.lock_now();
                shared.core.handle_datagram(now, from, &buf[..n]);
                transmit(&mut shared, &inner.socket);
            }
            Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => {
                // Includes ICMP port unreachable on Windows. Keep receiving, but do not spin
                // if the socket is broken.
                inner.lock().stats.recv_errors += 1;
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

/// Owned copies of events taken from the core, so that `poll_event` does not hold the lock.
#[derive(Default)]
struct EventBuf {
    recs: VecDeque<Rec>,
    bytes: Vec<u8>,
}

#[derive(Clone, Copy)]
enum Rec {
    ConnectRequest(PeerId, SocketAddr, (usize, usize)),
    Connected(PeerId, SocketAddr),
    Disconnected(PeerId, ReasonRec),
    Message(PeerId, u8, (usize, usize)),
}

#[derive(Clone, Copy)]
enum ReasonRec {
    Plain(DisconnectReason<'static>),
    Remote((usize, usize)),
    Rejected((usize, usize)),
}

impl EventBuf {
    fn store(&mut self, data: &[u8]) -> (usize, usize) {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(data);
        (start, data.len())
    }

    fn push(&mut self, e: Event<'_>) {
        if self.recs.is_empty() {
            self.bytes.clear();
        }
        let rec = match e {
            Event::ConnectRequest { peer, addr, data } => Rec::ConnectRequest(peer, addr, self.store(data)),
            Event::Connected { peer, addr } => Rec::Connected(peer, addr),
            Event::Disconnected { peer, reason } => Rec::Disconnected(
                peer,
                match reason {
                    DisconnectReason::Remote { data } => ReasonRec::Remote(self.store(data)),
                    DisconnectReason::Rejected { data } => ReasonRec::Rejected(self.store(data)),
                    DisconnectReason::Timeout => ReasonRec::Plain(DisconnectReason::Timeout),
                    DisconnectReason::Local => ReasonRec::Plain(DisconnectReason::Local),
                    DisconnectReason::ConnectTimeout => ReasonRec::Plain(DisconnectReason::ConnectTimeout),
                    DisconnectReason::ServerFull => ReasonRec::Plain(DisconnectReason::ServerFull),
                    DisconnectReason::VersionMismatch => ReasonRec::Plain(DisconnectReason::VersionMismatch),
                    DisconnectReason::Reconnected => ReasonRec::Plain(DisconnectReason::Reconnected),
                },
            ),
            Event::Message { peer, channel, data } => Rec::Message(peer, channel, self.store(data)),
        };
        self.recs.push_back(rec);
    }

    fn slice(&self, (start, len): (usize, usize)) -> &[u8] {
        &self.bytes[start..start + len]
    }

    fn pop(&mut self) -> Option<Event<'_>> {
        let rec = self.recs.pop_front()?;
        Some(match rec {
            Rec::ConnectRequest(peer, addr, d) => Event::ConnectRequest {
                peer,
                addr,
                data: self.slice(d),
            },
            Rec::Connected(peer, addr) => Event::Connected { peer, addr },
            Rec::Disconnected(peer, r) => Event::Disconnected {
                peer,
                reason: match r {
                    ReasonRec::Plain(r) => r,
                    ReasonRec::Remote(d) => DisconnectReason::Remote { data: self.slice(d) },
                    ReasonRec::Rejected(d) => DisconnectReason::Rejected { data: self.slice(d) },
                },
            },
            Rec::Message(peer, channel, d) => Event::Message {
                peer,
                channel,
                data: self.slice(d),
            },
        })
    }
}

/// A connection manager bound to a UDP socket.
pub struct Manager {
    inner: Arc<Inner>,
    thread: Option<JoinHandle<()>>,
    recv_buf: Vec<u8>,
    events: EventBuf,
}

impl Manager {
    /// Binds a socket with the default driver settings. Use port 0 for clients.
    pub fn bind(addr: impl ToSocketAddrs, config: Config) -> Result<Self, BindError> {
        Self::bind_with(addr, config, DriverConfig::default())
    }

    /// Binds a socket with explicit driver settings.
    pub fn bind_with(addr: impl ToSocketAddrs, config: Config, driver: DriverConfig) -> Result<Self, BindError> {
        let socket = UdpSocket::bind(addr).map_err(BindError::Io)?;
        if driver.receive_thread {
            socket.set_read_timeout(Some(RECV_TIMEOUT)).map_err(BindError::Io)?;
        } else {
            socket.set_nonblocking(true).map_err(BindError::Io)?;
        }
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).map_err(|_| BindError::Random)?;
        let mtu = config.mtu;
        let epoch = Instant::now();
        let core = netlib_core::Manager::new(config, secret, Time::ZERO).map_err(BindError::Config)?;
        let inner = Arc::new(Inner {
            shared: Mutex::new(Shared {
                core,
                tx_buf: vec![0; mtu],
                stats: SocketStats::default(),
            }),
            socket,
            epoch,
            stop: AtomicBool::new(false),
        });
        let thread = if driver.receive_thread {
            let inner = Arc::clone(&inner);
            let handle = std::thread::Builder::new()
                .name("netlib-recv".into())
                .spawn(move || receive_loop(inner, mtu))
                .map_err(BindError::Io)?;
            Some(handle)
        } else {
            None
        };
        Ok(Manager {
            inner,
            thread,
            // One extra byte detects oversized datagrams, which the core drops.
            recv_buf: vec![0; mtu + 1],
            events: EventBuf::default(),
        })
    }

    /// Current time on the manager's clock.
    pub fn now(&self) -> Time {
        self.inner.now()
    }

    /// The bound address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.socket.local_addr()
    }

    /// Runs `f` with the sans-IO core locked, for statistics and peer queries.
    pub fn with_core<R>(&self, f: impl FnOnce(&netlib_core::Manager) -> R) -> R {
        f(&self.inner.lock().core)
    }

    /// State of a peer.
    pub fn peer_state(&self, peer: PeerId) -> Option<PeerState> {
        self.with_core(|c| c.peer_state(peer))
    }

    /// Smoothed network RTT of a peer.
    pub fn peer_rtt(&self, peer: PeerId) -> Option<Duration> {
        self.with_core(|c| c.peer_rtt(peer))
    }

    /// Counters of a peer.
    pub fn peer_stats(&self, peer: PeerId) -> Option<PeerStats> {
        self.with_core(|c| c.peer_stats(peer).cloned())
    }

    /// Socket-level counters.
    pub fn socket_stats(&self) -> SocketStats {
        self.inner.lock().stats.clone()
    }

    /// Runs timers, sends handshake packets and collects events. Without the receive thread,
    /// also receives all pending datagrams.
    pub fn poll(&mut self) -> io::Result<()> {
        if self.thread.is_none() {
            loop {
                match self.inner.socket.recv_from(&mut self.recv_buf) {
                    Ok((n, from)) => {
                        let (mut shared, now) = self.inner.lock_now();
                        shared.core.handle_datagram(now, from, &self.recv_buf[..n]);
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    // Windows reports ICMP port unreachable from earlier sends as a receive error.
                    Err(e) if e.kind() == io::ErrorKind::ConnectionReset => self.inner.lock().stats.recv_errors += 1,
                    Err(e) => return Err(e),
                }
            }
        }
        let (mut shared, now) = self.inner.lock_now();
        shared.core.update(now);
        transmit(&mut shared, &self.inner.socket);
        while let Some(e) = shared.core.poll_event() {
            self.events.push(e);
        }
        Ok(())
    }

    /// Takes the next event collected by [`poll`](Self::poll). Its data borrows the manager
    /// until the next call.
    pub fn poll_event(&mut self) -> Option<Event<'_>> {
        self.events.pop()
    }

    /// Starts connecting to `addr`.
    pub fn connect(&mut self, addr: impl ToSocketAddrs, data: &[u8]) -> io::Result<Result<PeerId, ConnectError>> {
        let addr = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no address"))?;
        Ok(self.locked(|core, now| core.connect(now, addr, data)))
    }

    /// Accepts a pending incoming connection.
    pub fn accept(&mut self, peer: PeerId) -> Result<(), PeerError> {
        self.locked(|core, now| core.accept(now, peer))
    }

    /// Rejects a pending incoming connection.
    pub fn reject(&mut self, peer: PeerId, data: &[u8]) -> Result<(), PeerError> {
        self.locked(|core, _| core.reject(peer, data))
    }

    /// Closes a connection; the remote side is notified immediately.
    pub fn disconnect(&mut self, peer: PeerId, data: &[u8]) -> Result<(), PeerError> {
        self.locked(|core, _| core.disconnect(peer, data))
    }

    /// Queues a message for the next [`flush`](Self::flush).
    pub fn send(&mut self, peer: PeerId, channel: u8, data: &[u8]) -> Result<(), SendError> {
        self.inner.lock().core.send(peer, channel, data)
    }

    /// Packs queued messages into datagrams and sends them.
    pub fn flush(&mut self) {
        self.locked(|core, now| core.flush(now));
    }

    /// Time until the core has timer work, capped at `max`. Useful for sleeping in tools.
    pub fn time_to_next_timeout(&self, max: Duration) -> Duration {
        let (shared, now) = self.inner.lock_now();
        match shared.core.next_timeout() {
            Some(t) => (t - now).min(max),
            None => max,
        }
    }

    /// Runs `f` on the locked core with the current time, then sends what it queued.
    fn locked<R>(&self, f: impl FnOnce(&mut netlib_core::Manager, Time) -> R) -> R {
        let (mut shared, now) = self.inner.lock_now();
        let r = f(&mut shared.core, now);
        transmit(&mut shared, &self.inner.socket);
        r
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        self.inner.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}
