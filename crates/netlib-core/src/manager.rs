use alloc::collections::{BTreeMap, VecDeque};
use alloc::vec;
use alloc::vec::Vec;
use core::net::SocketAddr;
use core::time::Duration;

use crate::channel::{Cand, Channel, CollectStats, Piece, Received as RecvResult, RecvLimits, SendOpts};
use crate::config::{Config, ConfigError, DeliveryMode, MAX_CHANNELS, SendOptions};
use crate::event::{DisconnectReason, Event, PeerId};
use crate::rtt::RttEstimator;
use crate::seq::{Received, RecvWindow, seq_newer};
use crate::siphash::{Rng, SipHasher};
use crate::time::Time;
use crate::wire::DATA_HEADER_MAX;
use crate::wire::{
    BlockHeader, Control, DataHeader, FLAG_PING, MIGRATE_LEN, REJECT_BY_APP, REJECT_SERVER_FULL,
    REJECT_VERSION_MISMATCH, Reader, TYPE_DATA, WIRE_VERSION, Writer, decode_ack_delay, encode_ack_delay, encode_addr,
    varint_len,
};
use netlib_log::{Level, LogSink, Record, netlog};

/// Packets remembered for ack processing and RTT measurement.
const SENT_RING: usize = 256;
/// How many outgoing packets repeat an unchanged ack state.
const ACK_REPEAT: u8 = 3;
/// Copies of a DISCONNECT packet sent back to back.
const DISCONNECT_COPIES: usize = 2;

/// Connection state of a peer as seen by the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerState {
    /// Outgoing connection attempt in progress.
    Connecting,
    /// Incoming connection waiting for `accept` or `reject`.
    Pending,
    /// Connection established.
    Connected,
}

/// Errors of [`Manager::connect`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectError {
    /// A peer with this address already exists.
    AlreadyExists,
    /// Connect data exceeds `Config::max_connect_data`.
    DataTooLarge,
}

/// Errors of [`Manager::send`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    /// The peer does not exist or is not connected.
    NotConnected,
    /// No channel with this index.
    InvalidChannel,
    /// The message exceeds `Config::max_message_size`.
    TooLarge,
    /// `Config::max_reliable_queue` messages of this channel wait for an ack.
    QueueFull,
    /// `ttl` or `replace_key` was given for a reliable channel.
    InvalidOptions,
}

/// Errors of `accept`, `reject` and `disconnect`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerError {
    /// The peer does not exist.
    UnknownPeer,
    /// The peer is not in a state that allows this operation.
    InvalidState,
    /// Data exceeds `Config::max_connect_data`.
    DataTooLarge,
}

macro_rules! impl_error {
    ($($t:ty),*) => {$(
        impl core::fmt::Display for $t {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                core::fmt::Debug::fmt(self, f)
            }
        }
        impl core::error::Error for $t {}
    )*};
}
impl_error!(ConnectError, SendError, PeerError);

/// Per-peer counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PeerStats {
    /// Datagrams sent, control packets included.
    pub packets_sent: u64,
    /// Bytes of UDP payload sent.
    pub bytes_sent: u64,
    /// DATA packets accepted.
    pub packets_received: u64,
    /// Bytes of UDP payload accepted.
    pub bytes_received: u64,
    /// DATA packets without messages sent only to carry an ack.
    pub standalone_acks_sent: u64,
    /// Immediate acks sent in reply to PING packets; they give RTT samples without ack delay.
    pub ping_acks_sent: u64,
    /// Sent DATA packets confirmed by the remote side.
    pub packets_acked: u64,
    /// Duplicate or too old DATA packets dropped.
    pub packets_duplicate: u64,
    /// Malformed DATA packets dropped.
    pub packets_malformed: u64,
    /// Messages queued by `send` and sent.
    pub messages_sent: u64,
    /// Messages delivered to the application.
    pub messages_received: u64,
    /// Messages dropped by sequenced delivery.
    pub messages_stale: u64,
    /// Reliable messages or fragments received again.
    pub messages_duplicate: u64,
    /// Reliable messages or fragments sent again after a loss or timeout.
    pub messages_retransmitted: u64,
    /// Sent packets declared lost (not acked while 3 later packets were).
    pub packets_lost: u64,
    /// Unreliable messages dropped because their TTL expired before they could be sent.
    pub messages_expired: u64,
    /// Unreliable messages replaced by a newer one with the same `replace_key`.
    pub messages_replaced: u64,
    /// Address changes of the remote side accepted by this (server) side.
    pub migrations: u64,
}

/// Counters that are not tied to a peer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManagerStats {
    /// Datagrams passed to `handle_datagram`.
    pub datagrams_received: u64,
    /// Datagrams dropped: malformed, oversized, unexpected or from unknown senders.
    pub datagrams_dropped: u64,
    /// CHALLENGE_RESPONSE packets with an invalid or expired cookie.
    pub invalid_cookies: u64,
    /// CHALLENGE packets sent.
    pub challenges_sent: u64,
    /// WHO_ARE_YOU replies to DATA from unknown addresses.
    pub who_are_you_sent: u64,
    /// Client address changes accepted (before path validation).
    pub migrations: u64,
    /// Migration attempts with an invalid token or a replayed seq.
    pub invalid_migrations: u64,
    /// Address changes reverted because the new path was not validated in time.
    pub failed_path_validations: u64,
}

#[derive(Debug, Clone, Default)]
struct SentPacket {
    seq: u16,
    time: Time,
    in_use: bool,
    acked: bool,
    /// Reliable pieces carried: `(channel, message seq, start offset)`.
    units: Vec<(u8, u16, u32)>,
}

#[derive(Debug, Clone, Copy)]
enum State {
    /// Client: waiting for CHALLENGE.
    Connecting {
        next_retry: Time,
        deadline: Time,
    },
    /// Client: waiting for ACCEPT.
    Challenged {
        timestamp: u32,
        mac: u64,
        next_retry: Time,
        deadline: Time,
    },
    /// Server: waiting for the application to accept or reject.
    Pending {
        deadline: Time,
    },
    Connected,
}

struct Peer {
    /// `PeerId::to_bits` of this peer, for logs.
    id: u64,
    addr: SocketAddr,
    outgoing: bool,
    state: State,
    nonce: u64,
    conn_id: u32,
    session_key: u64,
    connect_data: Vec<u8>,
    last_recv: Time,
    last_send: Time,
    last_ping: Time,
    /// When the packet with the highest seq so far arrived; the base of the reported ack delay.
    largest_recv_time: Time,
    /// Last immediate reply to a PING, for rate limiting.
    last_ping_ack: Option<Time>,
    /// Server side: a new client address waiting for path validation.
    path: Option<PathCheck>,
    /// Client side: migration fields are sent until a packet from this seq on is acked.
    migrate_from: Option<u16>,
    /// Periodic statistics: time and counters of the last report.
    last_stats: Time,
    stats_base: PeerStats,
    next_seq: u16,
    recv: RecvWindow,
    received_any: bool,
    ack_pending: bool,
    ack_repeat: u8,
    sent: Vec<SentPacket>,
    /// Highest packet seq acknowledged by the remote side.
    largest_acked: Option<u16>,
    /// Oldest packet seq not yet acked or declared lost.
    loss_scan: u16,
    rtt: RttEstimator,
    modes: Vec<DeliveryMode>,
    channels: Vec<Channel>,
    /// Scratch lists for `flush`.
    cands: Vec<Cand>,
    sched: Scheduler,
    /// Bandwidth budget: available bytes and the time they were last refilled.
    tokens: i64,
    last_refill: Time,
    stats: PeerStats,
}

impl Peer {
    fn new(config: &Config, addr: SocketAddr, outgoing: bool, state: State, nonce: u64, now: Time) -> Self {
        Peer {
            id: 0,
            addr,
            outgoing,
            state,
            nonce,
            conn_id: 0,
            session_key: 0,
            connect_data: Vec::new(),
            last_recv: now,
            last_send: now,
            last_ping: now,
            largest_recv_time: now,
            last_ping_ack: None,
            path: None,
            migrate_from: None,
            last_stats: now,
            stats_base: PeerStats::default(),
            next_seq: 0,
            recv: RecvWindow::default(),
            received_any: false,
            ack_pending: false,
            ack_repeat: 0,
            sent: vec![SentPacket::default(); SENT_RING],
            largest_acked: None,
            loss_scan: 0,
            rtt: RttEstimator::new(config.min_rto, config.max_rto),
            modes: config.channels.iter().map(|c| c.mode).collect(),
            channels: config.channels.iter().map(|c| Channel::new(c.mode)).collect(),
            cands: Vec::new(),
            sched: Scheduler::default(),
            tokens: config
                .bandwidth
                .map_or(0, |rate| i64::from(rate / 10).max(2 * config.mtu as i64)),
            last_refill: now,
            stats: PeerStats::default(),
        }
    }

    fn public_state(&self) -> PeerState {
        match self.state {
            State::Connecting { .. } | State::Challenged { .. } => PeerState::Connecting,
            State::Pending { .. } => PeerState::Pending,
            State::Connected => PeerState::Connected,
        }
    }
}

/// A client address change awaiting validation (server side).
#[derive(Debug, Clone, Copy)]
struct PathCheck {
    addr: SocketAddr,
    old: SocketAddr,
    challenge: u64,
    deadline: Time,
    next_challenge: Time,
    /// Seq of the packet that triggered the move; another authenticated packet from `addr`
    /// validates the path as well.
    trigger_seq: u16,
    /// Bytes received from and sent to `addr` before validation (anti-amplification).
    received: usize,
    sent: usize,
}

/// Before validation a new address gets at most this multiple of what it sent (RFC 9000).
const AMPLIFICATION_FACTOR: usize = 3;

/// Token proving knowledge of the session key in a migrating DATA packet.
fn migrate_token(session_key: u64, conn_id: u32, seq: u16) -> u64 {
    let mut h = SipHasher::new([session_key, u64::from(conn_id) ^ 0x6d69_6772_6174_6521]);
    h.write(&seq.to_le_bytes());
    h.finish()
}

/// The manager's log sink; without one, logging costs a branch.
#[derive(Default)]
pub(crate) struct Logger {
    sink: Option<alloc::boxed::Box<dyn LogSink + Send>>,
}

impl LogSink for Logger {
    #[inline]
    fn enabled(&self, level: Level) -> bool {
        self.sink.as_ref().is_some_and(|s| s.enabled(level))
    }

    fn log(&mut self, record: &Record<'_>) {
        if let Some(s) = &mut self.sink {
            s.log(record);
        }
    }
}

/// Text of a disconnect reason, for logs.
fn reason_text(r: &ReasonRec) -> &'static str {
    match r {
        ReasonRec::Timeout => "timeout",
        ReasonRec::Remote(..) => "closed by the remote side",
        ReasonRec::Local => "closed locally",
        ReasonRec::ConnectTimeout => "connect timeout",
        ReasonRec::Rejected(..) => "rejected",
        ReasonRec::ServerFull => "server full",
        ReasonRec::VersionMismatch => "version mismatch",
        ReasonRec::Reconnected => "reconnected",
    }
}

struct Slot {
    generation: u32,
    peer: Option<Peer>,
}

/// Outgoing datagrams stored back to back in one buffer.
#[derive(Default)]
struct TxQueue {
    queue: VecDeque<(SocketAddr, u32, u32)>,
    bytes: Vec<u8>,
}

impl TxQueue {
    /// Reserves `len` bytes and returns their offset.
    fn reserve(&mut self, len: usize) -> usize {
        if self.queue.is_empty() {
            self.bytes.clear();
        }
        let start = self.bytes.len();
        self.bytes.resize(start + len, 0);
        start
    }

    fn commit(&mut self, addr: SocketAddr, start: usize, len: usize) {
        self.bytes.truncate(start + len);
        self.queue.push_back((addr, start as u32, len as u32));
    }

    fn push_control(&mut self, mtu: usize, addr: SocketAddr, c: &Control<'_>, stats: Option<&mut PeerStats>) {
        let start = self.reserve(mtu);
        match c.encode(&mut self.bytes[start..]) {
            Ok(n) => {
                self.commit(addr, start, n);
                if let Some(s) = stats {
                    s.packets_sent += 1;
                    s.bytes_sent += n as u64;
                }
            }
            Err(_) => self.bytes.truncate(start),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ReasonRec {
    Timeout,
    Remote(u32, u32),
    Local,
    ConnectTimeout,
    Rejected(u32, u32),
    ServerFull,
    VersionMismatch,
    Reconnected,
}

#[derive(Debug, Clone, Copy)]
enum EventRec {
    ConnectRequest {
        peer: PeerId,
        addr: SocketAddr,
        data: (u32, u32),
    },
    Connected {
        peer: PeerId,
        addr: SocketAddr,
    },
    Disconnected {
        peer: PeerId,
        reason: ReasonRec,
    },
    Message {
        peer: PeerId,
        channel: u8,
        data: (u32, u32),
    },
}

/// Pending events; payloads are stored back to back in one buffer.
#[derive(Default)]
struct EventQueue {
    recs: VecDeque<EventRec>,
    bytes: Vec<u8>,
}

impl EventQueue {
    fn store(&mut self, data: &[u8]) -> (u32, u32) {
        if self.recs.is_empty() {
            self.bytes.clear();
        }
        let start = self.bytes.len();
        self.bytes.extend_from_slice(data);
        (start as u32, data.len() as u32)
    }

    fn push(&mut self, rec: EventRec) {
        self.recs.push_back(rec);
    }

    fn slice(&self, (start, len): (u32, u32)) -> &[u8] {
        &self.bytes[start as usize..(start + len) as usize]
    }
}

/// One message inside a DATA packet.
struct Msg<'a> {
    channel: u8,
    /// Message seq and `(total, offset)` for fragments; reliable channels only.
    seq: Option<u16>,
    frag: Option<(u32, u32)>,
    data: &'a [u8],
}

struct BlockState {
    header: BlockHeader,
    remaining: usize,
    reliable: bool,
    prev_seq: u16,
}

/// Iterates over the messages of a DATA packet body. Yields `Err` once on malformed input.
struct MsgIter<'a, 'm> {
    r: Reader<'a>,
    modes: &'m [DeliveryMode],
    block: Option<BlockState>,
    finished: bool,
}

impl<'a, 'm> MsgIter<'a, 'm> {
    fn new(body: &'a [u8], modes: &'m [DeliveryMode]) -> Self {
        MsgIter {
            r: Reader::new(body),
            modes,
            block: None,
            finished: false,
        }
    }

    fn fail(&mut self) -> Option<Result<Msg<'a>, ()>> {
        self.finished = true;
        Some(Err(()))
    }

    fn message(&mut self) -> Option<Msg<'a>> {
        let b = self.block.as_mut()?;
        b.remaining -= 1;
        let last_of_tail = b.header.tail && b.remaining == 0;
        let len = if last_of_tail {
            None
        } else {
            Some(self.r.varint_usize()?)
        };
        let (mut seq, mut frag) = (None, None);
        if b.reliable {
            let tag = self.r.varint()?;
            let delta = u16::try_from(tag >> 1).ok().filter(|&d| d < 0x8000)?;
            let s = b.prev_seq.wrapping_add(delta);
            b.prev_seq = s;
            seq = Some(s);
            if tag & 1 == 1 {
                let total = u32::try_from(self.r.varint()?).ok()?;
                let offset = u32::try_from(self.r.varint()?).ok()?;
                if total as usize > crate::channel::MAX_FRAGMENTED_TOTAL || offset > total {
                    return None;
                }
                frag = Some((total, offset));
            }
        }
        let len = len.unwrap_or(self.r.remaining());
        let data = self.r.bytes(len)?;
        if let Some((total, offset)) = frag {
            if offset as usize + data.len() > total as usize {
                return None;
            }
        }
        Some(Msg {
            channel: b.header.channel,
            seq,
            frag,
            data,
        })
    }
}

impl<'a> Iterator for MsgIter<'a, '_> {
    type Item = Result<Msg<'a>, ()>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.finished {
                return None;
            }
            if let Some(b) = &self.block {
                if b.remaining > 0 {
                    let tail_end = b.header.tail && b.remaining == 1;
                    let Some(msg) = self.message() else {
                        return self.fail();
                    };
                    if tail_end {
                        self.finished = true;
                    }
                    return Some(Ok(msg));
                }
                self.block = None;
            }
            if self.r.is_empty() {
                self.finished = true;
                return None;
            }
            let Some(header) = BlockHeader::decode(&mut self.r) else {
                return self.fail();
            };
            let Some(mode) = self.modes.get(usize::from(header.channel)) else {
                return self.fail();
            };
            let reliable = mode.is_reliable();
            let prev_seq = if reliable {
                match self.r.u16() {
                    Some(base) => base,
                    None => return self.fail(),
                }
            } else {
                0
            };
            self.block = Some(BlockState {
                header,
                remaining: header.count,
                reliable,
                prev_seq,
            });
        }
    }
}

/// The sans-IO connection manager: feed it datagrams and time, take datagrams and events out.
///
/// A typical tick:
/// 1. pass every received datagram to [`handle_datagram`](Self::handle_datagram);
/// 2. call [`update`](Self::update) to run timers;
/// 3. drain [`poll_event`](Self::poll_event) and react, calling [`send`](Self::send);
/// 4. call [`flush`](Self::flush) and send every datagram from [`poll_transmit`](Self::poll_transmit).
pub struct Manager {
    config: Config,
    start: Time,
    cookie_key: [u64; 2],
    rng: Rng,
    slots: Vec<Slot>,
    free: Vec<u32>,
    by_addr: BTreeMap<SocketAddr, u32>,
    /// Server side: connected incoming peers by connection id, for migration.
    by_conn: BTreeMap<u32, u32>,
    incoming: usize,
    events: EventQueue,
    tx: TxQueue,
    scratch: Vec<(u32, Option<ReasonRec>)>,
    stats: ManagerStats,
    /// Latest time passed to any call; used as the enqueue time of `send`.
    now_hint: Time,
    log: Logger,
}

fn cookie_mac(key: [u64; 2], addr: &SocketAddr, nonce: u64, timestamp: u32, protocol_id: u32) -> u64 {
    let mut a = [0u8; 19];
    let n = encode_addr(addr, &mut a);
    let mut h = SipHasher::new(key);
    h.write(&a[..n]);
    h.write(&nonce.to_le_bytes());
    h.write(&timestamp.to_le_bytes());
    h.write(&protocol_id.to_le_bytes());
    h.finish()
}

fn key_from(bytes: &[u8]) -> [u64; 2] {
    let mut k = [0u8; 8];
    k.copy_from_slice(&bytes[..8]);
    let k0 = u64::from_le_bytes(k);
    k.copy_from_slice(&bytes[8..16]);
    [k0, u64::from_le_bytes(k)]
}

impl Manager {
    /// Creates a manager.
    ///
    /// `secret` must come from a cryptographically secure source: it keys the handshake
    /// cookies and the generator of nonces and connection ids.
    pub fn new(config: Config, secret: [u8; 32], now: Time) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Manager {
            config,
            start: now,
            cookie_key: key_from(&secret[..16]),
            rng: Rng::new(key_from(&secret[16..])),
            slots: Vec::new(),
            free: Vec::new(),
            by_addr: BTreeMap::new(),
            by_conn: BTreeMap::new(),
            incoming: 0,
            events: EventQueue::default(),
            tx: TxQueue::default(),
            scratch: Vec::new(),
            stats: ManagerStats::default(),
            now_hint: now,
            log: Logger::default(),
        })
    }

    /// The configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Sets the log sink, or removes it with `None`. Events follow the catalog in
    /// `docs/LOG_EVENTS.md`; their times are microseconds on the manager's clock.
    pub fn set_log_sink(&mut self, sink: Option<alloc::boxed::Box<dyn LogSink + Send>>) {
        self.log.sink = sink;
    }

    /// Global counters.
    pub fn stats(&self) -> &ManagerStats {
        &self.stats
    }

    fn now_secs(&self, now: Time) -> u32 {
        u32::try_from((now - self.start).as_secs()).unwrap_or(u32::MAX)
    }

    fn id_of(&self, index: u32) -> PeerId {
        PeerId {
            index,
            generation: self.slots[index as usize].generation,
        }
    }

    fn index_of(&self, id: PeerId) -> Option<usize> {
        let slot = self.slots.get(id.index as usize)?;
        (slot.generation == id.generation && slot.peer.is_some()).then_some(id.index as usize)
    }

    fn peer(&self, id: PeerId) -> Option<&Peer> {
        self.slots[self.index_of(id)?].peer.as_ref()
    }

    fn insert_peer(&mut self, mut peer: Peer) -> u32 {
        let addr = peer.addr;
        if !peer.outgoing {
            self.incoming += 1;
        }
        let index = match self.free.pop() {
            Some(i) => {
                peer.id = PeerId {
                    index: i,
                    generation: self.slots[i as usize].generation,
                }
                .to_bits();
                self.slots[i as usize].peer = Some(peer);
                i
            }
            None => {
                peer.id = PeerId {
                    index: self.slots.len() as u32,
                    generation: 0,
                }
                .to_bits();
                self.slots.push(Slot {
                    generation: 0,
                    peer: Some(peer),
                });
                (self.slots.len() - 1) as u32
            }
        };
        self.by_addr.insert(addr, index);
        index
    }

    fn remove_peer(&mut self, index: u32, reason: Option<ReasonRec>) {
        let id = self.id_of(index);
        let slot = &mut self.slots[index as usize];
        let Some(peer) = slot.peer.take() else {
            return;
        };
        slot.generation = slot.generation.wrapping_add(1);
        self.free.push(index);
        self.by_addr.remove(&peer.addr);
        if let Some(p) = peer.path {
            self.by_addr.remove(&p.old);
            self.by_addr.remove(&p.addr);
        }
        if !peer.outgoing && self.by_conn.get(&peer.conn_id) == Some(&index) {
            self.by_conn.remove(&peer.conn_id);
        }
        if !peer.outgoing {
            self.incoming -= 1;
        }
        if let Some(reason) = reason {
            netlog!(
                self.log,
                Info,
                self.now_hint.as_micros(),
                "Peer {PeerId} at {RemoteEndpoint} disconnected: {Reason}",
                PeerId = peer.id,
                RemoteEndpoint = peer.addr,
                Reason = reason_text(&reason)
            );
            self.events.push(EventRec::Disconnected { peer: id, reason });
        }
    }

    /// Starts connecting to `addr`. Completion is reported by `Event::Connected`
    /// or `Event::Disconnected`.
    pub fn connect(&mut self, now: Time, addr: SocketAddr, data: &[u8]) -> Result<PeerId, ConnectError> {
        if data.len() > self.config.max_connect_data {
            return Err(ConnectError::DataTooLarge);
        }
        if self.by_addr.contains_key(&addr) {
            return Err(ConnectError::AlreadyExists);
        }
        let nonce = self.rng.next_u64();
        let state = State::Connecting {
            next_retry: now + self.config.connect_retry,
            deadline: now + self.config.connect_timeout,
        };
        let mut peer = Peer::new(&self.config, addr, true, state, nonce, now);
        peer.connect_data = data.to_vec();
        let request = Control::ConnectRequest {
            protocol_id: self.config.protocol_id,
            version: WIRE_VERSION,
            nonce,
        };
        self.tx
            .push_control(self.config.mtu, addr, &request, Some(&mut peer.stats));
        let index = self.insert_peer(peer);
        let id = self.id_of(index);
        netlog!(
            self.log,
            Info,
            now.as_micros(),
            "Connecting to {RemoteEndpoint} as peer {PeerId}",
            RemoteEndpoint = addr,
            PeerId = id.to_bits()
        );
        Ok(id)
    }

    /// Accepts a pending incoming connection.
    pub fn accept(&mut self, now: Time, id: PeerId) -> Result<(), PeerError> {
        let index = self.index_of(id).ok_or(PeerError::UnknownPeer)?;
        let mut conn_id = self.rng.next_nonzero_u32();
        while self.by_conn.contains_key(&conn_id) {
            conn_id = self.rng.next_nonzero_u32();
        }
        let session_key = self.rng.next_u64();
        let peer = self.slots[index].peer.as_mut().ok_or(PeerError::UnknownPeer)?;
        if !matches!(peer.state, State::Pending { .. }) {
            return Err(PeerError::InvalidState);
        }
        peer.state = State::Connected;
        peer.conn_id = conn_id;
        peer.session_key = session_key;
        peer.last_recv = now;
        peer.last_send = now;
        peer.last_ping = now;
        self.by_conn.insert(conn_id, index as u32);
        netlog!(
            self.log,
            Info,
            now.as_micros(),
            "Peer {PeerId} connected from {RemoteEndpoint} as connection {ConnId}",
            PeerId = peer.id,
            RemoteEndpoint = peer.addr,
            ConnId = conn_id
        );
        let accept = Control::Accept {
            nonce: peer.nonce,
            conn_id,
            session_key,
        };
        self.tx
            .push_control(self.config.mtu, peer.addr, &accept, Some(&mut peer.stats));
        self.events.push(EventRec::Connected {
            peer: id,
            addr: peer.addr,
        });
        Ok(())
    }

    /// Rejects a pending incoming connection; `data` is delivered to the client.
    pub fn reject(&mut self, id: PeerId, data: &[u8]) -> Result<(), PeerError> {
        if data.len() > self.config.max_connect_data {
            return Err(PeerError::DataTooLarge);
        }
        let index = self.index_of(id).ok_or(PeerError::UnknownPeer)?;
        let peer = self.slots[index].peer.as_ref().ok_or(PeerError::UnknownPeer)?;
        if !matches!(peer.state, State::Pending { .. }) {
            return Err(PeerError::InvalidState);
        }
        let reject = Control::Reject {
            nonce: peer.nonce,
            reason: REJECT_BY_APP,
            data,
        };
        self.tx.push_control(self.config.mtu, peer.addr, &reject, None);
        netlog!(
            self.log,
            Info,
            self.now_hint.as_micros(),
            "Rejected connection request {PeerId} from {RemoteEndpoint}",
            PeerId = peer.id,
            RemoteEndpoint = peer.addr
        );
        self.remove_peer(index as u32, None);
        Ok(())
    }

    /// Closes a connection or cancels a connection attempt. Emits `Disconnected { reason: Local }`
    /// except for pending incoming peers, which are rejected instead.
    pub fn disconnect(&mut self, id: PeerId, data: &[u8]) -> Result<(), PeerError> {
        if data.len() > self.config.max_connect_data {
            return Err(PeerError::DataTooLarge);
        }
        let index = self.index_of(id).ok_or(PeerError::UnknownPeer)?;
        let peer = self.slots[index].peer.as_ref().ok_or(PeerError::UnknownPeer)?;
        match peer.state {
            State::Pending { .. } => return self.reject(id, data),
            State::Connected => {
                let packet = Control::Disconnect {
                    conn_id: peer.conn_id,
                    data,
                };
                for _ in 0..DISCONNECT_COPIES {
                    self.tx.push_control(self.config.mtu, peer.addr, &packet, None);
                }
            }
            State::Connecting { .. } | State::Challenged { .. } => {}
        }
        self.remove_peer(index as u32, Some(ReasonRec::Local));
        Ok(())
    }

    /// Queues a message with the channel defaults. It is put on the wire by a later
    /// [`flush`](Self::flush): the next one unless the bandwidth budget holds it back.
    pub fn send(&mut self, id: PeerId, channel: u8, data: &[u8]) -> Result<(), SendError> {
        self.send_with(id, channel, data, SendOptions::default())
    }

    /// Queues a message with per-message priority, TTL or replace key.
    pub fn send_with(&mut self, id: PeerId, channel: u8, data: &[u8], options: SendOptions) -> Result<(), SendError> {
        let index = self.index_of(id).ok_or(SendError::NotConnected)?;
        let config = &self.config;
        let now = self.now_hint;
        let peer = self.slots[index].peer.as_mut().ok_or(SendError::NotConnected)?;
        if !matches!(peer.state, State::Connected) {
            return Err(SendError::NotConnected);
        }
        let c = usize::from(channel);
        let ch = peer.channels.get_mut(c).ok_or(SendError::InvalidChannel)?;
        let defaults = &config.channels[c];
        if data.len() > config.max_message_size(ch.mode) {
            return Err(SendError::TooLarge);
        }
        let priority = options.priority.unwrap_or(defaults.priority);
        if ch.mode.is_reliable() {
            if options.ttl.is_some() || options.replace_key.is_some() {
                return Err(SendError::InvalidOptions);
            }
            // A sequenced channel replaces its queue, so it never fills up.
            if ch.mode != DeliveryMode::ReliableSequenced && ch.reliable_queue_len() >= config.max_reliable_queue {
                netlog!(
                    self.log,
                    Warn,
                    now.as_micros(),
                    "Reliable queue of channel {Channel} to peer {PeerId} is full: {Queued} messages wait for acks",
                    Channel = channel,
                    PeerId = peer.id,
                    Queued = ch.reliable_queue_len()
                );
                return Err(SendError::QueueFull);
            }
            ch.queue_reliable(data, config.single_reliable_limit(), now, priority);
        } else {
            let opts = SendOpts {
                priority,
                ttl: options.ttl.or(defaults.ttl),
                replace_key: options.replace_key,
            };
            if ch.queue_unreliable(data, now, opts) {
                peer.stats.messages_replaced += 1;
            }
        }
        Ok(())
    }

    /// Messages of a channel still waiting to be sent (unreliable) or acknowledged (reliable).
    pub fn queued_messages(&self, id: PeerId, channel: u8) -> Option<usize> {
        let ch = self.peer(id)?.channels.get(usize::from(channel))?;
        Some(if ch.mode.is_reliable() {
            ch.reliable_queue_len()
        } else {
            ch.unreliable_queue_len()
        })
    }

    /// Processes one received datagram.
    pub fn handle_datagram(&mut self, now: Time, from: SocketAddr, data: &[u8]) {
        self.now_hint = self.now_hint.max(now);
        self.stats.datagrams_received += 1;
        if data.is_empty() || data.len() > self.config.mtu {
            self.stats.datagrams_dropped += 1;
            return;
        }
        if data[0] & 0x0f == TYPE_DATA {
            self.handle_data(now, from, data);
            return;
        }
        let handled = match Control::decode(data) {
            Some(Control::ConnectRequest {
                protocol_id,
                version,
                nonce,
            }) => self.on_connect_request(now, from, protocol_id, version, nonce),
            Some(Control::Challenge { nonce, timestamp, mac }) => self.on_challenge(now, from, nonce, timestamp, mac),
            Some(Control::ChallengeResponse {
                nonce,
                timestamp,
                mac,
                data,
            }) => self.on_challenge_response(now, from, nonce, timestamp, mac, data),
            Some(Control::Accept {
                nonce,
                conn_id,
                session_key,
            }) => self.on_accept(now, from, nonce, conn_id, session_key),
            Some(Control::Reject { nonce, reason, data }) => self.on_reject(from, nonce, reason, data),
            Some(Control::Disconnect { conn_id, data }) => self.on_disconnect(from, conn_id, data),
            Some(Control::WhoAreYou) => self.on_who_are_you(from),
            Some(Control::PathChallenge { challenge }) => self.on_path_challenge(from, challenge),
            Some(Control::PathResponse { challenge }) => self.on_path_response(from, challenge),
            None => false,
        };
        if !handled {
            self.stats.datagrams_dropped += 1;
        }
    }

    fn on_connect_request(&mut self, now: Time, from: SocketAddr, protocol_id: u32, version: u16, nonce: u64) -> bool {
        if !self.config.accept_incoming || protocol_id != self.config.protocol_id {
            return false;
        }
        let existing = self.by_addr.get(&from).copied();
        if let Some(i) = existing {
            if self.slots[i as usize].peer.as_ref().is_some_and(|p| p.outgoing) {
                return false;
            }
        }
        let mtu = self.config.mtu;
        if version != WIRE_VERSION {
            let reject = Control::Reject {
                nonce,
                reason: REJECT_VERSION_MISMATCH,
                data: &[],
            };
            self.tx.push_control(mtu, from, &reject, None);
            netlog!(
                self.log,
                Warn,
                now.as_micros(),
                "Rejected {RemoteEndpoint}: wire version {Version} is not supported",
                RemoteEndpoint = from,
                Version = version
            );
            return true;
        }
        if existing.is_none() && self.incoming >= self.config.max_incoming {
            let reject = Control::Reject {
                nonce,
                reason: REJECT_SERVER_FULL,
                data: &[],
            };
            self.tx.push_control(mtu, from, &reject, None);
            netlog!(
                self.log,
                Info,
                now.as_micros(),
                "Rejected {RemoteEndpoint}: server full with {Incoming} peers",
                RemoteEndpoint = from,
                Incoming = self.incoming
            );
            return true;
        }
        let timestamp = self.now_secs(now);
        let mac = cookie_mac(self.cookie_key, &from, nonce, timestamp, self.config.protocol_id);
        self.tx
            .push_control(mtu, from, &Control::Challenge { nonce, timestamp, mac }, None);
        self.stats.challenges_sent += 1;
        netlog!(
            self.log,
            Trace,
            now.as_micros(),
            "Challenge sent to {RemoteEndpoint}",
            RemoteEndpoint = from
        );
        true
    }

    fn on_challenge(&mut self, now: Time, from: SocketAddr, nonce: u64, timestamp: u32, mac: u64) -> bool {
        let Some(&index) = self.by_addr.get(&from) else {
            return false;
        };
        let retry = self.config.connect_retry;
        let Some(peer) = self.slots[index as usize].peer.as_mut() else {
            return false;
        };
        let State::Connecting { deadline, .. } = peer.state else {
            return false;
        };
        if !peer.outgoing || peer.nonce != nonce {
            return false;
        }
        peer.state = State::Challenged {
            timestamp,
            mac,
            next_retry: now + retry,
            deadline,
        };
        let response = Control::ChallengeResponse {
            nonce,
            timestamp,
            mac,
            data: &peer.connect_data,
        };
        self.tx
            .push_control(self.config.mtu, from, &response, Some(&mut peer.stats));
        true
    }

    fn on_challenge_response(
        &mut self,
        now: Time,
        from: SocketAddr,
        nonce: u64,
        timestamp: u32,
        mac: u64,
        data: &[u8],
    ) -> bool {
        if !self.config.accept_incoming || data.len() > self.config.max_connect_data {
            return false;
        }
        let now_secs = self.now_secs(now);
        let fresh = timestamp <= now_secs && u64::from(now_secs - timestamp) <= self.config.cookie_lifetime.as_secs();
        if !fresh || mac != cookie_mac(self.cookie_key, &from, nonce, timestamp, self.config.protocol_id) {
            self.stats.invalid_cookies += 1;
            netlog!(
                self.log,
                Debug,
                now.as_micros(),
                "Invalid or expired handshake cookie from {RemoteEndpoint}",
                RemoteEndpoint = from
            );
            return false;
        }
        if let Some(&index) = self.by_addr.get(&from) {
            let Some(peer) = self.slots[index as usize].peer.as_mut() else {
                return false;
            };
            if peer.outgoing {
                return false;
            }
            if peer.nonce == nonce {
                if let State::Connected = peer.state {
                    // Our ACCEPT was lost; the client is still retrying.
                    let accept = Control::Accept {
                        nonce,
                        conn_id: peer.conn_id,
                        session_key: peer.session_key,
                    };
                    self.tx
                        .push_control(self.config.mtu, from, &accept, Some(&mut peer.stats));
                }
                return true;
            }
            // Same address, new nonce: the client restarted.
            let reason = matches!(peer.state, State::Connected).then_some(ReasonRec::Reconnected);
            self.remove_peer(index, reason);
        }
        if self.incoming >= self.config.max_incoming {
            let reject = Control::Reject {
                nonce,
                reason: REJECT_SERVER_FULL,
                data: &[],
            };
            self.tx.push_control(self.config.mtu, from, &reject, None);
            return true;
        }
        let state = State::Pending {
            deadline: now + self.config.connect_timeout,
        };
        let peer = Peer::new(&self.config, from, false, state, nonce, now);
        let index = self.insert_peer(peer);
        let id = self.id_of(index);
        netlog!(
            self.log,
            Info,
            now.as_micros(),
            "Connection request {PeerId} from {RemoteEndpoint} with {DataLength} bytes of data",
            PeerId = id.to_bits(),
            RemoteEndpoint = from,
            DataLength = data.len()
        );
        let data = self.events.store(data);
        self.events.push(EventRec::ConnectRequest {
            peer: id,
            addr: from,
            data,
        });
        true
    }

    fn on_accept(&mut self, now: Time, from: SocketAddr, nonce: u64, conn_id: u32, session_key: u64) -> bool {
        let Some(&index) = self.by_addr.get(&from) else {
            return false;
        };
        let id = self.id_of(index);
        let Some(peer) = self.slots[index as usize].peer.as_mut() else {
            return false;
        };
        if !peer.outgoing || peer.nonce != nonce || !matches!(peer.state, State::Challenged { .. }) {
            return false;
        }
        peer.state = State::Connected;
        peer.conn_id = conn_id;
        peer.session_key = session_key;
        peer.connect_data = Vec::new();
        peer.last_recv = now;
        peer.last_send = now;
        peer.last_ping = now;
        netlog!(
            self.log,
            Info,
            now.as_micros(),
            "Connected to {RemoteEndpoint} as peer {PeerId}, connection {ConnId}",
            RemoteEndpoint = from,
            PeerId = peer.id,
            ConnId = conn_id
        );
        self.events.push(EventRec::Connected { peer: id, addr: from });
        true
    }

    fn on_reject(&mut self, from: SocketAddr, nonce: u64, reason: u8, data: &[u8]) -> bool {
        let Some(&index) = self.by_addr.get(&from) else {
            return false;
        };
        let Some(peer) = self.slots[index as usize].peer.as_ref() else {
            return false;
        };
        let connecting = matches!(peer.state, State::Connecting { .. } | State::Challenged { .. });
        if !peer.outgoing || peer.nonce != nonce || !connecting {
            return false;
        }
        let reason = match reason {
            REJECT_SERVER_FULL => ReasonRec::ServerFull,
            REJECT_VERSION_MISMATCH => ReasonRec::VersionMismatch,
            _ => {
                let (s, l) = self.events.store(data);
                ReasonRec::Rejected(s, l)
            }
        };
        self.remove_peer(index, Some(reason));
        true
    }

    fn on_disconnect(&mut self, from: SocketAddr, conn_id: u32, data: &[u8]) -> bool {
        let Some(&index) = self.by_addr.get(&from) else {
            return false;
        };
        let Some(peer) = self.slots[index as usize].peer.as_ref() else {
            return false;
        };
        if !matches!(peer.state, State::Connected) || peer.conn_id != conn_id {
            return false;
        }
        let (s, l) = self.events.store(data);
        self.remove_peer(index, Some(ReasonRec::Remote(s, l)));
        true
    }

    fn on_who_are_you(&mut self, from: SocketAddr) -> bool {
        let Some(&index) = self.by_addr.get(&from) else {
            return false;
        };
        let Some(peer) = self.slots[index as usize].peer.as_mut() else {
            return false;
        };
        if !peer.outgoing || !matches!(peer.state, State::Connected) {
            return false;
        }
        // Our address changed (NAT rebinding): prove who we are in the next packets.
        if peer.migrate_from.is_none() {
            peer.migrate_from = Some(peer.next_seq);
            netlog!(
                self.log,
                Info,
                self.now_hint.as_micros(),
                "Server {RemoteEndpoint} does not know our address: peer {PeerId} proves its identity",
                RemoteEndpoint = from,
                PeerId = peer.id
            );
        }
        true
    }

    fn on_path_challenge(&mut self, from: SocketAddr, challenge: u64) -> bool {
        let Some(&index) = self.by_addr.get(&from) else {
            return false;
        };
        let Some(peer) = self.slots[index as usize].peer.as_mut() else {
            return false;
        };
        if !peer.outgoing || !matches!(peer.state, State::Connected) {
            return false;
        }
        let response = Control::PathResponse { challenge };
        self.tx
            .push_control(self.config.mtu, from, &response, Some(&mut peer.stats));
        true
    }

    fn on_path_response(&mut self, from: SocketAddr, challenge: u64) -> bool {
        let Some(&index) = self.by_addr.get(&from) else {
            return false;
        };
        let Some(peer) = self.slots[index as usize].peer.as_mut() else {
            return false;
        };
        let Some(p) = peer.path else {
            return false;
        };
        if p.addr != from || p.challenge != challenge {
            return false;
        }
        peer.path = None;
        if p.old != p.addr {
            self.by_addr.remove(&p.old);
        }
        netlog!(
            self.log,
            Info,
            self.now_hint.as_micros(),
            "Path to {RemoteEndpoint} of peer {PeerId} validated",
            RemoteEndpoint = from,
            PeerId = peer.id
        );
        true
    }

    /// DATA from an address no peer has: either a client that moved and proves it with
    /// migration fields, or one that has to be asked to do so.
    fn try_migrate(&mut self, now: Time, from: SocketAddr, data: &[u8]) -> Option<u32> {
        let mut r = Reader::new(data);
        let header = DataHeader::decode(&mut r)?;
        let Some((conn_id, token)) = header.migrate else {
            if self.config.accept_incoming {
                // One byte in reply to at least three: no amplification.
                self.tx.push_control(self.config.mtu, from, &Control::WhoAreYou, None);
                self.stats.who_are_you_sent += 1;
                netlog!(
                    self.log,
                    Debug,
                    now.as_micros(),
                    "DATA from unknown {RemoteEndpoint}: asked it to identify",
                    RemoteEndpoint = from
                );
            }
            return None;
        };
        let &index = self.by_conn.get(&conn_id)?;
        let challenge = self.rng.next_u64();
        let peer = self.slots[index as usize].peer.as_mut()?;
        let valid = !peer.outgoing
            && matches!(peer.state, State::Connected)
            && token == migrate_token(peer.session_key, conn_id, header.seq)
            && peer.recv.check(header.seq) == Received::New
            && !MsgIter::new(r.rest(), &peer.modes).any(|m| m.is_err());
        if !valid {
            self.stats.invalid_migrations += 1;
            netlog!(
                self.log,
                Warn,
                now.as_micros(),
                "Rejected move of connection {ConnId} to {RemoteEndpoint}: invalid token or replayed packet",
                ConnId = conn_id,
                RemoteEndpoint = from
            );
            return None;
        }
        let mut old = peer.addr;
        if let Some(p) = peer.path {
            // Moved again before the previous move was validated.
            old = p.old;
            if p.addr != old {
                self.by_addr.remove(&p.addr);
            }
        }
        let rto = peer.rtt.rto();
        peer.addr = from;
        peer.path = Some(PathCheck {
            addr: from,
            old,
            challenge,
            deadline: now + (rto * 3).max(Duration::from_secs(1)),
            next_challenge: now + rto,
            trigger_seq: header.seq,
            received: 0,
            sent: 9,
        });
        peer.stats.migrations += 1;
        netlog!(
            self.log,
            Info,
            now.as_micros(),
            "Peer {PeerId} moved from {OldEndpoint} to {NewEndpoint}",
            PeerId = peer.id,
            OldEndpoint = old,
            NewEndpoint = from
        );
        self.tx
            .push_control(self.config.mtu, from, &Control::PathChallenge { challenge }, None);
        self.by_addr.insert(from, index);
        self.stats.migrations += 1;
        Some(index)
    }

    fn handle_data(&mut self, now: Time, from: SocketAddr, data: &[u8]) {
        let index = match self.by_addr.get(&from) {
            Some(&index) => index,
            None => match self.try_migrate(now, from, data) {
                Some(index) => index,
                None => {
                    self.stats.datagrams_dropped += 1;
                    netlog!(
                        self.log,
                        Debug,
                        now.as_micros(),
                        "Dropped {Length} byte datagram from {RemoteEndpoint}: {Reason}",
                        Length = data.len(),
                        RemoteEndpoint = from,
                        Reason = "unknown sender"
                    );
                    return;
                }
            },
        };
        let id = self.id_of(index);
        let Manager {
            slots,
            events,
            stats,
            config,
            tx,
            by_addr,
            log,
            ..
        } = self;
        let Some(peer) = slots[index as usize].peer.as_mut() else {
            return;
        };
        let drop = |log: &mut Logger, reason: &str| {
            netlog!(
                log,
                Debug,
                now.as_micros(),
                "Dropped {Length} byte datagram from {RemoteEndpoint}: {Reason}",
                Length = data.len(),
                RemoteEndpoint = from,
                Reason = reason
            );
        };
        if !matches!(peer.state, State::Connected) {
            stats.datagrams_dropped += 1;
            drop(log, "peer not connected");
            return;
        }
        if let Some(p) = &mut peer.path {
            if p.addr == from {
                p.received += data.len();
            }
        }
        let mut r = Reader::new(data);
        let Some(header) = DataHeader::decode(&mut r) else {
            peer.stats.packets_malformed += 1;
            drop(log, "malformed header");
            return;
        };
        if let (Some(p), Some((conn_id, token))) = (peer.path, header.migrate) {
            // A second authenticated packet from the new address proves the client is
            // there, even if the path challenge or its response was lost.
            let authentic = conn_id == peer.conn_id && token == migrate_token(peer.session_key, conn_id, header.seq);
            if p.addr == from && header.seq != p.trigger_seq && authentic {
                peer.path = None;
                if p.old != p.addr {
                    by_addr.remove(&p.old);
                }
                netlog!(
                    log,
                    Info,
                    now.as_micros(),
                    "Path to {RemoteEndpoint} of peer {PeerId} validated",
                    RemoteEndpoint = from,
                    PeerId = peer.id
                );
            }
        }
        let body = r.rest();
        // Validate the whole packet before acting on any of it.
        if MsgIter::new(body, &peer.modes).any(|m| m.is_err()) {
            peer.stats.packets_malformed += 1;
            drop(log, "malformed body");
            return;
        }
        let largest = peer.recv.latest();
        match peer.recv.insert(header.seq) {
            Received::New => {}
            Received::Duplicate => {
                peer.stats.packets_duplicate += 1;
                drop(log, "duplicate");
                return;
            }
            Received::TooOld => {
                peer.stats.packets_duplicate += 1;
                drop(log, "older than the receive window");
                return;
            }
        }
        if largest.is_none_or(|l| seq_newer(header.seq, l)) {
            peer.largest_recv_time = now;
        }
        peer.last_recv = now;
        peer.received_any = true;
        peer.ack_repeat = ACK_REPEAT;
        peer.stats.packets_received += 1;
        peer.stats.bytes_received += data.len() as u64;
        if let Some((ack, bits, delay)) = header.ack {
            process_acks(peer, now, ack, bits, decode_ack_delay(delay), config.max_ack_delay, log);
        }
        let ping_ack_allowed = peer.last_ping_ack.is_none_or(|t| now - t >= config.ping_interval / 4);
        if header.flags & FLAG_PING != 0 && !ping_ack_allowed {
            peer.ack_pending = true;
        } else if header.flags & FLAG_PING != 0 {
            peer.last_ping_ack = Some(now);
            // Answer at once: a sample without ack delay keeps min_rtt honest, which bounds
            // how far reported ack delays may lower the RTT estimate.
            send_ping_ack(peer, now, config.mtu, tx);
        }
        let limits = RecvLimits {
            max_message: config.max_reliable_message,
            max_partial_bytes: config.max_reliable_message.saturating_mul(4),
        };
        let Peer {
            modes,
            channels,
            stats,
            ack_pending,
            ..
        } = peer;
        let mut messages = 0usize;
        let mut current = [false; MAX_CHANNELS];
        for (i, ch) in channels.iter().enumerate() {
            current[i] = ch.mode != DeliveryMode::UnreliableSequenced || ch.sequenced_current(header.seq);
        }
        for msg in MsgIter::new(body, modes) {
            let Ok(msg) = msg else { break };
            messages += 1;
            let c = usize::from(msg.channel);
            let ch = &mut channels[c];
            let mut deliver = |data: &[u8]| {
                let data = events.store(data);
                events.push(EventRec::Message {
                    peer: id,
                    channel: msg.channel,
                    data,
                });
                stats.messages_received += 1;
            };
            let Some(seq) = msg.seq else {
                if current[c] {
                    deliver(msg.data);
                } else {
                    stats.messages_stale += 1;
                }
                continue;
            };
            *ack_pending = true;
            match ch.receive_reliable(seq, msg.frag, msg.data, limits, &mut deliver) {
                RecvResult::Accepted | RecvResult::Fragment | RecvResult::Dropped => {}
                RecvResult::Duplicate => stats.messages_duplicate += 1,
                RecvResult::Stale => stats.messages_stale += 1,
            }
        }
        for (i, ch) in channels.iter_mut().enumerate() {
            if ch.mode == DeliveryMode::UnreliableSequenced && current[i] {
                ch.set_sequenced_latest(header.seq);
            }
        }
        netlog!(
            log,
            Trace,
            now.as_micros(),
            "Received datagram {Seq} of {Length} bytes with {Messages} messages from peer {PeerId}",
            Seq = header.seq,
            Length = data.len(),
            Messages = messages,
            PeerId = peer.id
        );
    }

    /// Runs timers: handshake retransmissions, connection and handshake timeouts.
    pub fn update(&mut self, now: Time) {
        self.now_hint = self.now_hint.max(now);
        let Manager {
            config,
            slots,
            tx,
            scratch,
            by_addr,
            stats,
            log,
            ..
        } = self;
        scratch.clear();
        let t = now.as_micros();
        for (index, slot) in slots.iter_mut().enumerate() {
            let Some(peer) = slot.peer.as_mut() else {
                continue;
            };
            let index = index as u32;
            match &mut peer.state {
                State::Connecting { next_retry, deadline } => {
                    if now >= *deadline {
                        netlog!(
                            log,
                            Warn,
                            t,
                            "No answer from {RemoteEndpoint}: connection attempt timed out",
                            RemoteEndpoint = peer.addr
                        );
                        scratch.push((index, Some(ReasonRec::ConnectTimeout)));
                    } else if now >= *next_retry {
                        netlog!(
                            log,
                            Debug,
                            t,
                            "Retrying connect request to {RemoteEndpoint}",
                            RemoteEndpoint = peer.addr
                        );
                        *next_retry = now + config.connect_retry;
                        let request = Control::ConnectRequest {
                            protocol_id: config.protocol_id,
                            version: WIRE_VERSION,
                            nonce: peer.nonce,
                        };
                        tx.push_control(config.mtu, peer.addr, &request, Some(&mut peer.stats));
                    }
                }
                State::Challenged {
                    timestamp,
                    mac,
                    next_retry,
                    deadline,
                } => {
                    if now >= *deadline {
                        netlog!(
                            log,
                            Warn,
                            t,
                            "Handshake with {RemoteEndpoint} timed out after the challenge",
                            RemoteEndpoint = peer.addr
                        );
                        scratch.push((index, Some(ReasonRec::ConnectTimeout)));
                    } else if now >= *next_retry {
                        netlog!(
                            log,
                            Debug,
                            t,
                            "Retrying challenge response to {RemoteEndpoint}",
                            RemoteEndpoint = peer.addr
                        );
                        *next_retry = now + config.connect_retry;
                        let response = Control::ChallengeResponse {
                            nonce: peer.nonce,
                            timestamp: *timestamp,
                            mac: *mac,
                            data: &peer.connect_data,
                        };
                        tx.push_control(config.mtu, peer.addr, &response, Some(&mut peer.stats));
                    }
                }
                State::Pending { deadline } => {
                    if now >= *deadline {
                        netlog!(
                            log,
                            Warn,
                            t,
                            "Connection request {PeerId} from {RemoteEndpoint} was neither accepted nor rejected in time",
                            PeerId = peer.id,
                            RemoteEndpoint = peer.addr
                        );
                        scratch.push((index, None));
                    }
                }
                State::Connected => {
                    if now - peer.last_recv >= config.timeout {
                        netlog!(
                            log,
                            Warn,
                            t,
                            "Peer {PeerId} at {RemoteEndpoint} timed out after {Silence} ms without packets",
                            PeerId = peer.id,
                            RemoteEndpoint = peer.addr,
                            Silence = now - peer.last_recv
                        );
                        scratch.push((index, Some(ReasonRec::Timeout)));
                    }
                    if let Some(iv) = config.stats_interval {
                        if now - peer.last_stats >= iv {
                            log_peer_stats(log, peer, now, config.mtu);
                        }
                    }
                    if let Some(p) = &mut peer.path {
                        if now < p.deadline && now >= p.next_challenge {
                            p.next_challenge = now + peer.rtt.rto();
                            p.sent += 9;
                            let challenge = Control::PathChallenge { challenge: p.challenge };
                            tx.push_control(config.mtu, p.addr, &challenge, None);
                        }
                    }
                    if let Some(p) = peer.path {
                        if now >= p.deadline {
                            // Not validated: go back to the old address.
                            peer.path = None;
                            peer.addr = p.old;
                            by_addr.remove(&p.addr);
                            stats.failed_path_validations += 1;
                            netlog!(
                                log,
                                Warn,
                                t,
                                "Path to {RemoteEndpoint} of peer {PeerId} not validated: back to {OldEndpoint}",
                                RemoteEndpoint = p.addr,
                                PeerId = peer.id,
                                OldEndpoint = p.old
                            );
                        }
                    }
                }
            }
        }
        for i in 0..self.scratch.len() {
            let (index, reason) = self.scratch[i];
            self.remove_peer(index, reason);
        }
    }

    /// Tells the manager that the local address changed, e.g. after the socket was
    /// recreated. Outgoing connections prove their identity in the next packets so the
    /// server moves them to the new address without a reconnect.
    pub fn local_address_changed(&mut self) {
        for peer in self.slots.iter_mut().filter_map(|s| s.peer.as_mut()) {
            if peer.outgoing && matches!(peer.state, State::Connected) && peer.migrate_from.is_none() {
                peer.migrate_from = Some(peer.next_seq);
                netlog!(
                    self.log,
                    Info,
                    self.now_hint.as_micros(),
                    "Local address changed: peer {PeerId} proves its identity to {RemoteEndpoint}",
                    PeerId = peer.id,
                    RemoteEndpoint = peer.addr
                );
            }
        }
    }

    /// Packs queued messages into DATA packets for every connected peer; also sends
    /// pending acks, pings and keepalives.
    pub fn flush(&mut self, now: Time) {
        self.now_hint = self.now_hint.max(now);
        let Manager {
            config, slots, tx, log, ..
        } = self;
        for slot in slots.iter_mut() {
            if let Some(peer) = slot.peer.as_mut() {
                if !matches!(peer.state, State::Connected) {
                    continue;
                }
                // Unvalidated address: send at most 3x what it sent us.
                let limit = peer
                    .path
                    .map(|p| (AMPLIFICATION_FACTOR * p.received).saturating_sub(p.sent));
                if limit.is_some_and(|l| l < DATA_HEADER_MAX + MIGRATE_LEN) {
                    continue;
                }
                let before = peer.stats.bytes_sent;
                flush_peer(peer, now, config, tx, limit, log);
                if let Some(p) = &mut peer.path {
                    p.sent += (peer.stats.bytes_sent - before) as usize;
                }
            }
        }
    }

    /// Takes the next outgoing datagram, copying it into `buf`, which must hold at least
    /// `Config::mtu` bytes. Returns the destination and the length.
    pub fn poll_transmit(&mut self, buf: &mut [u8]) -> Option<(SocketAddr, usize)> {
        let (addr, start, len) = self.tx.queue.pop_front()?;
        let src = &self.tx.bytes[start as usize..(start + len) as usize];
        let n = src.len().min(buf.len());
        debug_assert_eq!(n, src.len(), "poll_transmit buffer is smaller than the datagram");
        buf[..n].copy_from_slice(&src[..n]);
        Some((addr, n))
    }

    /// Takes the next event. The returned data borrows the manager until the next call.
    pub fn poll_event(&mut self) -> Option<Event<'_>> {
        let rec = self.events.recs.pop_front()?;
        let ev = &self.events;
        Some(match rec {
            EventRec::ConnectRequest { peer, addr, data } => Event::ConnectRequest {
                peer,
                addr,
                data: ev.slice(data),
            },
            EventRec::Connected { peer, addr } => Event::Connected { peer, addr },
            EventRec::Disconnected { peer, reason } => Event::Disconnected {
                peer,
                reason: match reason {
                    ReasonRec::Timeout => DisconnectReason::Timeout,
                    ReasonRec::Remote(s, l) => DisconnectReason::Remote { data: ev.slice((s, l)) },
                    ReasonRec::Local => DisconnectReason::Local,
                    ReasonRec::ConnectTimeout => DisconnectReason::ConnectTimeout,
                    ReasonRec::Rejected(s, l) => DisconnectReason::Rejected { data: ev.slice((s, l)) },
                    ReasonRec::ServerFull => DisconnectReason::ServerFull,
                    ReasonRec::VersionMismatch => DisconnectReason::VersionMismatch,
                    ReasonRec::Reconnected => DisconnectReason::Reconnected,
                },
            },
            EventRec::Message { peer, channel, data } => Event::Message {
                peer,
                channel,
                data: ev.slice(data),
            },
        })
    }

    /// The earliest time at which [`update`](Self::update) or [`flush`](Self::flush)
    /// has timer work to do.
    pub fn next_timeout(&self) -> Option<Time> {
        let mut next: Option<Time> = None;
        let mut consider = |t: Time| next = Some(next.map_or(t, |n| n.min(t)));
        for peer in self.slots.iter().filter_map(|s| s.peer.as_ref()) {
            match peer.state {
                State::Connecting { next_retry, deadline }
                | State::Challenged {
                    next_retry, deadline, ..
                } => {
                    consider(next_retry.min(deadline));
                }
                State::Pending { deadline } => consider(deadline),
                State::Connected => {
                    consider(peer.last_recv + self.config.timeout);
                    consider(peer.last_ping + self.config.ping_interval);
                }
            }
        }
        next
    }

    /// State of a peer, or `None` if it does not exist.
    pub fn peer_state(&self, id: PeerId) -> Option<PeerState> {
        self.peer(id).map(Peer::public_state)
    }

    /// Remote address of a peer.
    pub fn peer_addr(&self, id: PeerId) -> Option<SocketAddr> {
        self.peer(id).map(|p| p.addr)
    }

    /// Smoothed network round-trip time, once measured. The time the remote side held
    /// acks before sending them is excluded, so the value does not depend on its tick rate.
    pub fn peer_rtt(&self, id: PeerId) -> Option<Duration> {
        self.peer(id)?.rtt.srtt()
    }

    /// Smallest raw RTT sample: a lower bound of the network RTT.
    pub fn peer_min_rtt(&self, id: PeerId) -> Option<Duration> {
        self.peer(id)?.rtt.min_rtt()
    }

    /// Mean deviation of the network RTT (jitter estimate).
    pub fn peer_rtt_var(&self, id: PeerId) -> Option<Duration> {
        let p = self.peer(id)?;
        p.rtt.srtt().map(|_| p.rtt.rttvar())
    }

    /// Current retransmission timeout.
    pub fn peer_rto(&self, id: PeerId) -> Option<Duration> {
        Some(self.peer(id)?.rtt.rto())
    }

    /// Latest network RTT sample (ack delay excluded).
    pub fn peer_latest_rtt(&self, id: PeerId) -> Option<Duration> {
        let p = self.peer(id)?;
        p.rtt.srtt().map(|_| p.rtt.latest())
    }

    /// The session key of a connection. For tests that check secrets never reach logs.
    #[doc(hidden)]
    pub fn __session_key(&self, id: PeerId) -> Option<u64> {
        Some(self.peer(id)?.session_key)
    }

    /// Counters of a peer.
    pub fn peer_stats(&self, id: PeerId) -> Option<&PeerStats> {
        self.peer(id).map(|p| &p.stats)
    }

    /// Ids of all peers in any state.
    pub fn peers(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.slots
            .iter()
            .enumerate()
            .filter(|(_, s)| s.peer.is_some())
            .map(|(i, s)| PeerId {
                index: i as u32,
                generation: s.generation,
            })
    }
}

/// Logs the periodic `PeerStats` event and starts a new reporting window.
fn log_peer_stats(log: &mut Logger, peer: &mut Peer, now: Time, mtu: usize) {
    let secs = (now - peer.last_stats).as_secs_f64().max(1e-6);
    let s = &peer.stats;
    let b = &peer.stats_base;
    let sent = s.packets_sent - b.packets_sent;
    let lost = s.packets_lost - b.packets_lost;
    let bytes_out = s.bytes_sent - b.bytes_sent;
    let queued: usize = peer
        .channels
        .iter()
        .map(|c| c.reliable_queue_len() + c.unreliable_queue_len())
        .sum();
    netlog!(
        log,
        Info,
        now.as_micros(),
        "Peer {PeerId}: RTT {RttMs} ms ±{RttVarMs}, loss {LossPercent}%, out {BytesOutPerSec} B/s, in {BytesInPerSec} B/s, fill {FillPercent}%, retransmitted {Retransmitted}, queued {Queued}",
        PeerId = peer.id,
        RttMs = peer.rtt.srtt().unwrap_or_default(),
        RttVarMs = peer.rtt.rttvar(),
        LossPercent = (lost * 1000).checked_div(sent).unwrap_or(0) as f64 / 10.0,
        BytesOutPerSec = (bytes_out as f64 / secs) as u64,
        BytesInPerSec = ((s.bytes_received - b.bytes_received) as f64 / secs) as u64,
        FillPercent = (bytes_out * 100).checked_div(sent * mtu as u64).unwrap_or(0),
        Retransmitted = s.messages_retransmitted - b.messages_retransmitted,
        Queued = queued
    );
    peer.stats_base = peer.stats.clone();
    peer.last_stats = now;
}

/// Packet threshold for loss detection (RFC 9002): a packet is lost once a packet sent
/// this many packets later is acknowledged.
const PACKET_THRESHOLD: u16 = 3;

fn process_acks(
    peer: &mut Peer,
    now: Time,
    ack: u16,
    bits: u16,
    ack_delay: Duration,
    max_ack_delay: Duration,
    log: &mut Logger,
) {
    // An ack for a packet we have not sent yet is bogus.
    let last_sent = peer.next_seq.wrapping_sub(1);
    if peer.stats.packets_sent == 0 || seq_newer(ack, last_sent) {
        return;
    }
    let Peer {
        sent,
        channels,
        stats,
        rtt,
        ..
    } = peer;
    for i in 0..17u16 {
        if i > 0 && bits & (1 << (i - 1)) == 0 {
            continue;
        }
        let seq = ack.wrapping_sub(i);
        let slot = &mut sent[usize::from(seq) % SENT_RING];
        if slot.in_use && slot.seq == seq && !slot.acked {
            slot.acked = true;
            stats.packets_acked += 1;
            if i == 0 {
                rtt.on_ack(now - slot.time, ack_delay, max_ack_delay);
            }
            for &(ch, mseq, unit) in &slot.units {
                channels[usize::from(ch)].on_acked(mseq, unit);
            }
        }
    }
    if peer.largest_acked.is_none_or(|l| seq_newer(ack, l)) {
        peer.largest_acked = Some(ack);
    }
    if let Some(from) = peer.migrate_from {
        // The server accepted a packet sent from our new address: migration is complete.
        if !seq_newer(from, ack) {
            peer.migrate_from = None;
        }
    }
    let Some(largest) = peer.largest_acked else {
        return;
    };
    // Everything sent PACKET_THRESHOLD packets before the largest acked one and still
    // unacknowledged is lost.
    while seq_newer(largest.wrapping_sub(PACKET_THRESHOLD - 1), peer.loss_scan) {
        let seq = peer.loss_scan;
        let slot = &mut peer.sent[usize::from(seq) % SENT_RING];
        if slot.in_use && slot.seq == seq && !slot.acked {
            let pieces = slot.units.len();
            if declare_lost(slot, &mut peer.channels, &mut peer.stats) {
                netlog!(
                    log,
                    Debug,
                    now.as_micros(),
                    "Datagram {Seq} to peer {PeerId} lost with {Pieces} reliable pieces",
                    Seq = seq,
                    PeerId = peer.id,
                    Pieces = pieces
                );
            }
        }
        peer.loss_scan = seq.wrapping_add(1);
    }
}

/// Returns `true` if the packet carried reliable data (and so counts as a loss).
fn declare_lost(slot: &mut SentPacket, channels: &mut [Channel], stats: &mut PeerStats) -> bool {
    slot.in_use = false;
    if slot.units.is_empty() {
        // Packets without reliable data are not acked unless the remote side happens to send.
        return false;
    }
    stats.packets_lost += 1;
    for &(ch, mseq, unit) in &slot.units {
        channels[usize::from(ch)].on_lost(mseq, unit, slot.seq);
    }
    true
}

/// Takes the ring slot for packet `seq`. A packet still unacknowledged after the ring wrapped
/// around is lost.
fn record_packet<'a>(
    sent: &'a mut [SentPacket],
    channels: &mut [Channel],
    stats: &mut PeerStats,
    seq: u16,
    now: Time,
) -> &'a mut SentPacket {
    let slot = &mut sent[usize::from(seq) % SENT_RING];
    if slot.in_use && !slot.acked {
        declare_lost(slot, channels, stats);
    }
    slot.seq = seq;
    slot.time = now;
    slot.in_use = true;
    slot.acked = false;
    slot.units.clear();
    slot
}

/// Migration fields for packet `seq` while a client is migrating.
fn migrate_fields(peer: &Peer, seq: u16) -> Option<(u32, u64)> {
    (peer.outgoing && peer.migrate_from.is_some())
        .then(|| (peer.conn_id, migrate_token(peer.session_key, peer.conn_id, seq)))
}

fn send_ping_ack(peer: &mut Peer, now: Time, mtu: usize, tx: &mut TxQueue) {
    let Some((ack, bits)) = peer.recv.ack() else {
        return;
    };
    let seq = peer.next_seq;
    let header = DataHeader {
        flags: 0,
        seq,
        ack: Some((ack, bits, encode_ack_delay(now - peer.largest_recv_time))),
        migrate: migrate_fields(peer, seq),
    };
    let start = tx.reserve(mtu);
    let mut w = Writer::new(&mut tx.bytes[start..start + mtu]);
    if header.encode(&mut w).is_err() {
        tx.bytes.truncate(start);
        return;
    }
    let len = w.pos();
    tx.commit(peer.addr, start, len);
    record_packet(&mut peer.sent, &mut peer.channels, &mut peer.stats, seq, now);
    peer.next_seq = seq.wrapping_add(1);
    peer.stats.packets_sent += 1;
    peer.stats.bytes_sent += len as u64;
    peer.stats.ping_acks_sent += 1;
}

/// A piece selected for this flush and the packet it was placed in.
#[derive(Debug, Clone, Copy)]
struct Sel {
    cand: u32,
    start: u32,
    end: u32,
    packet: u32,
    /// Write order inside a packet: block rank, seq offset, start, selection index.
    key: (u8, u16, u32, u32),
}

/// A packet being planned.
#[derive(Debug, Clone, Copy)]
struct Plan {
    remaining: usize,
    counts: [u16; MAX_CHANNELS],
    /// Channel of an item that fits only without its length prefix and must come last.
    tail_channel: Option<u8>,
}

/// Scratch space of the scheduler, kept per peer so that flushing does not allocate.
#[derive(Debug, Default)]
struct Scheduler {
    sel: Vec<Sel>,
    placed: Vec<Sel>,
    plans: Vec<Plan>,
    /// `(remaining, plan index)` of plans with free space, sorted ascending.
    free: Vec<(usize, u32)>,
    /// Packet seq of each planned packet.
    seqs: Vec<u16>,
}

/// Pieces smaller than this are not cut from a fragmented message unless they finish it.
const MIN_PIECE: usize = 64;
/// Upper bound of a reliable tag varint: deltas within the window need at most 2 bytes.
const TAG_MAX: usize = varint_len(((crate::channel::RELIABLE_WINDOW as u64) << 1) | 1);

/// Extra bytes a block needs when it grows from `count` to `count + 1` items.
fn block_delta(count: usize, reliable: bool) -> usize {
    if count == 0 {
        1 + if reliable { 2 } else { 0 }
    } else {
        BlockHeader::encoded_len(count + 1) - BlockHeader::encoded_len(count)
    }
}

/// Upper bound of `block_delta` for any count.
fn block_delta_max(reliable: bool) -> usize {
    if reliable { 3 } else { 1 }
}

/// Bytes of an item without its block overhead: length prefix, reliable tag, fragment fields
/// and payload.
fn item_cost(piece: &Piece<'_>, reliable: bool, len: usize) -> usize {
    let mut cost = varint_len(len as u64) + len;
    if reliable {
        cost += TAG_MAX;
    }
    if let Some((total, offset)) = piece.frag {
        cost += varint_len(u64::from(total)) + varint_len(u64::from(offset));
    }
    cost
}

/// Largest piece of a fragment that fits into `space` bytes of item cost, or `None`.
fn piece_len(piece: &Piece<'_>, reliable: bool, want: usize, space: usize) -> Option<usize> {
    let fixed = item_cost(piece, reliable, 0) - 1; // without the 1-byte length of 0
    let mut p = want.min(space.checked_sub(fixed)?);
    while p > 0 && fixed + varint_len(p as u64) + p > space {
        p -= 1;
    }
    (p == want || p >= MIN_PIECE).then_some(p)
}

impl Scheduler {
    fn clear(&mut self) {
        self.sel.clear();
        self.placed.clear();
        self.plans.clear();
        self.free.clear();
        self.seqs.clear();
    }

    fn new_plan(&mut self, capacity: usize) -> u32 {
        self.plans.push(Plan {
            remaining: capacity,
            counts: [0; MAX_CHANNELS],
            tail_channel: None,
        });
        (self.plans.len() - 1) as u32
    }

    fn set_free(&mut self, plan: u32, old: Option<usize>) {
        if let Some(r) = old {
            if let Ok(i) = self.free.binary_search(&(r, plan)) {
                self.free.remove(i);
            }
        }
        let r = self.plans[plan as usize].remaining;
        if r != 0 {
            let i = self.free.binary_search(&(r, plan)).unwrap_or_else(|i| i);
            self.free.insert(i, (r, plan));
        }
    }

    /// Accounts `cost` plus the block overhead of `channel` in `plan`.
    fn place(&mut self, plan: u32, channel: u8, reliable: bool, cost: usize, sel: Sel) {
        let p = &mut self.plans[plan as usize];
        let old = p.remaining;
        let c = &mut p.counts[usize::from(channel)];
        p.remaining -= block_delta(usize::from(*c), reliable) + cost;
        *c += 1;
        self.placed.push(Sel { packet: plan, ..sel });
        self.set_free(plan, Some(old));
    }
}

/// Flushes one peer. `strict_limit` caps the bytes sent in this flush exactly (used for the
/// anti-amplification limit); the bandwidth budget may be exceeded by one item.
fn flush_peer(
    peer: &mut Peer,
    now: Time,
    config: &Config,
    tx: &mut TxQueue,
    strict_limit: Option<usize>,
    log: &mut Logger,
) {
    let t = now.as_micros();
    if peer.outgoing && peer.migrate_from.is_none() && now - peer.last_recv >= config.migration_probe {
        // Silence from the server may mean our address changed: include proof of identity.
        peer.migrate_from = Some(peer.next_seq);
        netlog!(
            log,
            Info,
            t,
            "No packets from {RemoteEndpoint} for {Silence} ms: peer {PeerId} proves its identity in case our address changed",
            RemoteEndpoint = peer.addr,
            Silence = now - peer.last_recv,
            PeerId = peer.id
        );
    }
    let peer_id = peer.id;
    let migrating = peer.outgoing && peer.migrate_from.is_some();
    let (conn_id, session_key) = (peer.conn_id, peer.session_key);
    let ping_due = now - peer.last_ping >= config.ping_interval;
    let rto = peer.rtt.rto();
    if let Some(rate) = config.bandwidth {
        let burst = i64::from(rate / 10).max(2 * config.mtu as i64);
        let gained = (now - peer.last_refill).as_micros() * u128::from(rate) / 1_000_000;
        peer.tokens = (peer.tokens + i64::try_from(gained).unwrap_or(i64::MAX)).min(burst);
        peer.last_refill = now;
    }
    let Peer {
        channels,
        cands,
        sched,
        sent,
        stats,
        recv,
        addr,
        ..
    } = peer;
    cands.clear();
    sched.clear();
    let mut cstats = CollectStats::default();
    for (i, ch) in channels.iter_mut().enumerate() {
        ch.collect(
            i as u8,
            now,
            rto,
            config.max_rto,
            config.starvation_time,
            cands,
            &mut cstats,
        );
    }
    stats.messages_expired += cstats.expired;
    if cstats.expired > 0 {
        netlog!(
            log,
            Debug,
            t,
            "Dropped {Count} unreliable messages to peer {PeerId}: TTL expired before they could be sent",
            Count = cstats.expired,
            PeerId = peer_id
        );
    }
    let ack = if peer.received_any && (peer.ack_pending || peer.ack_repeat > 0) {
        recv.ack()
            .map(|(ack, bits)| (ack, bits, encode_ack_delay(now - peer.largest_recv_time)))
    } else {
        None
    };
    let header_len = if ack.is_some() { DATA_HEADER_MAX } else { 3 } + if migrating { MIGRATE_LEN } else { 0 };
    let mtu = config.mtu;
    let capacity = mtu - header_len;

    // ---- phase 1: what goes out now, by effective priority, within the budget ----
    // Highest effective priority first; among equals, the oldest message first.
    cands.sort_unstable_by_key(|c| (core::cmp::Reverse(c.priority), c.enqueued, c.channel, c.order, c.start));
    let tokens = config
        .bandwidth
        .map(|_| usize::try_from(peer.tokens.max(0)).unwrap_or(0));
    let budget = match (tokens, strict_limit) {
        (Some(t), Some(l)) => Some(t.min(l.saturating_sub(header_len))),
        (t, l) => t.or(l.map(|l| l.saturating_sub(header_len))),
    };
    let mut used = 0usize;
    for (i, c) in cands.iter().enumerate() {
        let ch = &channels[usize::from(c.channel)];
        let reliable = ch.mode.is_reliable();
        let piece = ch.piece(c.kind, c.start, c.end);
        let len = (c.end - c.start) as usize;
        let mut end = c.end;
        let mut cost = item_cost(&piece, reliable, len) + block_delta_max(reliable);
        if let Some(b) = budget {
            if used >= b {
                break;
            }
            if used + cost > b && !c.splittable && strict_limit.is_some() {
                continue;
            }
            if used + cost > b && c.splittable {
                let space = b - used - block_delta_max(reliable);
                let Some(p) = piece_len(&piece, reliable, len, space) else {
                    continue;
                };
                end = c.start + p as u32;
                cost = item_cost(&piece, reliable, p) + block_delta_max(reliable);
            }
        }
        // Each full packet also costs a header.
        used += cost + cost * header_len / capacity;
        let seq_offset = if reliable { ch.seq_offset(piece.seq) } else { 0 };
        let idx = sched.sel.len() as u32;
        sched.sel.push(Sel {
            cand: i as u32,
            start: c.start,
            end,
            packet: 0,
            key: (c.channel, seq_offset, c.start, idx),
        });
    }

    let held = cands.len() - sched.sel.len();
    if held > 0 {
        netlog!(
            log,
            Debug,
            t,
            "Budget held back {Held} of {Candidates} pieces for peer {PeerId}",
            Held = held,
            Candidates = cands.len(),
            PeerId = peer_id
        );
    }

    // ---- phase 2: best fit decreasing into packets; fragments fill the gaps ----
    let sel_len = sched.sel.len();
    {
        let cands = &*cands;
        let channels = &*channels;
        let cost_of = |s: &Sel| {
            let c = &cands[s.cand as usize];
            let ch = &channels[usize::from(c.channel)];
            item_cost(
                &ch.piece(c.kind, s.start, s.end),
                ch.mode.is_reliable(),
                (s.end - s.start) as usize,
            )
        };
        sched.sel.sort_unstable_by_key(|s| {
            let splittable = cands[s.cand as usize].splittable;
            (splittable, core::cmp::Reverse(cost_of(s)), s.key.3)
        });
        for i in 0..sel_len {
            let s = sched.sel[i];
            let c = cands[s.cand as usize];
            let ch = &channels[usize::from(c.channel)];
            let reliable = ch.mode.is_reliable();
            if !c.splittable {
                let cost = cost_of(&s);
                let need = cost + block_delta_max(reliable);
                let fi = sched.free.partition_point(|&(r, _)| r < need);
                if let Some(&(_, plan)) = sched.free.get(fi) {
                    sched.place(plan, c.channel, reliable, cost, s);
                } else if need <= capacity {
                    let plan = sched.new_plan(capacity);
                    sched.place(plan, c.channel, reliable, cost, s);
                } else {
                    // Fits only as the last item of an empty packet, without its length.
                    let plan = sched.new_plan(capacity);
                    sched.plans[plan as usize].remaining = 0;
                    sched.plans[plan as usize].tail_channel = Some(c.channel);
                    sched.placed.push(Sel { packet: plan, ..s });
                }
                continue;
            }
            // A fragment range: cut pieces into the largest gaps, then into new packets.
            let mut start = s.start;
            while start < s.end {
                let want = (s.end - start) as usize;
                let piece = ch.piece(c.kind, start, s.end);
                let (plan, space) = match sched.free.last() {
                    Some(&(r, plan)) if r > block_delta_max(reliable) => (plan, r - block_delta_max(reliable)),
                    _ => (u32::MAX, 0),
                };
                let p = (plan != u32::MAX)
                    .then(|| piece_len(&piece, reliable, want, space))
                    .flatten();
                let (plan, p) = match p {
                    Some(p) => (plan, p),
                    None => {
                        let plan = sched.new_plan(capacity);
                        let space = capacity - block_delta_max(reliable);
                        let p = piece_len(&piece, reliable, want, space).expect("a piece fits an empty packet");
                        (plan, p)
                    }
                };
                let end = start + p as u32;
                let sub = ch.piece(c.kind, start, end);
                let cost = item_cost(&sub, reliable, p);
                let key = (s.key.0, s.key.1, start, s.key.3);
                sched.place(plan, c.channel, reliable, cost, Sel { start, end, key, ..s });
                start = end;
            }
        }
    }
    let packets = sched.plans.len();
    if packets == 0 && !ping_due && !peer.ack_pending {
        for c in channels.iter_mut() {
            stats.messages_stale += c.after_flush();
        }
        return;
    }

    // ---- write packets ----
    for (pi, p) in sched.plans.iter().enumerate() {
        if let Some(tc) = p.tail_channel {
            for s in sched.placed.iter_mut() {
                if s.packet as usize == pi && cands[s.cand as usize].channel == tc {
                    s.key.0 = u8::MAX;
                }
            }
        }
    }
    sched.placed.sort_unstable_by_key(|s| (s.packet, s.key));
    let mut i = 0;
    for packet in 0..packets.max(1) {
        let seq = peer.next_seq;
        sched.seqs.push(seq);
        let flags = if packet == 0 && ping_due { FLAG_PING } else { 0 };
        let start = tx.reserve(mtu);
        let mut w = Writer::new(&mut tx.bytes[start..start + mtu]);
        let record = record_packet(sent, channels, stats, seq, now);
        let first_item = i;
        let ok = (|| {
            let migrate = migrating.then(|| (conn_id, migrate_token(session_key, conn_id, seq)));
            DataHeader {
                flags,
                seq,
                ack,
                migrate,
            }
            .encode(&mut w)?;
            while i < sched.placed.len() && sched.placed[i].packet as usize == packet {
                let c = cands[sched.placed[i].cand as usize].channel;
                let ch = &channels[usize::from(c)];
                let reliable = ch.mode.is_reliable();
                let count = sched.placed[i..]
                    .iter()
                    .take_while(|s| s.packet as usize == packet && cands[s.cand as usize].channel == c)
                    .count();
                let last_block = sched.placed.get(i + count).is_none_or(|s| s.packet as usize != packet);
                BlockHeader {
                    channel: c,
                    tail: last_block,
                    count,
                }
                .encode(&mut w)?;
                let first = sched.placed[i];
                let base = ch.piece(cands[first.cand as usize].kind, first.start, first.end).seq;
                if reliable {
                    w.u16(base)?;
                }
                let mut prev = base;
                for (j, s) in sched.placed[i..i + count].iter().enumerate() {
                    let p = ch.piece(cands[s.cand as usize].kind, s.start, s.end);
                    if !(last_block && j == count - 1) {
                        w.varint(p.data.len() as u64)?;
                    }
                    if reliable {
                        let delta = u64::from(p.seq.wrapping_sub(prev));
                        w.varint((delta << 1) | u64::from(p.frag.is_some()))?;
                        if let Some((total, offset)) = p.frag {
                            w.varint(u64::from(total))?;
                            w.varint(u64::from(offset))?;
                        }
                        prev = p.seq;
                        record.units.push((c, p.seq, s.start));
                    }
                    w.bytes(p.data)?;
                }
                i += count;
            }
            Ok::<(), crate::wire::Full>(())
        })();
        debug_assert!(ok.is_ok(), "packet planner overflowed the buffer");
        let len = w.pos();
        tx.commit(*addr, start, len);
        peer.next_seq = seq.wrapping_add(1);
        peer.tokens -= len as i64;
        let items = (i - first_item) as u64;
        netlog!(
            log,
            Trace,
            t,
            "Sent datagram {Seq} of {Length} bytes with {Pieces} pieces to peer {PeerId}",
            Seq = seq,
            Length = len,
            Pieces = items,
            PeerId = peer_id
        );
        stats.packets_sent += 1;
        stats.bytes_sent += len as u64;
        stats.messages_sent += items;
        if items == 0 && flags & FLAG_PING == 0 {
            stats.standalone_acks_sent += 1;
        }
    }

    // ---- record transmissions, per message in offset order ----
    sched.placed.sort_unstable_by_key(|s| (s.cand, s.start));
    for s in &sched.placed {
        let c = cands[s.cand as usize];
        let seq = sched.seqs[s.packet as usize];
        if channels[usize::from(c.channel)].mark_sent(c.kind, s.start, s.end, now, seq) {
            stats.messages_retransmitted += 1;
            netlog!(
                log,
                Debug,
                t,
                "Retransmitted bytes {Start}..{End} of message {MessageSeq} on channel {Channel} to peer {PeerId} in datagram {Seq}",
                Start = s.start,
                End = s.end,
                MessageSeq = channels[usize::from(c.channel)].piece(c.kind, s.start, s.end).seq,
                Channel = c.channel,
                PeerId = peer_id,
                Seq = seq
            );
        }
    }
    if ping_due {
        peer.last_ping = now;
    }
    peer.last_send = now;
    peer.ack_pending = false;
    if ack.is_some() {
        peer.ack_repeat = peer.ack_repeat.saturating_sub(1);
    }
    for c in channels.iter_mut() {
        stats.messages_stale += c.after_flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ChannelConfig;

    #[test]
    fn migrate_token_is_siphash_of_seq_with_session_key_and_conn_id() {
        let mut h = SipHasher::new([7, 9 ^ 0x6d69_6772_6174_6521]);
        h.write(&5u16.to_le_bytes());
        assert_eq!(migrate_token(7, 9, 5), h.finish());
        assert_ne!(migrate_token(7, 9, 5), migrate_token(7, 9, 6));
        assert_ne!(migrate_token(7, 9, 5), migrate_token(8, 9, 5));
        assert_ne!(migrate_token(7, 9, 5), migrate_token(7, 10, 5));
    }

    #[test]
    fn logger_without_sink_is_disabled() {
        let l = Logger::default();
        assert!(!l.enabled(Level::Error));
    }

    #[test]
    fn malformed_message_blocks_are_rejected() {
        let modes = [DeliveryMode::Unreliable, DeliveryMode::ReliableOrdered];
        let valid = |body: &[u8]| !MsgIter::new(body, &modes).any(|m| m.is_err());
        let count = |body: &[u8]| MsgIter::new(body, &modes).filter(|m| m.is_ok()).count();
        // Unreliable block, 2 messages: len 1 "a", tail "bc".
        assert!(valid(&[0x50, 1, b'a', b'b', b'c']));
        assert_eq!(count(&[0x50, 1, b'a', b'b', b'c']), 2);
        assert!(!valid(&[0x40, 5, b'a']), "length past the end");
        assert!(!valid(&[0x2f, 0]), "unknown channel");
        assert!(!valid(&[0x41]), "missing length");
        // Reliable block (channel 1): base seq 0, tag 0, len 1.
        assert!(valid(&[0x21, 0, 0, 1, 0, b'x']));
        // Fragment: tag 1 (delta 0, frag), total 4, offset 2, len 2.
        assert!(valid(&[0x21, 0, 0, 2, 1, 4, 2, b'x', b'y']));
        assert!(!valid(&[0x21, 0, 0, 2, 1, 4, 3, b'x', b'y']), "offset + len past total");
        assert!(!valid(&[0x21, 0, 0, 2, 1, 4, 5, b'x', b'y']), "offset past total");
        assert!(!valid(&[0x21, 0, 0, 1, 0x80, 0x80, 0x04, b'x']), "delta >= 0x8000");
        assert!(valid(&[0x21, 0, 0, 1, 0xfe, 0xff, 0x01, b'x']), "delta 0x7fff");
        // total > MAX_FRAGMENTED_TOTAL (2^24 + 1 as varint).
        assert!(!valid(&[0x21, 0, 0, 1, 1, 0x81, 0x80, 0x80, 0x08, 0, b'x']));
        assert!(
            valid(&[0x21, 0, 0, 1, 1, 0x80, 0x80, 0x80, 0x08, 0, b'x']),
            "total = 2^24"
        );
        // A failing message ends the iteration with exactly one error.
        let errs = MsgIter::new(&[0x40, 5, b'a'], &modes).filter(|m| m.is_err()).count();
        assert_eq!(errs, 1);
    }

    // ---- Forged control packets: each must be dropped (and counted) without effect. ----

    const S: SocketAddr = SocketAddr::new(core::net::IpAddr::V4(core::net::Ipv4Addr::new(10, 0, 0, 1)), 9000);
    const C: SocketAddr = SocketAddr::new(core::net::IpAddr::V4(core::net::Ipv4Addr::new(10, 0, 0, 2)), 5000);
    const C2: SocketAddr = SocketAddr::new(core::net::IpAddr::V4(core::net::Ipv4Addr::new(10, 0, 0, 2)), 5001);

    fn drain(m: &mut Manager) -> Vec<Vec<u8>> {
        let mut buf = [0u8; 2048];
        let mut out = Vec::new();
        while let Some((_, n)) = m.poll_transmit(&mut buf) {
            out.push(buf[..n].to_vec());
        }
        out
    }

    fn enc(c: &Control<'_>) -> Vec<u8> {
        let mut b = [0u8; 1500];
        let n = c.encode(&mut b).unwrap();
        b[..n].to_vec()
    }

    /// Whether `m` dropped `pkt` (counted in `datagrams_dropped`).
    fn dropped(m: &mut Manager, now: Time, from: SocketAddr, pkt: &[u8]) -> bool {
        let d = m.stats().datagrams_dropped;
        m.handle_datagram(now, from, pkt);
        m.stats().datagrams_dropped == d + 1
    }

    fn deliver(m: &mut Manager, now: Time, from: SocketAddr, pkts: &[Vec<u8>]) {
        for p in pkts {
            assert!(!dropped(m, now, from, p), "valid packet type {} dropped", p[0] & 0x0f);
        }
    }

    fn server() -> Manager {
        let config = Config {
            accept_incoming: true,
            ..Config::default()
        };
        Manager::new(config, [1; 32], Time::from_micros(0)).unwrap()
    }

    fn client() -> Manager {
        Manager::new(Config::default(), [2; 32], Time::from_micros(0)).unwrap()
    }

    fn accept_all(m: &mut Manager, now: Time) {
        let mut pending = Vec::new();
        while let Some(e) = m.poll_event() {
            if let Event::ConnectRequest { peer, .. } = e {
                pending.push(peer);
            }
        }
        for p in pending {
            m.accept(now, p).unwrap();
        }
    }

    #[test]
    fn forged_handshake_packets_are_dropped() {
        // Late start: a cookie age computed as `now + timestamp` would be expired.
        let t = Time::from_millis(30_000);
        let (mut s, mut c) = (server(), client());
        assert!(dropped(&mut s, t, C, &[]), "empty datagram");
        let mtu = s.config().mtu;
        assert!(dropped(&mut s, t, C, &vec![0u8; mtu + 1]), "oversized datagram");
        assert_eq!(s.stats().datagrams_received, 2);
        assert_eq!(s.stats().datagrams_dropped, 2);

        let id = c.connect(t, S, b"hi").unwrap();
        c.flush(t);
        let req = drain(&mut c);
        let Some(Control::ConnectRequest { nonce, .. }) = Control::decode(&req[0]) else {
            panic!("no CONNECT_REQUEST");
        };
        // ACCEPT and REJECT before the challenge, with the right nonce.
        let accept = |nonce| {
            enc(&Control::Accept {
                nonce,
                conn_id: 1,
                session_key: 1,
            })
        };
        assert!(dropped(&mut c, t, S, &accept(nonce)), "ACCEPT while connecting");
        assert!(dropped(&mut c, t, C2, &accept(nonce)), "ACCEPT from an unknown address");
        deliver(&mut s, t, C, &req);
        let chal = drain(&mut s);
        let Some(Control::Challenge { timestamp, mac, .. }) = Control::decode(&chal[0]) else {
            panic!("no CHALLENGE");
        };
        let forged = enc(&Control::Challenge {
            nonce: nonce ^ 1,
            timestamp,
            mac,
        });
        assert!(dropped(&mut c, t, S, &forged), "CHALLENGE with a wrong nonce");
        assert!(dropped(&mut s, t, C, &chal[0]), "CHALLENGE to the server");
        deliver(&mut c, t, S, &chal);
        assert!(dropped(&mut c, t, S, &accept(nonce ^ 1)), "ACCEPT with a wrong nonce");
        let resp = drain(&mut c);

        // CHALLENGE_RESPONSE: the cookie does not cover the data, the size limit must hold.
        let max = s.config().max_connect_data;
        let big = vec![0u8; max + 1];
        let response = |data| {
            enc(&Control::ChallengeResponse {
                nonce,
                timestamp,
                mac,
                data,
            })
        };
        assert!(dropped(&mut s, t, C, &response(&big)), "connect data over the limit");
        assert_eq!(s.stats().invalid_cookies, 0);
        assert!(dropped(&mut s, t, C2, &resp[0]), "cookie bound to another address");
        assert_eq!(s.stats().invalid_cookies, 1);
        assert!(
            !dropped(&mut s, t, C, &response(&big[..max])),
            "connect data at the limit"
        );
        accept_all(&mut s, t);
        let acc = drain(&mut s);
        let Some(Control::Accept { conn_id, .. }) = Control::decode(&acc[0]) else {
            panic!("no ACCEPT");
        };
        deliver(&mut c, t, S, &acc);
        assert_eq!(c.peer_state(id), Some(PeerState::Connected));

        // Connected: handshake packets and wrong-role packets are ignored.
        let reject = enc(&Control::Reject {
            nonce,
            reason: REJECT_BY_APP,
            data: &[],
        });
        assert!(dropped(&mut c, t, S, &reject), "REJECT after connecting");
        assert!(dropped(&mut c, t, S, &acc[0]), "repeated ACCEPT");
        assert!(dropped(&mut c, t, S, &chal[0]), "CHALLENGE after connecting");
        assert!(
            dropped(&mut s, t, C, &enc(&Control::WhoAreYou)),
            "WHO_ARE_YOU to the server"
        );
        let pc = enc(&Control::PathChallenge { challenge: 5 });
        assert!(dropped(&mut s, t, C, &pc), "PATH_CHALLENGE to the server");
        assert!(
            dropped(&mut s, t, C, &enc(&Control::PathResponse { challenge: 5 })),
            "unsolicited PATH_RESPONSE"
        );
        assert!(!dropped(&mut c, t, S, &pc));
        assert_eq!(
            Control::decode(&drain(&mut c)[0]),
            Some(Control::PathResponse { challenge: 5 })
        );
        let disc = |conn_id| enc(&Control::Disconnect { conn_id, data: b"bye" });
        assert!(
            dropped(&mut c, t, S, &disc(conn_id ^ 1)),
            "DISCONNECT with a wrong conn_id"
        );
        assert_eq!(c.peer_state(id), Some(PeerState::Connected));
        assert!(!dropped(&mut c, t, S, &disc(conn_id)));
        assert_eq!(c.peer_state(id), None);
        assert_eq!(c.stats().datagrams_received, c.stats().datagrams_dropped + 4);
    }

    #[test]
    fn reject_reasons_are_reported() {
        let t = Time::from_millis(1);
        for (code, want) in [
            (REJECT_SERVER_FULL, "ServerFull"),
            (REJECT_VERSION_MISMATCH, "VersionMismatch"),
            (REJECT_BY_APP, "Rejected"),
        ] {
            let mut c = client();
            c.connect(t, S, &[]).unwrap();
            c.flush(t);
            let Some(Control::ConnectRequest { nonce, .. }) = Control::decode(&drain(&mut c)[0]) else {
                panic!("no CONNECT_REQUEST");
            };
            let reject = |nonce| {
                enc(&Control::Reject {
                    nonce,
                    reason: code,
                    data: b"no",
                })
            };
            assert!(dropped(&mut c, t, S, &reject(nonce ^ 1)), "REJECT with a wrong nonce");
            assert!(!dropped(&mut c, t, S, &reject(nonce)));
            let reason = loop {
                match c.poll_event() {
                    Some(Event::Disconnected { reason, .. }) => break alloc::format!("{reason:?}"),
                    Some(_) => {}
                    None => panic!("no Disconnected"),
                }
            };
            assert!(reason.starts_with(want), "{reason} for code {code}");
        }
    }

    /// Runs `rounds` exchanges between a client seen at `seen` and the server.
    fn exchange(s: &mut Manager, c: &mut Manager, seen: SocketAddr, t: &mut Time, rounds: usize) {
        for _ in 0..rounds {
            *t = *t + Duration::from_millis(10);
            c.update(*t);
            c.flush(*t);
            for p in drain(c) {
                s.handle_datagram(*t, seen, &p);
            }
            accept_all(s, *t);
            s.update(*t);
            s.flush(*t);
            for p in drain(s) {
                c.handle_datagram(*t, S, &p);
            }
            while c.poll_event().is_some() {}
        }
    }

    #[test]
    fn migration_counts_no_valid_packet_as_dropped() {
        let mut t = Time::from_millis(1);
        let (mut s, mut c) = (server(), client());
        let id = c.connect(t, S, &[]).unwrap();
        exchange(&mut s, &mut c, C, &mut t, 5);
        assert_eq!(c.peer_state(id), Some(PeerState::Connected));
        assert_eq!(s.stats().datagrams_dropped, 0);
        assert_eq!(s.by_conn.len(), 1);
        // NAT rebinding: the first packet from C2 gets WHO_ARE_YOU, the rest migrate.
        for _ in 0..10 {
            c.send(id, 0, b"x").unwrap();
            exchange(&mut s, &mut c, C2, &mut t, 1);
        }
        assert_eq!(s.stats().migrations, 1);
        assert_eq!(s.stats().failed_path_validations, 0);
        let server_peer = s.peers().next().unwrap();
        assert_eq!(s.peer_addr(server_peer), Some(C2));
        let dropped_by_server = s.stats().datagrams_dropped;
        assert!(
            dropped_by_server <= 1,
            "only the packet answered by WHO_ARE_YOU: {dropped_by_server}"
        );
        assert_eq!(s.stats().who_are_you_sent, dropped_by_server);
        assert!(!s.by_addr.contains_key(&C));
        // Disconnect frees every index.
        s.disconnect(server_peer, &[]).unwrap();
        assert!(s.by_conn.is_empty());
        assert!(s.by_addr.is_empty());
    }

    #[test]
    fn queued_messages_counts_per_channel() {
        let mut t = Time::from_millis(1);
        let config = Config {
            channels: vec![
                ChannelConfig::new(DeliveryMode::Unreliable),
                ChannelConfig::new(DeliveryMode::ReliableOrdered),
            ],
            ..Config::default()
        };
        let mut s = Manager::new(
            Config {
                accept_incoming: true,
                ..config.clone()
            },
            [1; 32],
            t,
        )
        .unwrap();
        let mut c = Manager::new(config, [2; 32], t).unwrap();
        let id = c.connect(t, S, &[]).unwrap();
        exchange(&mut s, &mut c, C, &mut t, 5);
        for _ in 0..2 {
            c.send(id, 0, b"u").unwrap();
        }
        for _ in 0..3 {
            c.send(id, 1, b"r").unwrap();
        }
        assert_eq!(c.queued_messages(id, 0), Some(2));
        assert_eq!(c.queued_messages(id, 1), Some(3));
        assert_eq!(c.queued_messages(id, 2), None);
        c.flush(t);
        let sent = drain(&mut c);
        assert_eq!(c.queued_messages(id, 0), Some(0));
        assert_eq!(c.queued_messages(id, 1), Some(3), "reliable until acknowledged");
        deliver(&mut s, t, C, &sent);
        // A datagram of exactly the MTU is accepted: seq 1000, one unreliable tail message.
        let mtu = s.config().mtu;
        let mut full = vec![TYPE_DATA, 0xe8, 0x03, 0x30];
        full.resize(mtu, 7);
        deliver(&mut s, t, C, &[full]);
        let mut sizes = Vec::new();
        while let Some(e) = s.poll_event() {
            if let Event::Message { data, .. } = e {
                sizes.push(data.len());
            }
        }
        assert_eq!(sizes, [1, 1, 1, 1, 1, mtu - 4]);
    }

    fn pair(channels: &[DeliveryMode], tweak: impl Fn(&mut Config)) -> (Manager, Manager, PeerId, PeerId, Time) {
        let mut t = Time::from_millis(1);
        let mut config = Config {
            channels: channels.iter().map(|&m| ChannelConfig::new(m)).collect(),
            ..Config::default()
        };
        tweak(&mut config);
        let mut s = Manager::new(
            Config {
                accept_incoming: true,
                ..config.clone()
            },
            [1; 32],
            t,
        )
        .unwrap();
        let mut c = Manager::new(config, [2; 32], t).unwrap();
        let id = c.connect(t, S, &[]).unwrap();
        exchange(&mut s, &mut c, C, &mut t, 5);
        assert_eq!(c.peer_state(id), Some(PeerState::Connected));
        let sp = s.peers().next().unwrap();
        (s, c, id, sp, t)
    }

    fn with_seq(pkt: &[u8], seq: u16) -> Vec<u8> {
        let mut p = pkt.to_vec();
        p[1..3].copy_from_slice(&seq.to_le_bytes());
        p
    }

    fn seq_of(pkt: &[u8]) -> u16 {
        u16::from_le_bytes([pkt[1], pkt[2]])
    }

    #[test]
    fn peer_stats_count_every_kind_of_packet_and_message() {
        use DeliveryMode::*;
        let (mut s, mut c, id, sp, t) = pair(&[Unreliable, ReliableOrdered, UnreliableSequenced], |_| {});
        let base = s.peer_stats(sp).unwrap().clone();
        let mut want = base.clone();
        let check = |s: &Manager, want: &PeerStats| assert_eq!(s.peer_stats(sp).unwrap(), want);
        let packet = |c: &mut Manager, ch: u8, msgs: &[&[u8]]| {
            for m in msgs {
                c.send(id, ch, m).unwrap();
            }
            c.flush(t);
            let mut p = drain(c);
            assert_eq!(p.len(), 1);
            p.pop().unwrap()
        };
        let d0 = s.stats().datagrams_dropped;

        let p1 = packet(&mut c, 0, &[b"a", b"b", b"c"]);
        s.handle_datagram(t, C, &p1);
        want.packets_received += 1;
        want.bytes_received += p1.len() as u64;
        want.messages_received += 3;
        check(&s, &want);
        s.handle_datagram(t, C, &p1);
        want.packets_duplicate += 1;
        check(&s, &want);
        s.handle_datagram(t, C, &with_seq(&p1, seq_of(&p1).wrapping_sub(1000)));
        want.packets_duplicate += 1;
        check(&s, &want);

        s.handle_datagram(t, C, &[0x80, 0, 0]);
        want.packets_malformed += 1;
        check(&s, &want);
        let p2 = packet(&mut c, 0, &[b"d"]);
        let mut bad = vec![TYPE_DATA];
        bad.extend_from_slice(&p2[1..3]);
        bad.extend_from_slice(&[0x2f, 0]);
        s.handle_datagram(t, C, &bad);
        want.packets_malformed += 1;
        check(&s, &want);
        s.handle_datagram(t, C, &p2);
        want.packets_received += 1;
        want.bytes_received += p2.len() as u64;
        want.messages_received += 1;
        check(&s, &want);

        // Sequenced: the older packet's message is stale.
        let p4 = packet(&mut c, 2, &[b"old"]);
        let p5 = packet(&mut c, 2, &[b"new"]);
        s.handle_datagram(t, C, &p5);
        s.handle_datagram(t, C, &p4);
        want.packets_received += 2;
        want.bytes_received += (p4.len() + p5.len()) as u64;
        want.messages_received += 1;
        want.messages_stale += 1;
        check(&s, &want);
        // The same reliable message in two packets: delivered once.
        let p3 = packet(&mut c, 1, &[b"r"]);
        s.handle_datagram(t, C, &p3);
        s.handle_datagram(t, C, &with_seq(&p3, seq_of(&p3).wrapping_add(50)));
        want.packets_received += 2;
        want.bytes_received += 2 * p3.len() as u64;
        want.messages_received += 1;
        want.messages_duplicate += 1;
        check(&s, &want);

        // Packet-level drops are peer statistics, not manager drops.
        assert_eq!(s.stats().datagrams_dropped, d0);
    }

    fn types(pkts: &[Vec<u8>]) -> Vec<u8> {
        pkts.iter().map(|p| p[0] & 0x0f).collect()
    }

    #[test]
    fn handshake_retries_follow_the_retry_timer() {
        let t = Time::from_millis(1);
        let retry = Config::default().connect_retry;
        let half = retry / 2;
        let (mut s, mut c) = (server(), client());
        c.connect(t, S, &[]).unwrap();
        c.flush(t);
        let req = drain(&mut c);
        assert_eq!(types(&req), [1]);
        c.update(t + half);
        assert!(drain(&mut c).is_empty(), "retry before its time");
        c.update(t + retry);
        assert_eq!(types(&drain(&mut c)), [1]);
        c.update(t + retry + half);
        assert!(drain(&mut c).is_empty());
        // Challenged: the response is retried on the same timer.
        let t = t + retry + half;
        s.handle_datagram(t, C, &req[0]);
        deliver(&mut c, t, S, &drain(&mut s));
        assert_eq!(types(&drain(&mut c)), [3]);
        c.update(t + half);
        assert!(drain(&mut c).is_empty(), "retry before its time");
        c.update(t + retry);
        assert_eq!(types(&drain(&mut c)), [3]);
    }

    /// Moves the client of a fresh pair to C2 without delivering anything to it, so the
    /// server's path challenges go unanswered.
    fn unvalidated_move(min_rto: Duration) -> (Manager, PeerId, Time) {
        let (mut s, mut c, id, sp, mut t) = pair(&[DeliveryMode::Unreliable], |cfg| cfg.min_rto = min_rto);
        c.local_address_changed();
        c.send(id, 0, b"x").unwrap();
        t = t + Duration::from_millis(10);
        c.flush(t);
        let moved = drain(&mut c);
        deliver(&mut s, t, C2, &moved);
        assert_eq!(s.peer_addr(sp), Some(C2));
        assert_eq!(s.peer_stats(sp).unwrap().migrations, 1);
        let challenges = drain(&mut s);
        assert!(types(&challenges).contains(&8), "PATH_CHALLENGE");
        (s, sp, t)
    }

    #[test]
    fn unvalidated_path_reverts_after_three_rto() {
        let rto = Duration::from_millis(1000);
        let (mut s, sp, t) = unvalidated_move(rto);
        let rto = s.peer_rto(sp).unwrap();
        assert!(rto >= Duration::from_millis(1000));
        // Re-challenged once per RTO, never in between.
        s.update(t + rto / 2);
        assert!(drain(&mut s).iter().all(|p| p[0] & 0x0f != 8));
        s.update(t + rto);
        assert!(types(&drain(&mut s)).contains(&8));
        s.update(t + rto * 2 + rto / 2);
        drain(&mut s);
        assert_eq!(s.stats().failed_path_validations, 0, "deadline is 3 RTO, not 1 s");
        assert_eq!(s.peer_addr(sp), Some(C2));
        s.update(t + rto * 3);
        drain(&mut s);
        assert_eq!(s.stats().failed_path_validations, 1);
        assert_eq!(s.peer_addr(sp), Some(C));
        assert!(!s.by_addr.contains_key(&C2));
        s.update(t + rto * 4);
        let late = drain(&mut s);
        assert!(
            late.iter().all(|p| p[0] & 0x0f != 8),
            "no challenges after the deadline: {late:?}"
        );
    }

    #[test]
    fn second_authenticated_packet_validates_the_path() {
        let (mut s, mut c, id, sp, mut t) = pair(&[DeliveryMode::Unreliable], |_| {});
        c.local_address_changed();
        for _ in 0..2 {
            c.send(id, 0, b"x").unwrap();
            t = t + Duration::from_millis(10);
            c.flush(t);
            deliver(&mut s, t, C2, &drain(&mut c));
        }
        // Validated without any PATH_RESPONSE: the old address is forgotten.
        assert_eq!(s.peer_addr(sp), Some(C2));
        assert!(!s.by_addr.contains_key(&C));
        s.update(t + Duration::from_secs(5));
        assert_eq!(s.stats().failed_path_validations, 0);
    }

    #[test]
    fn moving_again_before_validation_forgets_the_middle_address() {
        let (mut s, mut c, id, sp, mut t) = pair(&[DeliveryMode::Unreliable], |_| {});
        c.local_address_changed();
        c.send(id, 0, b"x").unwrap();
        t = t + Duration::from_millis(10);
        c.flush(t);
        let first = drain(&mut c);
        deliver(&mut s, t, C2, &first);
        // The next authenticated packet arrives from yet another address.
        c.send(id, 0, b"y").unwrap();
        c.flush(t);
        let c3 = SocketAddr::new(C.ip(), 5002);
        let moved = drain(&mut c);
        deliver(&mut s, t, c3, &moved[..1]);
        assert_eq!(s.peer_addr(sp), Some(c3));
        assert!(!s.by_addr.contains_key(&C2));
        assert_eq!(s.stats().migrations, 2);
        // Failing validation (deadline: 3 RTO, at least 1 s) goes back to the original address.
        s.update(t + Duration::from_secs(5));
        assert_eq!(s.peer_addr(sp), Some(C));
    }

    type Props = Vec<(&'static str, alloc::string::String)>;

    #[derive(Clone, Default)]
    struct Capture(std::sync::Arc<std::sync::Mutex<Vec<Props>>>);

    impl LogSink for Capture {
        fn enabled(&self, _: Level) -> bool {
            true
        }
        fn log(&mut self, r: &Record<'_>) {
            let props = r
                .properties
                .iter()
                .map(|p| (p.name, alloc::format!("{}", p.value)))
                .collect();
            self.0.lock().unwrap().push(props);
        }
    }

    #[test]
    fn peer_stats_event_reports_the_window_only() {
        let (mut s, _, _, sp, t) = pair(&[DeliveryMode::Unreliable], |_| {});
        let index = s.index_of(sp).unwrap();
        let peer = s.slots[index].peer.as_mut().unwrap();
        peer.stats_base = PeerStats {
            packets_sent: 100,
            packets_lost: 10,
            bytes_sent: 100_000,
            bytes_received: 50_000,
            messages_retransmitted: 5,
            ..PeerStats::default()
        };
        peer.stats = PeerStats {
            packets_sent: 200,
            packets_lost: 15,
            bytes_sent: 160_000,
            bytes_received: 80_000,
            messages_retransmitted: 7,
            ..PeerStats::default()
        };
        peer.last_stats = t;
        let cap = Capture::default();
        let mut log = Logger {
            sink: Some(alloc::boxed::Box::new(cap.clone())),
        };
        let end = t + Duration::from_secs(2);
        log_peer_stats(&mut log, peer, end, 1200);
        let got = cap.0.lock().unwrap()[0].clone();
        let get = |n: &str| got.iter().find(|(k, _)| *k == n).unwrap().1.clone();
        assert_eq!(get("LossPercent"), "5");
        assert_eq!(get("BytesOutPerSec"), "30000");
        assert_eq!(get("BytesInPerSec"), "15000");
        assert_eq!(get("FillPercent"), "50");
        assert_eq!(get("Retransmitted"), "2");
        assert_eq!(peer.stats_base, peer.stats);
        assert_eq!(peer.last_stats, end);
    }

    #[test]
    fn only_connected_outgoing_peers_prove_a_new_address() {
        let (mut s, mut c, id, sp, _) = pair(&[DeliveryMode::Unreliable], |_| {});
        let migrating = |m: &Manager, id: PeerId| m.peer(id).unwrap().migrate_from.is_some();
        s.local_address_changed();
        assert!(!migrating(&s, sp), "the server side never migrates");
        let t = Time::from_millis(1);
        let pending = c.connect(t, C2, &[]).unwrap();
        c.local_address_changed();
        assert!(!migrating(&c, pending), "not connected yet");
        assert!(migrating(&c, id));
    }

    #[test]
    fn unvalidated_address_gets_at_most_three_times_its_bytes() {
        let (mut s, mut c, id, sp, mut t) = pair(&[DeliveryMode::Unreliable], |_| {});
        c.local_address_changed();
        c.send(id, 0, &[1; 300]).unwrap();
        t = t + Duration::from_millis(10);
        c.flush(t);
        deliver(&mut s, t, C2, &drain(&mut c));
        let received = s.peer(sp).unwrap().path.unwrap().received;
        let challenge: usize = drain(&mut s).iter().map(Vec::len).sum();
        let budget = 3 * received - challenge;
        let mut sent = Vec::new();
        for n in [1, 40, 40] {
            for _ in 0..n {
                s.send(sp, 0, &[2; 50]).unwrap();
            }
            s.flush(t);
            sent.push(drain(&mut s).iter().map(Vec::len).sum::<usize>());
        }
        let total: usize = sent.iter().sum();
        assert!(sent[0] > 0 && sent[1] > 0, "{sent:?}");
        assert_eq!(sent[2], 0, "budget spent: {sent:?}");
        assert!(total <= budget, "{total} > {budget}");
        assert!(total + 80 > budget, "{total} far below {budget}");
    }

    #[test]
    fn acks_for_packets_never_sent_are_ignored() {
        let (mut s, mut c, id, sp, t) = pair(&[DeliveryMode::Unreliable], |_| {});
        s.send(sp, 0, b"lost").unwrap();
        s.flush(t);
        let last = seq_of(&drain(&mut s)[0]);
        let before = s.peer_stats(sp).unwrap().clone();
        c.send(id, 0, b"x").unwrap();
        c.flush(t);
        let real = drain(&mut c).remove(0);
        // Fresh seq, ack 5 ahead of the last packet sent, every earlier bit set.
        let mut p = vec![TYPE_DATA | crate::wire::FLAG_ACK, real[1], real[2]];
        p.extend_from_slice(&last.wrapping_add(5).to_le_bytes());
        p.extend_from_slice(&[0xff, 0xff, 0]);
        s.handle_datagram(t, C, &p);
        let after = s.peer_stats(sp).unwrap();
        assert_eq!(after.packets_received, before.packets_received + 1);
        assert_eq!(after.packets_acked, before.packets_acked);
        assert_eq!(after.packets_lost, before.packets_lost);
    }

    #[test]
    fn next_timeout_is_the_earliest_timer() {
        let t = Time::from_millis(1);
        let mut c = client();
        assert_eq!(c.next_timeout(), None);
        c.connect(t, S, &[]).unwrap();
        assert_eq!(c.next_timeout(), Some(t + c.config().connect_retry));
        let (_, c, id, _, t) = pair(&[DeliveryMode::Unreliable], |_| {});
        let peer = c.peer(id).unwrap();
        let want = (peer.last_ping + c.config().ping_interval).min(peer.last_recv + c.config().timeout);
        assert_eq!(c.next_timeout(), Some(want));
        assert!(want > t);
    }

    /// A DATA packet from the client side with a fresh seq and the given ack fields.
    fn ack_packet(seq: u16, ack: u16, bits: u16) -> Vec<u8> {
        let mut p = vec![TYPE_DATA | crate::wire::FLAG_ACK];
        p.extend_from_slice(&seq.to_le_bytes());
        p.extend_from_slice(&ack.to_le_bytes());
        p.extend_from_slice(&bits.to_le_bytes());
        p.push(0);
        p
    }

    #[test]
    fn acks_count_once_and_trigger_packet_threshold_loss() {
        let (mut s, _, _, sp, t) = pair(&[DeliveryMode::Unreliable, DeliveryMode::ReliableOrdered], |_| {});
        let mut seqs = Vec::new();
        for i in 0..6u8 {
            s.send(sp, 1, &[i]).unwrap();
            s.flush(t);
            seqs.push(seq_of(&drain(&mut s)[0]));
        }
        let before = s.peer_stats(sp).unwrap().clone();
        let stat = |s: &Manager| {
            let p = s.peer_stats(sp).unwrap();
            (
                p.packets_acked - before.packets_acked,
                p.packets_lost - before.packets_lost,
            )
        };
        // Only the fifth packet arrives: the first two are PACKET_THRESHOLD behind it.
        s.handle_datagram(t, C, &ack_packet(1000, seqs[4], 0));
        assert_eq!(stat(&s), (1, 2));
        // The same ack again changes nothing.
        s.handle_datagram(t, C, &ack_packet(1001, seqs[4], 0));
        assert_eq!(stat(&s), (1, 2));
        // The third and fourth arrived too: bits 2 and 1 below the sixth (bit 0 is the fifth).
        s.handle_datagram(t, C, &ack_packet(1002, seqs[5], 0b111));
        assert_eq!(stat(&s), (4, 2));
        // The lost messages are retransmitted, the others are not.
        s.flush(t);
        assert_eq!(
            s.peer_stats(sp).unwrap().messages_retransmitted - before.messages_retransmitted,
            2
        );
    }

    #[test]
    fn many_tiny_messages_fit_their_packets() {
        let (mut s, mut c, id, _, t) = pair(&[DeliveryMode::Unreliable, DeliveryMode::ReliableOrdered], |_| {});
        for i in 0..600u32 {
            c.send(id, (i % 2) as u8, &[i as u8]).unwrap();
        }
        c.flush(t);
        let pkts = drain(&mut c);
        assert!(pkts.iter().all(|p| p.len() <= c.config().mtu));
        deliver(&mut s, t, C, &pkts);
        let mut got = 0;
        while let Some(e) = s.poll_event() {
            got += usize::from(matches!(e, Event::Message { .. }));
        }
        assert_eq!(got, 600);
        // Dense packing: one byte of payload costs about one byte of framing.
        let total: usize = pkts.iter().map(Vec::len).sum();
        assert!(total < 600 * 3, "{total} bytes in {} packets", pkts.len());
    }

    #[test]
    fn block_deltas_add_up_to_the_block_header() {
        for reliable in [false, true] {
            let mut sum = 0;
            for n in 1..400 {
                sum += block_delta(n - 1, reliable);
                let base = if reliable { 2 } else { 0 };
                assert_eq!(sum, BlockHeader::encoded_len(n) + base, "n {n}, reliable {reliable}");
                assert!(block_delta(n, reliable) <= block_delta_max(reliable));
            }
        }
    }

    #[test]
    fn fragment_pieces_fill_the_space_exactly() {
        let piece = Piece {
            seq: 0,
            frag: Some((3000, 0)),
            data: &[],
        };
        // Fixed cost: tag 2 + total 2 + offset 1; then a 1- or 2-byte length.
        assert_eq!(TAG_MAX, 2);
        assert_eq!(piece_len(&piece, true, 3000, 100), Some(94));
        assert_eq!(piece_len(&piece, true, 3000, 200), Some(193));
        assert_eq!(piece_len(&piece, true, 3000, 5), None);
        // Too small to cut, unless it finishes the message.
        assert_eq!(piece_len(&piece, true, 50, 40), None);
        assert_eq!(piece_len(&piece, true, 30, 40), Some(30));
        assert_eq!(piece_len(&piece, true, 30, 36), Some(30));
        assert_eq!(piece_len(&piece, true, 30, 35), None);
    }
}
