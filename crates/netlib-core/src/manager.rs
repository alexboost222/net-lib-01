use alloc::collections::{BTreeMap, VecDeque};
use alloc::vec;
use alloc::vec::Vec;
use core::net::SocketAddr;
use core::time::Duration;

use crate::channel::{Channel, Item, ItemInfo, Received as RecvResult, RecvLimits};
use crate::config::{Config, ConfigError, DeliveryMode, MAX_CHANNELS};
use crate::event::{DisconnectReason, Event, PeerId};
use crate::rtt::RttEstimator;
use crate::seq::{Received, RecvWindow, seq_newer};
use crate::siphash::{Rng, SipHasher};
use crate::time::Time;
use crate::wire::{
    BlockHeader, Control, DataHeader, FLAG_PING, REJECT_BY_APP, REJECT_SERVER_FULL, REJECT_VERSION_MISMATCH, Reader,
    TYPE_DATA, WIRE_VERSION, Writer, decode_ack_delay, encode_ack_delay, encode_addr, varint_len,
};

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
}

#[derive(Debug, Clone, Default)]
struct SentPacket {
    seq: u16,
    time: Time,
    in_use: bool,
    acked: bool,
    /// Reliable units carried: `(channel, message seq, fragment index)`.
    units: Vec<(u8, u16, u16)>,
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
    /// Scratch list for `flush`.
    items: Vec<(u8, Item)>,
    stats: PeerStats,
}

impl Peer {
    fn new(config: &Config, addr: SocketAddr, outgoing: bool, state: State, nonce: u64, now: Time) -> Self {
        Peer {
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
            items: Vec::new(),
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
    /// Message seq and `(count, index)` for fragments; reliable channels only.
    seq: Option<u16>,
    frag: Option<(u16, u16)>,
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
                let count = self.r.varint_usize()?;
                let index = self.r.varint_usize()?;
                if !(2..=crate::channel::MAX_FRAGMENTS).contains(&count) || index >= count {
                    return None;
                }
                frag = Some((count as u16, index as u16));
            }
        }
        let len = len.unwrap_or(self.r.remaining());
        let data = self.r.bytes(len)?;
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
    incoming: usize,
    events: EventQueue,
    tx: TxQueue,
    scratch: Vec<(u32, Option<ReasonRec>)>,
    stats: ManagerStats,
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
            incoming: 0,
            events: EventQueue::default(),
            tx: TxQueue::default(),
            scratch: Vec::new(),
            stats: ManagerStats::default(),
        })
    }

    /// The configuration.
    pub fn config(&self) -> &Config {
        &self.config
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

    fn insert_peer(&mut self, peer: Peer) -> u32 {
        let addr = peer.addr;
        if !peer.outgoing {
            self.incoming += 1;
        }
        let index = match self.free.pop() {
            Some(i) => {
                self.slots[i as usize].peer = Some(peer);
                i
            }
            None => {
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
        if !peer.outgoing {
            self.incoming -= 1;
        }
        if let Some(reason) = reason {
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
        Ok(self.id_of(index))
    }

    /// Accepts a pending incoming connection.
    pub fn accept(&mut self, now: Time, id: PeerId) -> Result<(), PeerError> {
        let index = self.index_of(id).ok_or(PeerError::UnknownPeer)?;
        let conn_id = self.rng.next_nonzero_u32();
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

    /// Queues a message. It is put on the wire by the next [`flush`](Self::flush).
    pub fn send(&mut self, id: PeerId, channel: u8, data: &[u8]) -> Result<(), SendError> {
        let index = self.index_of(id).ok_or(SendError::NotConnected)?;
        let config = &self.config;
        let peer = self.slots[index].peer.as_mut().ok_or(SendError::NotConnected)?;
        if !matches!(peer.state, State::Connected) {
            return Err(SendError::NotConnected);
        }
        let ch = peer
            .channels
            .get_mut(usize::from(channel))
            .ok_or(SendError::InvalidChannel)?;
        if data.len() > config.max_message_size(ch.mode) {
            return Err(SendError::TooLarge);
        }
        if ch.mode.is_reliable() {
            // A sequenced channel replaces its queue, so it never fills up.
            if ch.mode != DeliveryMode::ReliableSequenced && ch.reliable_queue_len() >= config.max_reliable_queue {
                return Err(SendError::QueueFull);
            }
            ch.queue_reliable(data, config.single_reliable_limit(), config.fragment_size());
        } else {
            ch.queue_unreliable(data);
        }
        Ok(())
    }

    /// Processes one received datagram.
    pub fn handle_datagram(&mut self, now: Time, from: SocketAddr, data: &[u8]) {
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
            return true;
        }
        if existing.is_none() && self.incoming >= self.config.max_incoming {
            let reject = Control::Reject {
                nonce,
                reason: REJECT_SERVER_FULL,
                data: &[],
            };
            self.tx.push_control(mtu, from, &reject, None);
            return true;
        }
        let timestamp = self.now_secs(now);
        let mac = cookie_mac(self.cookie_key, &from, nonce, timestamp, self.config.protocol_id);
        self.tx
            .push_control(mtu, from, &Control::Challenge { nonce, timestamp, mac }, None);
        self.stats.challenges_sent += 1;
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

    fn handle_data(&mut self, now: Time, from: SocketAddr, data: &[u8]) {
        let Some(&index) = self.by_addr.get(&from) else {
            self.stats.datagrams_dropped += 1;
            return;
        };
        let id = self.id_of(index);
        let Manager {
            slots,
            events,
            stats,
            config,
            tx,
            ..
        } = self;
        let Some(peer) = slots[index as usize].peer.as_mut() else {
            return;
        };
        if !matches!(peer.state, State::Connected) {
            stats.datagrams_dropped += 1;
            return;
        }
        let mut r = Reader::new(data);
        let Some(header) = DataHeader::decode(&mut r) else {
            peer.stats.packets_malformed += 1;
            return;
        };
        let body = r.rest();
        // Validate the whole packet before acting on any of it.
        if MsgIter::new(body, &peer.modes).any(|m| m.is_err()) {
            peer.stats.packets_malformed += 1;
            return;
        }
        let largest = peer.recv.latest();
        match peer.recv.insert(header.seq) {
            Received::New => {}
            Received::Duplicate | Received::TooOld => {
                peer.stats.packets_duplicate += 1;
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
            process_acks(peer, now, ack, bits, decode_ack_delay(delay), config.max_ack_delay);
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
        let mut current = [false; MAX_CHANNELS];
        for (i, ch) in channels.iter().enumerate() {
            current[i] = ch.mode != DeliveryMode::UnreliableSequenced || ch.sequenced_current(header.seq);
        }
        for msg in MsgIter::new(body, modes) {
            let Ok(msg) = msg else { break };
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
    }

    /// Runs timers: handshake retransmissions, connection and handshake timeouts.
    pub fn update(&mut self, now: Time) {
        let Manager {
            config,
            slots,
            tx,
            scratch,
            ..
        } = self;
        scratch.clear();
        for (index, slot) in slots.iter_mut().enumerate() {
            let Some(peer) = slot.peer.as_mut() else {
                continue;
            };
            let index = index as u32;
            match &mut peer.state {
                State::Connecting { next_retry, deadline } => {
                    if now >= *deadline {
                        scratch.push((index, Some(ReasonRec::ConnectTimeout)));
                    } else if now >= *next_retry {
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
                        scratch.push((index, Some(ReasonRec::ConnectTimeout)));
                    } else if now >= *next_retry {
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
                        scratch.push((index, None));
                    }
                }
                State::Connected => {
                    if now - peer.last_recv >= config.timeout {
                        scratch.push((index, Some(ReasonRec::Timeout)));
                    }
                }
            }
        }
        for i in 0..self.scratch.len() {
            let (index, reason) = self.scratch[i];
            self.remove_peer(index, reason);
        }
    }

    /// Packs queued messages into DATA packets for every connected peer; also sends
    /// pending acks, pings and keepalives.
    pub fn flush(&mut self, now: Time) {
        let Manager { config, slots, tx, .. } = self;
        for slot in slots.iter_mut() {
            if let Some(peer) = slot.peer.as_mut() {
                if matches!(peer.state, State::Connected) {
                    flush_peer(peer, now, config, tx);
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

/// Packet threshold for loss detection (RFC 9002): a packet is lost once a packet sent
/// this many packets later is acknowledged.
const PACKET_THRESHOLD: u16 = 3;

fn process_acks(peer: &mut Peer, now: Time, ack: u16, bits: u16, ack_delay: Duration, max_ack_delay: Duration) {
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
    let Some(largest) = peer.largest_acked else {
        return;
    };
    // Everything sent PACKET_THRESHOLD packets before the largest acked one and still
    // unacknowledged is lost.
    while seq_newer(largest.wrapping_sub(PACKET_THRESHOLD - 1), peer.loss_scan) {
        let seq = peer.loss_scan;
        let slot = &mut peer.sent[usize::from(seq) % SENT_RING];
        if slot.in_use && slot.seq == seq && !slot.acked {
            declare_lost(slot, &mut peer.channels, &mut peer.stats);
        }
        peer.loss_scan = seq.wrapping_add(1);
    }
}

fn declare_lost(slot: &mut SentPacket, channels: &mut [Channel], stats: &mut PeerStats) {
    slot.in_use = false;
    if slot.units.is_empty() {
        // Packets without reliable data are not acked unless the remote side happens to send.
        return;
    }
    stats.packets_lost += 1;
    for &(ch, mseq, unit) in &slot.units {
        channels[usize::from(ch)].on_lost(mseq, unit, slot.seq);
    }
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

fn send_ping_ack(peer: &mut Peer, now: Time, mtu: usize, tx: &mut TxQueue) {
    let Some((ack, bits)) = peer.recv.ack() else {
        return;
    };
    let seq = peer.next_seq;
    let header = DataHeader {
        flags: 0,
        seq,
        ack: Some((ack, bits, encode_ack_delay(now - peer.largest_recv_time))),
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

/// Wire size of an item inside a block: `(bytes before the payload without the length
/// prefix, payload length)`.
fn item_wire(info: &ItemInfo<'_>, reliable: bool, prev_seq: u16) -> (usize, usize) {
    let mut meta = 0;
    if reliable {
        let delta = u64::from(info.seq.wrapping_sub(prev_seq));
        meta += varint_len((delta << 1) | u64::from(info.frag.is_some()));
        if let Some((count, index)) = info.frag {
            meta += varint_len(u64::from(count)) + varint_len(u64::from(index));
        }
    }
    (meta, info.data.len())
}

fn flush_peer(peer: &mut Peer, now: Time, config: &Config, tx: &mut TxQueue) {
    let ping_due = now - peer.last_ping >= config.ping_interval;
    let rto = peer.rtt.rto();
    let Peer {
        channels,
        items,
        sent,
        stats,
        recv,
        addr,
        ..
    } = peer;
    items.clear();
    for (i, ch) in channels.iter_mut().enumerate() {
        ch.collect(i as u8, now, rto, config.max_rto, items);
    }
    if items.is_empty() && !ping_due && !peer.ack_pending {
        return;
    }
    let ack = if peer.received_any && (peer.ack_pending || peer.ack_repeat > 0) {
        recv.ack()
            .map(|(ack, bits)| (ack, bits, encode_ack_delay(now - peer.largest_recv_time)))
    } else {
        None
    };
    let mtu = config.mtu;
    let mut pos = 0usize;
    let mut first = true;
    loop {
        let seq = peer.next_seq;
        let start = tx.reserve(mtu);
        let mut w = Writer::new(&mut tx.bytes[start..start + mtu]);
        let flags = if first && ping_due { FLAG_PING } else { 0 };
        if (DataHeader { flags, seq, ack }).encode(&mut w).is_err() {
            tx.bytes.truncate(start);
            return;
        }
        let record = record_packet(sent, channels, stats, seq, now);
        let mut messages = 0u64;
        while pos < items.len() {
            let c = items[pos].0;
            let ch = &channels[usize::from(c)];
            let reliable = ch.mode.is_reliable();
            let fixed = if reliable { 2 } else { 0 };
            let remaining = w.remaining();
            let base = ch.info(items[pos].1).seq;
            // Plan the block: as many items of this channel as fit.
            let (mut k, mut size, mut prev) = (0usize, 0usize, base);
            let mut tail = false;
            while pos + k < items.len() && items[pos + k].0 == c {
                let info = ch.info(items[pos + k].1);
                let (meta, len) = item_wire(&info, reliable, prev);
                let with_len = varint_len(len as u64) + meta + len;
                if BlockHeader::encoded_len(k + 1) + fixed + size + with_len <= remaining {
                    size += with_len;
                    k += 1;
                    prev = info.seq;
                    continue;
                }
                // The item may still fit as the last one, without its length prefix.
                if BlockHeader::encoded_len(k + 1) + fixed + size + meta + len <= remaining {
                    size += meta + len;
                    k += 1;
                }
                tail = true;
                break;
            }
            if k == 0 {
                break;
            }
            if !tail {
                let after = remaining - BlockHeader::encoded_len(k) - fixed - size;
                tail = match items.get(pos + k) {
                    None => true,
                    Some(&(nc, nitem)) => {
                        let nch = &channels[usize::from(nc)];
                        let ninfo = nch.info(nitem);
                        let nrel = nch.mode.is_reliable();
                        let (meta, len) = item_wire(&ninfo, nrel, ninfo.seq);
                        1 + if nrel { 2 } else { 0 } + meta + len > after
                    }
                };
            }
            let ok = (|| {
                BlockHeader {
                    channel: c,
                    tail,
                    count: k,
                }
                .encode(&mut w)?;
                if reliable {
                    w.u16(base)?;
                }
                let mut prev = base;
                for j in 0..k {
                    let info = ch.info(items[pos + j].1);
                    if !(tail && j == k - 1) {
                        w.varint(info.data.len() as u64)?;
                    }
                    if reliable {
                        let delta = u64::from(info.seq.wrapping_sub(prev));
                        w.varint((delta << 1) | u64::from(info.frag.is_some()))?;
                        if let Some((count, index)) = info.frag {
                            w.varint(u64::from(count))?;
                            w.varint(u64::from(index))?;
                        }
                        prev = info.seq;
                    }
                    w.bytes(info.data)?;
                }
                Ok::<(), crate::wire::Full>(())
            })();
            debug_assert!(ok.is_ok(), "packet planner overflowed the buffer");
            if ok.is_err() {
                break;
            }
            let ch = &mut channels[usize::from(c)];
            for &(_, item) in &items[pos..pos + k] {
                if let Item::Reliable(..) = item {
                    let info = ch.info(item);
                    record.units.push((c, info.seq, info.frag.map_or(0, |f| f.1)));
                    if ch.mark_sent(item, now, seq) {
                        stats.messages_retransmitted += 1;
                    }
                }
            }
            messages += k as u64;
            pos += k;
            if tail {
                break;
            }
        }
        let len = w.pos();
        tx.commit(*addr, start, len);
        peer.next_seq = seq.wrapping_add(1);
        stats.packets_sent += 1;
        stats.bytes_sent += len as u64;
        stats.messages_sent += messages;
        if messages == 0 && flags & FLAG_PING == 0 {
            stats.standalone_acks_sent += 1;
        }
        first = false;
        if pos >= items.len() {
            break;
        }
        if messages == 0 {
            debug_assert!(false, "an item does not fit into an empty packet");
            break;
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
        c.clear_unreliable();
    }
}
