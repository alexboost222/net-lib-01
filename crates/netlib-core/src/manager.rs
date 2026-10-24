use alloc::collections::{BTreeMap, VecDeque};
use alloc::vec;
use alloc::vec::Vec;
use core::net::SocketAddr;
use core::time::Duration;

use crate::config::{Config, ConfigError, DeliveryMode};
use crate::event::{DisconnectReason, Event, PeerId};
use crate::rtt::RttEstimator;
use crate::seq::{Received, RecvWindow, seq_newer};
use crate::siphash::{Rng, SipHasher};
use crate::time::Time;
use crate::wire::{
    BlockHeader, Control, DataHeader, FLAG_PING, REJECT_BY_APP, REJECT_SERVER_FULL, REJECT_VERSION_MISMATCH, Reader,
    TYPE_DATA, WIRE_VERSION, Writer, encode_addr, varint_len,
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

#[derive(Debug, Clone, Copy, Default)]
struct SentPacket {
    seq: u16,
    time: Time,
    in_use: bool,
    acked: bool,
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

struct Channel {
    mode: DeliveryMode,
    send_bytes: Vec<u8>,
    /// `(offset, len)` into `send_bytes`.
    send_msgs: Vec<(u32, u32)>,
    /// Packet seq of the last message delivered on a sequenced channel.
    last_recv_seq: Option<u16>,
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
    next_seq: u16,
    recv: RecvWindow,
    received_any: bool,
    ack_pending: bool,
    ack_repeat: u8,
    sent: Vec<SentPacket>,
    rtt: RttEstimator,
    channels: Vec<Channel>,
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
            next_seq: 0,
            recv: RecvWindow::default(),
            received_any: false,
            ack_pending: false,
            ack_repeat: 0,
            sent: vec![SentPacket::default(); SENT_RING],
            rtt: RttEstimator::new(config.min_rto, config.max_rto),
            channels: config
                .channels
                .iter()
                .map(|c| Channel {
                    mode: c.mode,
                    send_bytes: Vec::new(),
                    send_msgs: Vec::new(),
                    last_recv_seq: None,
                })
                .collect(),
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
    data: &'a [u8],
}

/// Iterates over the messages of a DATA packet body. Yields `Err` once on malformed input.
struct MsgIter<'a, 'c> {
    r: Reader<'a>,
    channels: &'c [Channel],
    block: Option<(BlockHeader, usize)>,
    finished: bool,
}

impl<'a, 'c> MsgIter<'a, 'c> {
    fn new(body: &'a [u8], channels: &'c [Channel]) -> Self {
        MsgIter {
            r: Reader::new(body),
            channels,
            block: None,
            finished: false,
        }
    }

    fn fail(&mut self) -> Option<Result<Msg<'a>, ()>> {
        self.finished = true;
        Some(Err(()))
    }
}

impl<'a> Iterator for MsgIter<'a, '_> {
    type Item = Result<Msg<'a>, ()>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.finished {
                return None;
            }
            if let Some((header, remaining)) = &mut self.block {
                if *remaining > 0 {
                    *remaining -= 1;
                    let header = *header;
                    let last_of_tail = header.tail && *remaining == 0;
                    let len = if last_of_tail {
                        self.r.remaining()
                    } else {
                        match self.r.varint_usize() {
                            Some(l) => l,
                            None => return self.fail(),
                        }
                    };
                    if self.channels[usize::from(header.channel)].mode.is_reliable() {
                        // Reliable channels are not implemented yet and rejected by config.
                        return self.fail();
                    }
                    let Some(data) = self.r.bytes(len) else {
                        return self.fail();
                    };
                    if last_of_tail {
                        self.finished = true;
                    }
                    return Some(Ok(Msg {
                        channel: header.channel,
                        data,
                    }));
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
            if usize::from(header.channel) >= self.channels.len() {
                return self.fail();
            }
            self.block = Some((header, header.count));
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
        let max = self.config.max_message_size(DeliveryMode::Unreliable);
        let peer = self.slots[index].peer.as_mut().ok_or(SendError::NotConnected)?;
        if !matches!(peer.state, State::Connected) {
            return Err(SendError::NotConnected);
        }
        let ch = peer
            .channels
            .get_mut(usize::from(channel))
            .ok_or(SendError::InvalidChannel)?;
        if data.len() > max {
            return Err(SendError::TooLarge);
        }
        ch.send_msgs.push((ch.send_bytes.len() as u32, data.len() as u32));
        ch.send_bytes.extend_from_slice(data);
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
            slots, events, stats, ..
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
        if MsgIter::new(body, &peer.channels).any(|m| m.is_err()) {
            peer.stats.packets_malformed += 1;
            return;
        }
        match peer.recv.insert(header.seq) {
            Received::New => {}
            Received::Duplicate | Received::TooOld => {
                peer.stats.packets_duplicate += 1;
                return;
            }
        }
        peer.last_recv = now;
        peer.received_any = true;
        peer.ack_repeat = ACK_REPEAT;
        peer.stats.packets_received += 1;
        peer.stats.bytes_received += data.len() as u64;
        if header.flags & FLAG_PING != 0 {
            peer.ack_pending = true;
        }
        if let Some((ack, bits)) = header.ack {
            process_acks(peer, now, ack, bits);
        }
        let Peer { channels, stats, .. } = peer;
        let mut iter = MsgIter::new(body, channels);
        let mut deliver: [bool; crate::config::MAX_CHANNELS] = [false; crate::config::MAX_CHANNELS];
        for (i, ch) in iter.channels.iter().enumerate() {
            deliver[i] = match ch.mode {
                DeliveryMode::UnreliableSequenced => ch
                    .last_recv_seq
                    .is_none_or(|last| last == header.seq || seq_newer(header.seq, last)),
                _ => true,
            };
        }
        for msg in iter.by_ref() {
            let Ok(msg) = msg else { break };
            if deliver[usize::from(msg.channel)] {
                let data = events.store(msg.data);
                events.push(EventRec::Message {
                    peer: id,
                    channel: msg.channel,
                    data,
                });
                stats.messages_received += 1;
            } else {
                stats.messages_stale += 1;
            }
        }
        for (i, ch) in channels.iter_mut().enumerate() {
            if ch.mode == DeliveryMode::UnreliableSequenced && deliver[i] {
                ch.last_recv_seq = Some(header.seq);
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

    /// Smoothed round-trip time, once measured.
    pub fn peer_rtt(&self, id: PeerId) -> Option<Duration> {
        self.peer(id)?.rtt.srtt()
    }

    /// Current retransmission timeout.
    pub fn peer_rto(&self, id: PeerId) -> Option<Duration> {
        Some(self.peer(id)?.rtt.rto())
    }

    /// Latest raw RTT sample.
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

fn process_acks(peer: &mut Peer, now: Time, ack: u16, bits: u16) {
    // An ack for a packet we have not sent yet is bogus.
    let last_sent = peer.next_seq.wrapping_sub(1);
    if peer.stats.packets_sent == 0 || seq_newer(ack, last_sent) {
        return;
    }
    for i in 0..17u16 {
        if i > 0 && bits & (1 << (i - 1)) == 0 {
            continue;
        }
        let seq = ack.wrapping_sub(i);
        let slot = &mut peer.sent[usize::from(seq) % SENT_RING];
        if slot.in_use && slot.seq == seq && !slot.acked {
            slot.acked = true;
            peer.stats.packets_acked += 1;
            if i == 0 {
                peer.rtt.sample(now - slot.time);
            }
        }
    }
}

/// Length of the first queued message of the first non-empty channel at or after `from`.
fn next_message_len(channels: &[Channel], from: usize) -> Option<usize> {
    channels[from..]
        .iter()
        .find_map(|c| c.send_msgs.first().map(|m| m.1 as usize))
}

fn flush_peer(peer: &mut Peer, now: Time, config: &Config, tx: &mut TxQueue) {
    let ping_due = now - peer.last_ping >= config.ping_interval;
    let has_data = peer.channels.iter().any(|c| !c.send_msgs.is_empty());
    if !has_data && !ping_due && !peer.ack_pending {
        return;
    }
    let ack = if peer.received_any && (peer.ack_pending || peer.ack_repeat > 0) {
        peer.recv.ack()
    } else {
        None
    };
    let mtu = config.mtu;
    let (mut ch, mut mi) = (0usize, 0usize);
    let mut first = true;
    loop {
        let start = tx.reserve(mtu);
        let mut w = Writer::new(&mut tx.bytes[start..start + mtu]);
        let flags = if first && ping_due { FLAG_PING } else { 0 };
        let seq = peer.next_seq;
        let header = DataHeader { flags, seq, ack };
        if header.encode(&mut w).is_err() {
            tx.bytes.truncate(start);
            return;
        }
        let mut messages = 0u64;
        'blocks: while ch < peer.channels.len() {
            let msgs = &peer.channels[ch].send_msgs;
            if mi >= msgs.len() {
                ch += 1;
                mi = 0;
                continue;
            }
            let remaining = w.remaining();
            let pending = &msgs[mi..];
            let (mut k, mut size) = (0usize, 0usize);
            while k < pending.len() {
                let l = pending[k].1 as usize;
                let add = varint_len(l as u64) + l;
                if BlockHeader::encoded_len(k + 1) + size + add > remaining {
                    break;
                }
                size += add;
                k += 1;
            }
            let mut tail = false;
            if k < pending.len() {
                // The next message may still fit as the last one, without its length prefix.
                let l = pending[k].1 as usize;
                if BlockHeader::encoded_len(k + 1) + size + l <= remaining {
                    k += 1;
                    size += l;
                }
                tail = true;
            }
            if k == 0 {
                break 'blocks;
            }
            if !tail {
                let after = remaining - BlockHeader::encoded_len(k) - size;
                tail = match next_message_len(&peer.channels, ch + 1) {
                    None => true,
                    Some(l) => BlockHeader::encoded_len(1) + l > after,
                };
            }
            let block = BlockHeader {
                channel: ch as u8,
                tail,
                count: k,
            };
            let chan = &peer.channels[ch];
            let ok = (|| {
                block.encode(&mut w)?;
                for (j, &(off, len)) in pending[..k].iter().enumerate() {
                    if !(tail && j == k - 1) {
                        w.varint(u64::from(len))?;
                    }
                    w.bytes(&chan.send_bytes[off as usize..(off + len) as usize])?;
                }
                Ok::<(), crate::wire::Full>(())
            })();
            debug_assert!(ok.is_ok(), "packet planner overflowed the buffer");
            if ok.is_err() {
                break 'blocks;
            }
            messages += k as u64;
            mi += k;
            if tail {
                break 'blocks;
            }
        }
        let len = w.pos();
        tx.commit(peer.addr, start, len);
        peer.sent[usize::from(seq) % SENT_RING] = SentPacket {
            seq,
            time: now,
            in_use: true,
            acked: false,
        };
        peer.next_seq = seq.wrapping_add(1);
        peer.stats.packets_sent += 1;
        peer.stats.bytes_sent += len as u64;
        peer.stats.messages_sent += messages;
        if messages == 0 && flags & FLAG_PING == 0 {
            peer.stats.standalone_acks_sent += 1;
        }
        first = false;
        let more = ch < peer.channels.len()
            && (mi < peer.channels[ch].send_msgs.len() || next_message_len(&peer.channels, ch + 1).is_some());
        if !more {
            break;
        }
        if messages == 0 {
            debug_assert!(false, "a message does not fit into an empty packet");
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
    for c in &mut peer.channels {
        c.send_msgs.clear();
        c.send_bytes.clear();
    }
}
