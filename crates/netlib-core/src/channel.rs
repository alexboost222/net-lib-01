//! Per-channel send and receive state for all delivery modes.
//!
//! Reliable messages are tracked as byte ranges ("chunks"). A message longer than
//! `Config::single_reliable_limit` is fragmented: the scheduler cuts pieces of any size to fill
//! the space left in a packet, and a lost piece can be cut again when it is retransmitted.

use alloc::collections::{BTreeMap, VecDeque};
use alloc::vec::Vec;
use core::time::Duration;

use crate::config::DeliveryMode;
use crate::seq::seq_newer;
use crate::time::Time;

/// Reliable messages that may be in flight at once per channel; also the receive window.
pub(crate) const RELIABLE_WINDOW: u16 = 1024;
/// Largest fragmented message accepted on the wire.
pub(crate) const MAX_FRAGMENTED_TOTAL: usize = 1 << 24;
/// Retransmission timeouts double per retry up to this many times.
const MAX_BACKOFF_SHIFT: u8 = 3;

/// Options of a single `send`, resolved against the channel defaults.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SendOpts {
    pub(crate) priority: u8,
    pub(crate) ttl: Option<Duration>,
    pub(crate) replace_key: Option<u32>,
}

#[derive(Debug, Clone, Copy)]
struct UnrelMsg {
    off: u32,
    len: u32,
    enqueued: Time,
    expires: Option<Time>,
    priority: u8,
    key: Option<u32>,
    order: u64,
    sent: bool,
    dead: bool,
}

/// A byte range of a reliable message that was put on the wire.
#[derive(Debug, Clone, Copy)]
struct Chunk {
    start: u32,
    end: u32,
    acked: bool,
    lost: bool,
    last_sent: Time,
    /// Packet that carried the latest transmission.
    last_pkt: u16,
    retries: u8,
}

#[derive(Debug, Default)]
struct OutMsg {
    seq: u16,
    data: Vec<u8>,
    fragmented: bool,
    priority: u8,
    enqueued: Time,
    order: u64,
    /// Sorted by `start`, covering `0..sent_upto`.
    chunks: Vec<Chunk>,
    sent_upto: u32,
    unacked: u32,
}

impl OutMsg {
    fn len(&self) -> u32 {
        self.data.len() as u32
    }

    fn complete(&self) -> bool {
        !self.chunks.is_empty() && self.sent_upto == self.len() && self.unacked == 0
    }

    fn chunk(&mut self, start: u32) -> Option<&mut Chunk> {
        let i = self.chunks.binary_search_by_key(&start, |c| c.start).ok()?;
        Some(&mut self.chunks[i])
    }
}

/// What a candidate refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CandKind {
    /// Index into the unreliable send queue.
    Unreliable(u32),
    /// Message index in the reliable out queue.
    Reliable(u32),
}

/// Something the scheduler may put into a packet: a whole message, or a byte range of a
/// fragmented reliable message (`start..end`).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Cand {
    /// Effective priority; higher goes first.
    pub(crate) priority: u8,
    pub(crate) enqueued: Time,
    pub(crate) order: u64,
    pub(crate) channel: u8,
    pub(crate) kind: CandKind,
    pub(crate) start: u32,
    pub(crate) end: u32,
    pub(crate) splittable: bool,
}

/// A piece ready to be written.
pub(crate) struct Piece<'a> {
    pub(crate) seq: u16,
    /// `(total, offset)` for fragments.
    pub(crate) frag: Option<(u32, u32)>,
    pub(crate) data: &'a [u8],
}

enum Slot {
    Empty,
    /// Unordered: delivered.
    Seen,
    /// Ordered: complete, waiting for earlier messages.
    Ready(Vec<u8>),
}

struct Partial {
    buf: Vec<u8>,
    /// Received ranges, sorted and merged.
    ranges: Vec<(u32, u32)>,
}

impl Partial {
    /// Adds `start..end`; returns `false` if it was already covered completely.
    fn add(&mut self, start: u32, end: u32) -> bool {
        if self.ranges.iter().any(|&(s, e)| s <= start && end <= e) {
            return false;
        }
        let mut s = start;
        let mut e = end;
        self.ranges.retain(|&(rs, re)| {
            if re < s || rs > e {
                true
            } else {
                s = s.min(rs);
                e = e.max(re);
                false
            }
        });
        let pos = self.ranges.partition_point(|&(rs, _)| rs < s);
        self.ranges.insert(pos, (s, e));
        true
    }

    fn complete(&self) -> bool {
        self.ranges.len() == 1 && self.ranges[0] == (0, self.buf.len() as u32)
    }
}

/// Result of receiving one reliable message or fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Received {
    /// Delivered (possibly together with buffered followers) or buffered for ordering.
    Accepted,
    /// A fragment was stored; the message is not complete yet.
    Fragment,
    /// Already received.
    Duplicate,
    /// Older than the latest delivered message of a sequenced channel.
    Stale,
    /// Outside the receive window, inconsistent or over the reassembly limit.
    Dropped,
}

/// Limits applied by the receiver.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RecvLimits {
    pub(crate) max_message: usize,
    pub(crate) max_partial_bytes: usize,
}

/// Counters updated while collecting candidates.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct CollectStats {
    pub(crate) expired: u64,
}

pub(crate) struct Channel {
    pub(crate) mode: DeliveryMode,
    /// Unreliable send queue; data lives in `send_bytes`.
    send_bytes: Vec<u8>,
    send_msgs: Vec<UnrelMsg>,
    next_order: u64,
    /// Reliable send state.
    next_seq: u16,
    out: VecDeque<OutMsg>,
    pool: Vec<OutMsg>,
    /// Latest delivered packet seq (UnreliableSequenced) or message seq (ReliableSequenced).
    last_recv_seq: Option<u16>,
    /// Lowest message seq not yet delivered (ordered) or received (unordered).
    recv_next: u16,
    slots: VecDeque<Slot>,
    partial: BTreeMap<u16, Partial>,
    partial_bytes: usize,
}

fn backoff(rto: Duration, retries: u8, max_rto: Duration) -> Duration {
    (rto * (1u32 << retries.min(MAX_BACKOFF_SHIFT))).min(max_rto)
}

/// Base priority raised linearly with waiting time; reaches 255 after `starvation`.
pub(crate) fn effective_priority(base: u8, waited: Duration, starvation: Duration) -> u8 {
    let t = starvation.as_micros().max(1);
    let w = waited.as_micros().min(t);
    base + (u128::from(255 - base) * w / t) as u8
}

impl Channel {
    pub(crate) fn new(mode: DeliveryMode) -> Self {
        Channel {
            mode,
            send_bytes: Vec::new(),
            send_msgs: Vec::new(),
            next_order: 0,
            next_seq: 0,
            out: VecDeque::new(),
            pool: Vec::new(),
            last_recv_seq: None,
            recv_next: 0,
            slots: VecDeque::new(),
            partial: BTreeMap::new(),
            partial_bytes: 0,
        }
    }

    fn order(&mut self) -> u64 {
        self.next_order += 1;
        self.next_order
    }

    /// Queues an unreliable message. Returns `true` if it replaced a queued one.
    pub(crate) fn queue_unreliable(&mut self, data: &[u8], now: Time, opts: SendOpts) -> bool {
        let mut replaced = false;
        if let Some(key) = opts.replace_key {
            for m in self.send_msgs.iter_mut().filter(|m| !m.dead && m.key == Some(key)) {
                m.dead = true;
                replaced = true;
            }
        }
        let order = self.order();
        self.send_msgs.push(UnrelMsg {
            off: self.send_bytes.len() as u32,
            len: data.len() as u32,
            enqueued: now,
            expires: opts.ttl.map(|t| now + t),
            priority: opts.priority,
            key: opts.replace_key,
            order,
            sent: false,
            dead: false,
        });
        self.send_bytes.extend_from_slice(data);
        replaced
    }

    /// Number of reliable messages not yet acknowledged.
    pub(crate) fn reliable_queue_len(&self) -> usize {
        self.out.len()
    }

    /// Number of unreliable messages waiting to be sent.
    pub(crate) fn unreliable_queue_len(&self) -> usize {
        self.send_msgs.iter().filter(|m| !m.dead).count()
    }

    /// Queues a reliable message; messages longer than `single_limit` are fragmented.
    pub(crate) fn queue_reliable(&mut self, data: &[u8], single_limit: usize, now: Time, priority: u8) {
        if self.mode == DeliveryMode::ReliableSequenced {
            // Only the latest message matters: drop everything older.
            while let Some(m) = self.out.pop_front() {
                self.pool.push(m);
            }
        }
        let order = self.order();
        let mut m = self.pool.pop().unwrap_or_default();
        m.seq = self.next_seq;
        m.data.clear();
        m.data.extend_from_slice(data);
        m.fragmented = data.len() > single_limit;
        m.priority = priority;
        m.enqueued = now;
        m.order = order;
        m.chunks.clear();
        m.sent_upto = 0;
        m.unacked = 0;
        self.next_seq = self.next_seq.wrapping_add(1);
        self.out.push_back(m);
    }

    /// Appends everything this channel wants to send now to `cands`. Expired unreliable
    /// messages are dropped and reliable chunks whose retransmission timeout passed are
    /// marked lost first.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn collect(
        &mut self,
        channel: u8,
        now: Time,
        rto: Duration,
        max_rto: Duration,
        starvation: Duration,
        cands: &mut Vec<Cand>,
        stats: &mut CollectStats,
    ) {
        if !self.mode.is_reliable() {
            for (i, m) in self.send_msgs.iter_mut().enumerate() {
                if m.dead {
                    continue;
                }
                if m.expires.is_some_and(|e| now >= e) {
                    m.dead = true;
                    stats.expired += 1;
                    continue;
                }
                cands.push(Cand {
                    priority: effective_priority(m.priority, now - m.enqueued, starvation),
                    enqueued: m.enqueued,
                    order: m.order,
                    channel,
                    kind: CandKind::Unreliable(i as u32),
                    start: 0,
                    end: m.len,
                    splittable: false,
                });
            }
            return;
        }
        let Some(base) = self.out.front().map(|m| m.seq) else {
            return;
        };
        for (mi, m) in self.out.iter_mut().enumerate() {
            if m.seq.wrapping_sub(base) >= RELIABLE_WINDOW {
                break;
            }
            let priority = effective_priority(m.priority, now - m.enqueued, starvation);
            let mut push = |start: u32, end: u32| {
                cands.push(Cand {
                    priority,
                    enqueued: m.enqueued,
                    order: m.order,
                    channel,
                    kind: CandKind::Reliable(mi as u32),
                    start,
                    end,
                    splittable: m.fragmented,
                });
            };
            for c in m.chunks.iter_mut() {
                if !c.acked && !c.lost && now - c.last_sent >= backoff(rto, c.retries, max_rto) {
                    c.lost = true;
                }
                if c.lost && !c.acked {
                    push(c.start, c.end);
                }
            }
            if m.sent_upto < m.data.len() as u32 || m.data.is_empty() && m.chunks.is_empty() {
                push(m.sent_upto, m.data.len() as u32);
            }
        }
    }

    pub(crate) fn piece(&self, kind: CandKind, start: u32, end: u32) -> Piece<'_> {
        match kind {
            CandKind::Unreliable(i) => {
                let m = &self.send_msgs[i as usize];
                Piece {
                    seq: 0,
                    frag: None,
                    data: &self.send_bytes[m.off as usize..(m.off + m.len) as usize],
                }
            }
            CandKind::Reliable(mi) => {
                let m = &self.out[mi as usize];
                Piece {
                    seq: m.seq,
                    frag: m.fragmented.then_some((m.len(), start)),
                    data: &m.data[start as usize..end as usize],
                }
            }
        }
    }

    /// Records a transmission of `start..end`; returns `true` for a retransmission.
    pub(crate) fn mark_sent(&mut self, kind: CandKind, start: u32, end: u32, now: Time, pkt: u16) -> bool {
        let mi = match kind {
            CandKind::Unreliable(i) => {
                self.send_msgs[i as usize].sent = true;
                return false;
            }
            CandKind::Reliable(mi) => mi as usize,
        };
        let m = &mut self.out[mi];
        if start >= m.sent_upto {
            m.chunks.push(Chunk {
                start,
                end,
                acked: false,
                lost: false,
                last_sent: now,
                last_pkt: pkt,
                retries: 0,
            });
            m.sent_upto = end;
            m.unacked += 1;
            return false;
        }
        let i = m
            .chunks
            .binary_search_by_key(&start, |c| c.start)
            .expect("retransmitted pieces start at a chunk boundary");
        let c = m.chunks[i];
        if end < c.end {
            // Only the front of the lost chunk fitted: split it.
            m.chunks.insert(i + 1, Chunk { start: end, ..c });
            m.unacked += 1;
        }
        m.chunks[i] = Chunk {
            end,
            lost: false,
            last_sent: now,
            last_pkt: pkt,
            retries: c.retries.saturating_add(1),
            ..c
        };
        true
    }

    /// Drops sent and expired unreliable messages after a flush and compacts the queue.
    /// On a sequenced channel, unsent messages older than one that was sent are stale.
    pub(crate) fn after_flush(&mut self) -> u64 {
        if self.mode.is_reliable() {
            return 0;
        }
        let newest_sent = if self.mode == DeliveryMode::UnreliableSequenced {
            self.send_msgs.iter().filter(|m| m.sent).map(|m| m.order).max()
        } else {
            None
        };
        let mut stale = 0;
        let mut write = 0usize;
        let mut byte_write = 0u32;
        for read in 0..self.send_msgs.len() {
            let mut m = self.send_msgs[read];
            if !m.sent && !m.dead && newest_sent.is_some_and(|n| m.order < n) {
                m.dead = true;
                stale += 1;
            }
            if m.sent || m.dead {
                continue;
            }
            self.send_bytes
                .copy_within(m.off as usize..(m.off + m.len) as usize, byte_write as usize);
            m.off = byte_write;
            byte_write += m.len;
            self.send_msgs[write] = m;
            write += 1;
        }
        self.send_msgs.truncate(write);
        self.send_bytes.truncate(byte_write as usize);
        stale
    }

    fn find(&mut self, seq: u16) -> Option<&mut OutMsg> {
        let base = self.out.front()?.seq;
        let m = self.out.get_mut(usize::from(seq.wrapping_sub(base)))?;
        (m.seq == seq).then_some(m)
    }

    /// The seq offset of a reliable message from the oldest unacked one, for ordering pieces.
    pub(crate) fn seq_offset(&self, seq: u16) -> u16 {
        self.out.front().map_or(0, |m| seq.wrapping_sub(m.seq))
    }

    pub(crate) fn on_acked(&mut self, seq: u16, start: u32) {
        if let Some(m) = self.find(seq) {
            if let Some(c) = m.chunk(start) {
                if !c.acked {
                    c.acked = true;
                    m.unacked -= 1;
                }
            }
        }
        while self.out.front().is_some_and(OutMsg::complete) {
            let m = self.out.pop_front().expect("front exists");
            self.pool.push(m);
        }
    }

    /// The packet `pkt`, which carried `seq` from `start`, was declared lost.
    pub(crate) fn on_lost(&mut self, seq: u16, start: u32, pkt: u16) {
        if let Some(c) = self.find(seq).and_then(|m| m.chunk(start)) {
            // A later retransmission may still be in flight.
            if !c.acked && c.last_pkt == pkt {
                c.lost = true;
            }
        }
    }

    // ---- receive side ----

    /// UnreliableSequenced: whether a message from packet `pkt_seq` is still current.
    pub(crate) fn sequenced_current(&self, pkt_seq: u16) -> bool {
        self.last_recv_seq
            .is_none_or(|last| last == pkt_seq || seq_newer(pkt_seq, last))
    }

    pub(crate) fn set_sequenced_latest(&mut self, pkt_seq: u16) {
        self.last_recv_seq = Some(pkt_seq);
    }

    fn slot(&mut self, d: usize) -> &mut Slot {
        while self.slots.len() <= d {
            self.slots.push_back(Slot::Empty);
        }
        &mut self.slots[d]
    }

    /// Receives a reliable message or fragment and passes complete messages to `deliver`
    /// in the order the mode requires.
    pub(crate) fn receive_reliable(
        &mut self,
        seq: u16,
        frag: Option<(u32, u32)>,
        data: &[u8],
        limits: RecvLimits,
        deliver: &mut dyn FnMut(&[u8]),
    ) -> Received {
        let d = usize::from(seq.wrapping_sub(self.recv_next));
        match self.mode {
            DeliveryMode::ReliableSequenced => {
                if self.last_recv_seq.is_some_and(|l| !seq_newer(seq, l)) {
                    return Received::Stale;
                }
            }
            _ => {
                if d >= 0x8000 {
                    return Received::Duplicate;
                }
                if d >= usize::from(RELIABLE_WINDOW) {
                    return Received::Dropped;
                }
                if !matches!(self.slots.get(d), None | Some(Slot::Empty)) {
                    return Received::Duplicate;
                }
            }
        }

        let assembled;
        let complete: &[u8] = match frag {
            None => data,
            Some((total, offset)) => match self.add_fragment(seq, total, offset, data, limits) {
                Ok(Some(v)) => {
                    assembled = v;
                    &assembled
                }
                Ok(None) => return Received::Fragment,
                Err(r) => return r,
            },
        };

        match self.mode {
            DeliveryMode::ReliableSequenced => {
                self.last_recv_seq = Some(seq);
                // Partial messages older than this one will never be delivered.
                while let Some(s) = self.partial.keys().copied().find(|&s| !seq_newer(s, seq)) {
                    self.drop_partial(s);
                }
                deliver(complete);
            }
            DeliveryMode::ReliableUnordered => {
                deliver(complete);
                *self.slot(d) = Slot::Seen;
                self.advance(deliver);
            }
            _ => {
                if d == 0 {
                    deliver(complete);
                    *self.slot(0) = Slot::Seen;
                    self.advance(deliver);
                } else {
                    *self.slot(d) = Slot::Ready(complete.to_vec());
                }
            }
        }
        Received::Accepted
    }

    /// Moves `recv_next` past delivered messages, delivering buffered ordered ones.
    fn advance(&mut self, deliver: &mut dyn FnMut(&[u8])) {
        loop {
            match self.slots.front() {
                Some(Slot::Seen) => {}
                Some(Slot::Ready(data)) => deliver(data),
                _ => break,
            }
            self.slots.pop_front();
            self.recv_next = self.recv_next.wrapping_add(1);
        }
    }

    fn drop_partial(&mut self, seq: u16) {
        if let Some(p) = self.partial.remove(&seq) {
            self.partial_bytes -= p.buf.len();
        }
    }

    fn add_fragment(
        &mut self,
        seq: u16,
        total: u32,
        offset: u32,
        data: &[u8],
        limits: RecvLimits,
    ) -> Result<Option<Vec<u8>>, Received> {
        let end = offset as usize + data.len();
        if end > total as usize || total as usize > limits.max_message {
            return Err(Received::Dropped);
        }
        if !self.partial.contains_key(&seq) {
            if self.partial_bytes + total as usize > limits.max_partial_bytes {
                return Err(Received::Dropped);
            }
            self.partial_bytes += total as usize;
            self.partial.insert(
                seq,
                Partial {
                    buf: alloc::vec![0; total as usize],
                    ranges: Vec::new(),
                },
            );
        }
        let p = self.partial.get_mut(&seq).expect("inserted");
        if p.buf.len() != total as usize {
            return Err(Received::Dropped);
        }
        if !p.add(offset, end as u32) {
            return Err(Received::Duplicate);
        }
        p.buf[offset as usize..end].copy_from_slice(data);
        if !p.complete() {
            return Ok(None);
        }
        let p = self.partial.remove(&seq).expect("present");
        self.partial_bytes -= p.buf.len();
        Ok(Some(p.buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    const LIMITS: RecvLimits = RecvLimits {
        max_message: 1 << 20,
        max_partial_bytes: 1 << 20,
    };
    const OPTS: SendOpts = SendOpts {
        priority: 128,
        ttl: None,
        replace_key: None,
    };

    fn recv(ch: &mut Channel, seq: u16, frag: Option<(u32, u32)>, data: &[u8], out: &mut Vec<Vec<u8>>) -> Received {
        ch.receive_reliable(seq, frag, data, LIMITS, &mut |d| out.push(d.to_vec()))
    }

    fn collect(ch: &mut Channel, now: Time) -> Vec<Cand> {
        let mut cands = Vec::new();
        let ms = Duration::from_millis(1);
        ch.collect(
            0,
            now,
            ms * 100,
            ms * 2000,
            ms * 500,
            &mut cands,
            &mut CollectStats::default(),
        );
        cands
    }

    #[test]
    fn ordered_buffers_until_gap_is_filled() {
        let mut ch = Channel::new(DeliveryMode::ReliableOrdered);
        let mut out = Vec::new();
        assert_eq!(recv(&mut ch, 2, None, b"c", &mut out), Received::Accepted);
        assert_eq!(recv(&mut ch, 1, None, b"b", &mut out), Received::Accepted);
        assert!(out.is_empty());
        assert_eq!(recv(&mut ch, 0, None, b"a", &mut out), Received::Accepted);
        assert_eq!(out, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(recv(&mut ch, 1, None, b"b", &mut out), Received::Duplicate);
        assert_eq!(recv(&mut ch, 2000, None, b"x", &mut out), Received::Dropped);
    }

    #[test]
    fn unordered_delivers_at_once_and_dedups() {
        let mut ch = Channel::new(DeliveryMode::ReliableUnordered);
        let mut out = Vec::new();
        recv(&mut ch, 1, None, b"b", &mut out);
        recv(&mut ch, 1, None, b"b", &mut out);
        recv(&mut ch, 0, None, b"a", &mut out);
        recv(&mut ch, 0, None, b"a", &mut out);
        assert_eq!(out, vec![b"b".to_vec(), b"a".to_vec()]);
        assert_eq!(ch.recv_next, 2);
    }

    #[test]
    fn sequenced_keeps_latest() {
        let mut ch = Channel::new(DeliveryMode::ReliableSequenced);
        let mut out = Vec::new();
        recv(&mut ch, 5, None, b"5", &mut out);
        assert_eq!(recv(&mut ch, 3, None, b"3", &mut out), Received::Stale);
        recv(&mut ch, 7, Some((2, 1)), b"y", &mut out);
        recv(&mut ch, 8, None, b"8", &mut out);
        assert_eq!(recv(&mut ch, 7, Some((2, 0)), b"x", &mut out), Received::Stale);
        assert_eq!(out, vec![b"5".to_vec(), b"8".to_vec()]);
        assert_eq!(ch.partial_bytes, 0);
    }

    #[test]
    fn byte_range_fragments_reassemble_in_any_order() {
        let mut ch = Channel::new(DeliveryMode::ReliableOrdered);
        let mut out = Vec::new();
        assert_eq!(recv(&mut ch, 0, Some((6, 4)), b"ef", &mut out), Received::Fragment);
        assert_eq!(recv(&mut ch, 0, Some((6, 4)), b"ef", &mut out), Received::Duplicate);
        assert_eq!(
            recv(&mut ch, 0, Some((7, 0)), b"a", &mut out),
            Received::Dropped,
            "total mismatch"
        );
        assert_eq!(
            recv(&mut ch, 0, Some((6, 5)), b"fg", &mut out),
            Received::Dropped,
            "past the end"
        );
        assert_eq!(recv(&mut ch, 0, Some((6, 0)), b"abc", &mut out), Received::Fragment);
        // Overlapping retransmission of a re-cut range.
        assert_eq!(recv(&mut ch, 0, Some((6, 2)), b"cd", &mut out), Received::Accepted);
        assert_eq!(out, vec![b"abcdef".to_vec()]);
        assert_eq!(ch.partial_bytes, 0);
    }

    #[test]
    fn reassembly_respects_limits() {
        let mut ch = Channel::new(DeliveryMode::ReliableUnordered);
        let limits = RecvLimits {
            max_message: 4,
            max_partial_bytes: 6,
        };
        let mut out = Vec::new();
        let mut r = |ch: &mut Channel, seq, total, off, d: &[u8]| {
            ch.receive_reliable(seq, Some((total, off)), d, limits, &mut |d| out.push(d.to_vec()))
        };
        assert_eq!(
            r(&mut ch, 0, 5, 0, b"aa"),
            Received::Dropped,
            "message over max_message"
        );
        assert_eq!(r(&mut ch, 0, 4, 0, b"aa"), Received::Fragment);
        assert_eq!(
            r(&mut ch, 1, 4, 0, b"cc"),
            Received::Dropped,
            "peer over max_partial_bytes"
        );
    }

    #[test]
    fn partial_merges_ranges() {
        let mut p = Partial {
            buf: vec![0; 10],
            ranges: Vec::new(),
        };
        assert!(p.add(4, 6));
        assert!(p.add(0, 2));
        assert!(!p.add(4, 5));
        assert!(p.add(2, 4));
        assert_eq!(p.ranges, vec![(0, 6)]);
        assert!(p.add(5, 10));
        assert!(p.complete());
    }

    #[test]
    fn chunks_split_on_partial_retransmission() {
        let mut ch = Channel::new(DeliveryMode::ReliableOrdered);
        let t0 = Time::from_millis(1000);
        ch.queue_reliable(&[7; 100], 10, t0, 128);
        let c = collect(&mut ch, t0);
        assert_eq!((c.len(), c[0].start, c[0].end, c[0].splittable), (1, 0, 100, true));
        // Sent in two pieces in packets 1 and 2.
        assert!(!ch.mark_sent(c[0].kind, 0, 60, t0, 1));
        assert!(!ch.mark_sent(c[0].kind, 60, 100, t0, 2));
        assert!(collect(&mut ch, t0).is_empty());
        assert_eq!(ch.piece(c[0].kind, 60, 100).frag, Some((100, 60)));
        // Packet 1 lost; its range is resent in two smaller pieces.
        ch.on_lost(0, 0, 1);
        let c = collect(&mut ch, t0);
        assert_eq!((c.len(), c[0].start, c[0].end), (1, 0, 60));
        assert!(ch.mark_sent(c[0].kind, 0, 25, t0, 3));
        let c = collect(&mut ch, t0);
        assert_eq!((c[0].start, c[0].end), (25, 60));
        assert!(ch.mark_sent(c[0].kind, 25, 60, t0, 4));
        for (start, _) in [(0, 3), (25, 4), (60, 2)] {
            ch.on_acked(0, start);
        }
        assert_eq!(ch.reliable_queue_len(), 0);
    }

    #[test]
    fn timeouts_mark_chunks_lost_with_backoff() {
        let mut ch = Channel::new(DeliveryMode::ReliableOrdered);
        let t0 = Time::from_millis(1000);
        let ms = Duration::from_millis(1);
        ch.queue_reliable(b"hello", 100, t0, 128);
        let c = collect(&mut ch, t0);
        ch.mark_sent(c[0].kind, 0, 5, t0, 0);
        assert!(collect(&mut ch, t0 + ms * 99).is_empty());
        let c = collect(&mut ch, t0 + ms * 100);
        assert_eq!(c.len(), 1);
        ch.mark_sent(c[0].kind, 0, 5, t0 + ms * 100, 1);
        // Second retry waits twice as long.
        assert!(collect(&mut ch, t0 + ms * 299).is_empty());
        assert_eq!(collect(&mut ch, t0 + ms * 300).len(), 1);
    }

    #[test]
    fn unreliable_ttl_replace_and_compaction() {
        let mut ch = Channel::new(DeliveryMode::Unreliable);
        let t0 = Time::from_millis(1000);
        let ms = Duration::from_millis(1);
        let ttl = SendOpts {
            ttl: Some(ms * 50),
            ..OPTS
        };
        let keyed = SendOpts {
            replace_key: Some(7),
            ..OPTS
        };
        ch.queue_unreliable(b"short-lived", t0, ttl);
        ch.queue_unreliable(b"pos v1", t0, keyed);
        assert!(ch.queue_unreliable(b"pos v2", t0, keyed));
        ch.queue_unreliable(b"other", t0, OPTS);
        let mut stats = CollectStats::default();
        let mut cands = Vec::new();
        ch.collect(0, t0 + ms * 60, ms, ms, ms * 500, &mut cands, &mut stats);
        assert_eq!(stats.expired, 1);
        let data: Vec<&[u8]> = cands.iter().map(|c| ch.piece(c.kind, 0, 0).data).collect();
        assert_eq!(data, vec![&b"pos v2"[..], b"other"]);
        // Send only "other"; "pos v2" stays queued and is compacted to the front.
        ch.mark_sent(cands[1].kind, 0, 5, t0, 0);
        ch.after_flush();
        assert_eq!(ch.unreliable_queue_len(), 1);
        assert_eq!(ch.send_bytes, b"pos v2");
    }

    #[test]
    fn sequenced_unreliable_drops_older_unsent() {
        let mut ch = Channel::new(DeliveryMode::UnreliableSequenced);
        let t0 = Time::from_millis(1000);
        ch.queue_unreliable(b"old", t0, OPTS);
        ch.queue_unreliable(b"new", t0, SendOpts { priority: 200, ..OPTS });
        let cands = collect(&mut ch, t0);
        ch.mark_sent(cands[1].kind, 0, 3, t0, 0);
        assert_eq!(ch.after_flush(), 1, "the older message became stale");
        assert_eq!(ch.unreliable_queue_len(), 0);
    }

    #[test]
    fn priority_ages_to_the_top() {
        let ms = Duration::from_millis(1);
        assert_eq!(effective_priority(10, Duration::ZERO, ms * 500), 10);
        assert_eq!(effective_priority(10, ms * 250, ms * 500), 132);
        assert_eq!(effective_priority(10, ms * 500, ms * 500), 255);
        assert_eq!(effective_priority(255, ms * 9000, ms * 500), 255);
    }

    #[test]
    fn sequenced_sender_drops_superseded_messages() {
        let mut ch = Channel::new(DeliveryMode::ReliableSequenced);
        ch.queue_reliable(b"1", 100, Time::ZERO, 128);
        ch.queue_reliable(b"2", 100, Time::ZERO, 128);
        assert_eq!(ch.reliable_queue_len(), 1);
        let c = collect(&mut ch, Time::ZERO);
        assert_eq!(ch.piece(c[0].kind, c[0].start, c[0].end).data, b"2");
        assert_eq!(ch.piece(c[0].kind, 0, 1).seq, 1);
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let ms = Duration::from_millis(1);
        assert_eq!(backoff(ms * 100, 0, ms * 2000), ms * 100);
        assert_eq!(backoff(ms * 100, 2, ms * 2000), ms * 400);
        assert_eq!(backoff(ms * 100, 9, ms * 2000), ms * 800);
        assert_eq!(backoff(ms * 400, 9, ms * 2000), ms * 2000);
    }
}
