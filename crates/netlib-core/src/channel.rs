//! Per-channel send and receive state for all delivery modes.

use alloc::collections::{BTreeMap, VecDeque};
use alloc::vec::Vec;
use core::time::Duration;

use crate::config::DeliveryMode;
use crate::seq::seq_newer;
use crate::time::Time;

/// Reliable messages that may be in flight at once per channel; also the receive window.
pub(crate) const RELIABLE_WINDOW: u16 = 1024;
/// Largest fragment count accepted on the wire.
pub(crate) const MAX_FRAGMENTS: usize = 4096;
/// Retransmission timeouts double per retry up to this many times.
const MAX_BACKOFF_SHIFT: u8 = 3;

/// Transmission state of one reliable message or one of its fragments.
#[derive(Debug, Clone, Copy, Default)]
struct Unit {
    sent: bool,
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
    frag_size: usize,
    units: Vec<Unit>,
    unacked: usize,
}

impl OutMsg {
    fn unit_data(&self, unit: usize) -> &[u8] {
        if self.units.len() == 1 {
            return &self.data;
        }
        let start = unit * self.frag_size;
        &self.data[start..(start + self.frag_size).min(self.data.len())]
    }
}

/// Something to put on the wire for a channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Item {
    /// Index into the unreliable send queue.
    Unreliable(u32),
    /// Message index in the reliable out queue and unit (fragment) index.
    Reliable(u32, u16),
}

/// What the packet writer needs to know about an item.
pub(crate) struct ItemInfo<'a> {
    pub(crate) seq: u16,
    /// `(count, index)` for fragments.
    pub(crate) frag: Option<(u16, u16)>,
    pub(crate) data: &'a [u8],
}

enum Slot {
    Empty,
    /// Unordered: delivered. Ordered: never used.
    Seen,
    /// Ordered: complete, waiting for earlier messages.
    Ready(Vec<u8>),
}

struct Partial {
    frags: Vec<Option<Vec<u8>>>,
    received: usize,
    bytes: usize,
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

pub(crate) struct Channel {
    pub(crate) mode: DeliveryMode,
    /// Unreliable send queue: `(offset, len)` into `send_bytes`, cleared on flush.
    send_bytes: Vec<u8>,
    send_msgs: Vec<(u32, u32)>,
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

impl Channel {
    pub(crate) fn new(mode: DeliveryMode) -> Self {
        Channel {
            mode,
            send_bytes: Vec::new(),
            send_msgs: Vec::new(),
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

    pub(crate) fn queue_unreliable(&mut self, data: &[u8]) {
        self.send_msgs.push((self.send_bytes.len() as u32, data.len() as u32));
        self.send_bytes.extend_from_slice(data);
    }

    /// Number of reliable messages not yet acknowledged.
    pub(crate) fn reliable_queue_len(&self) -> usize {
        self.out.len()
    }

    /// Queues a reliable message; messages longer than `single_limit` are split into
    /// fragments of `frag_size` bytes.
    pub(crate) fn queue_reliable(&mut self, data: &[u8], single_limit: usize, frag_size: usize) {
        if self.mode == DeliveryMode::ReliableSequenced {
            // Only the latest message matters: drop everything older.
            while let Some(m) = self.out.pop_front() {
                self.pool.push(m);
            }
        }
        let count = if data.len() <= single_limit {
            1
        } else {
            data.len().div_ceil(frag_size)
        };
        let mut m = self.pool.pop().unwrap_or_default();
        m.seq = self.next_seq;
        m.data.clear();
        m.data.extend_from_slice(data);
        m.frag_size = frag_size;
        m.units.clear();
        m.units.resize(count, Unit::default());
        m.unacked = count;
        self.next_seq = self.next_seq.wrapping_add(1);
        self.out.push_back(m);
    }

    /// Appends everything this channel wants to send now to `items`. Reliable units whose
    /// retransmission timeout expired are marked lost first.
    pub(crate) fn collect(
        &mut self,
        channel: u8,
        now: Time,
        rto: Duration,
        max_rto: Duration,
        items: &mut Vec<(u8, Item)>,
    ) {
        if !self.mode.is_reliable() {
            items.extend((0..self.send_msgs.len() as u32).map(|i| (channel, Item::Unreliable(i))));
            return;
        }
        let Some(base) = self.out.front().map(|m| m.seq) else {
            return;
        };
        for (mi, m) in self.out.iter_mut().enumerate() {
            if m.seq.wrapping_sub(base) >= RELIABLE_WINDOW {
                break;
            }
            for (ui, u) in m.units.iter_mut().enumerate() {
                if u.sent && !u.acked && !u.lost && now - u.last_sent >= backoff(rto, u.retries, max_rto) {
                    u.lost = true;
                }
                if !u.acked && (!u.sent || u.lost) {
                    items.push((channel, Item::Reliable(mi as u32, ui as u16)));
                }
            }
        }
    }

    pub(crate) fn info(&self, item: Item) -> ItemInfo<'_> {
        match item {
            Item::Unreliable(i) => {
                let (off, len) = self.send_msgs[i as usize];
                ItemInfo {
                    seq: 0,
                    frag: None,
                    data: &self.send_bytes[off as usize..(off + len) as usize],
                }
            }
            Item::Reliable(mi, ui) => {
                let m = &self.out[mi as usize];
                ItemInfo {
                    seq: m.seq,
                    frag: (m.units.len() > 1).then_some((m.units.len() as u16, ui)),
                    data: m.unit_data(usize::from(ui)),
                }
            }
        }
    }

    /// Records a transmission; returns `true` for a retransmission.
    pub(crate) fn mark_sent(&mut self, item: Item, now: Time, pkt: u16) -> bool {
        let Item::Reliable(mi, ui) = item else {
            return false;
        };
        let u = &mut self.out[mi as usize].units[usize::from(ui)];
        let retransmit = u.sent;
        if retransmit {
            u.retries = u.retries.saturating_add(1);
        }
        u.sent = true;
        u.lost = false;
        u.last_sent = now;
        u.last_pkt = pkt;
        retransmit
    }

    /// Clears the unreliable queue after a flush.
    pub(crate) fn clear_unreliable(&mut self) {
        self.send_msgs.clear();
        self.send_bytes.clear();
    }

    fn find(&mut self, seq: u16) -> Option<&mut OutMsg> {
        let base = self.out.front()?.seq;
        let m = self.out.get_mut(usize::from(seq.wrapping_sub(base)))?;
        (m.seq == seq).then_some(m)
    }

    pub(crate) fn on_acked(&mut self, seq: u16, unit: u16) {
        if let Some(m) = self.find(seq) {
            if let Some(u) = m.units.get_mut(usize::from(unit)) {
                if !u.acked {
                    u.acked = true;
                    m.unacked -= 1;
                }
            }
        }
        while self.out.front().is_some_and(|m| m.unacked == 0) {
            let m = self.out.pop_front().expect("front exists");
            self.pool.push(m);
        }
    }

    /// The packet `pkt` was declared lost.
    pub(crate) fn on_lost(&mut self, seq: u16, unit: u16, pkt: u16) {
        if let Some(u) = self.find(seq).and_then(|m| m.units.get_mut(usize::from(unit))) {
            // A later retransmission may still be in flight.
            if !u.acked && u.last_pkt == pkt {
                u.lost = true;
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
        frag: Option<(u16, u16)>,
        data: &[u8],
        limits: RecvLimits,
        deliver: &mut dyn FnMut(&[u8]),
    ) -> Received {
        // Duplicate and window checks.
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

        // Reassembly.
        let assembled;
        let complete: &[u8] = match frag {
            None => data,
            Some((count, index)) => match self.add_fragment(seq, count, index, data, limits) {
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
                let stale: Vec<u16> = self.partial.keys().copied().filter(|&s| !seq_newer(s, seq)).collect();
                for s in stale {
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
            self.partial_bytes -= p.bytes;
        }
    }

    fn add_fragment(
        &mut self,
        seq: u16,
        count: u16,
        index: u16,
        data: &[u8],
        limits: RecvLimits,
    ) -> Result<Option<Vec<u8>>, Received> {
        let p = self.partial.entry(seq).or_insert_with(|| Partial {
            frags: Vec::new(),
            received: 0,
            bytes: 0,
        });
        if p.frags.is_empty() {
            p.frags.resize(usize::from(count), None);
        }
        if p.frags.len() != usize::from(count) {
            return Err(Received::Dropped);
        }
        let slot = &mut p.frags[usize::from(index)];
        if slot.is_some() {
            return Err(Received::Duplicate);
        }
        if p.bytes + data.len() > limits.max_message || self.partial_bytes + data.len() > limits.max_partial_bytes {
            if p.received == 0 {
                self.partial.remove(&seq);
            }
            return Err(Received::Dropped);
        }
        *slot = Some(data.to_vec());
        p.received += 1;
        p.bytes += data.len();
        self.partial_bytes += data.len();
        if p.received < p.frags.len() {
            return Ok(None);
        }
        let p = self.partial.remove(&seq).expect("present");
        self.partial_bytes -= p.bytes;
        let mut out = Vec::with_capacity(p.bytes);
        for f in p.frags.into_iter().flatten() {
            out.extend_from_slice(&f);
        }
        Ok(Some(out))
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

    fn recv(ch: &mut Channel, seq: u16, frag: Option<(u16, u16)>, data: &[u8], out: &mut Vec<Vec<u8>>) -> Received {
        ch.receive_reliable(seq, frag, data, LIMITS, &mut |d| out.push(d.to_vec()))
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
        // The partial message 7 is now stale.
        assert_eq!(recv(&mut ch, 7, Some((2, 0)), b"x", &mut out), Received::Stale);
        assert_eq!(out, vec![b"5".to_vec(), b"8".to_vec()]);
        assert_eq!(ch.partial_bytes, 0);
    }

    #[test]
    fn fragments_reassemble_in_any_order() {
        let mut ch = Channel::new(DeliveryMode::ReliableOrdered);
        let mut out = Vec::new();
        assert_eq!(recv(&mut ch, 0, Some((3, 2)), b"c", &mut out), Received::Fragment);
        assert_eq!(recv(&mut ch, 0, Some((3, 2)), b"c", &mut out), Received::Duplicate);
        assert_eq!(recv(&mut ch, 0, Some((4, 0)), b"a", &mut out), Received::Dropped);
        assert_eq!(recv(&mut ch, 0, Some((3, 0)), b"a", &mut out), Received::Fragment);
        assert_eq!(recv(&mut ch, 0, Some((3, 1)), b"b", &mut out), Received::Accepted);
        assert_eq!(out, vec![b"abc".to_vec()]);
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
        let mut r = |ch: &mut Channel, seq, idx, d: &[u8]| {
            ch.receive_reliable(seq, Some((2, idx)), d, limits, &mut |d| out.push(d.to_vec()))
        };
        assert_eq!(r(&mut ch, 0, 0, b"aaa"), Received::Fragment);
        assert_eq!(r(&mut ch, 0, 1, b"bb"), Received::Dropped, "message over max_message");
        assert_eq!(r(&mut ch, 1, 0, b"ccc"), Received::Fragment);
        assert_eq!(r(&mut ch, 2, 0, b"d"), Received::Dropped, "peer over max_partial_bytes");
    }

    #[test]
    fn send_queue_acks_and_losses() {
        let mut ch = Channel::new(DeliveryMode::ReliableOrdered);
        ch.queue_reliable(b"hello", 100, 10);
        ch.queue_reliable(&[7; 25], 20, 10); // 3 fragments
        let mut items = Vec::new();
        let rto = Duration::from_millis(100);
        let t0 = Time::from_millis(1000);
        ch.collect(0, t0, rto, Duration::from_secs(2), &mut items);
        assert_eq!(items.len(), 4);
        assert_eq!(ch.info(items[3].1).frag, Some((3, 2)));
        assert_eq!(ch.info(items[3].1).data.len(), 5);
        for (i, &(_, it)) in items.iter().enumerate() {
            assert!(!ch.mark_sent(it, t0, i as u16));
        }
        // Nothing is due before the RTO.
        items.clear();
        ch.collect(0, t0 + rto / 2, rto, Duration::from_secs(2), &mut items);
        assert!(items.is_empty());
        // Packet 1 (the first fragment) is declared lost; the rest are acked.
        ch.on_acked(0, 0);
        ch.on_lost(1, 0, 1);
        ch.on_acked(1, 1);
        ch.on_acked(1, 2);
        assert_eq!(ch.reliable_queue_len(), 1);
        ch.collect(0, t0 + rto / 2, rto, Duration::from_secs(2), &mut items);
        assert_eq!(items.len(), 1);
        assert!(ch.mark_sent(items[0].1, t0, 9));
        // A loss report for the old packet does not re-trigger the retransmitted unit.
        ch.on_lost(1, 0, 1);
        items.clear();
        ch.collect(0, t0, rto, Duration::from_secs(2), &mut items);
        assert!(items.is_empty());
        ch.on_acked(1, 0);
        assert_eq!(ch.reliable_queue_len(), 0);
    }

    #[test]
    fn sequenced_sender_drops_superseded_messages() {
        let mut ch = Channel::new(DeliveryMode::ReliableSequenced);
        ch.queue_reliable(b"1", 100, 10);
        ch.queue_reliable(b"2", 100, 10);
        assert_eq!(ch.reliable_queue_len(), 1);
        let mut items = Vec::new();
        ch.collect(
            0,
            Time::ZERO,
            Duration::from_millis(100),
            Duration::from_secs(1),
            &mut items,
        );
        assert_eq!(ch.info(items[0].1).data, b"2");
        assert_eq!(ch.info(items[0].1).seq, 1);
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
