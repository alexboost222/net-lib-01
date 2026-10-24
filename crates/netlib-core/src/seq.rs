//! Wrapping sequence numbers and the receive window used for dedup and acks.

/// `a` is newer than `b` in wrapping u16 space.
#[inline]
pub(crate) fn seq_newer(a: u16, b: u16) -> bool {
    a != b && a.wrapping_sub(b) < 0x8000
}

/// Number of past sequence numbers remembered for duplicate detection.
pub(crate) const RECV_WINDOW: u16 = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Received {
    New,
    Duplicate,
    TooOld,
}

/// Tracks which packet sequence numbers were received. Bit `d` means `latest - d` was seen.
#[derive(Debug, Clone, Default)]
pub(crate) struct RecvWindow {
    latest: Option<u16>,
    bits: [u64; 4],
}

impl RecvWindow {
    fn get(&self, age: u16) -> bool {
        let age = usize::from(age);
        self.bits[age / 64] & (1 << (age % 64)) != 0
    }

    fn set(&mut self, age: u16) {
        let age = usize::from(age);
        self.bits[age / 64] |= 1 << (age % 64);
    }

    fn shift(&mut self, n: u16) {
        if n >= RECV_WINDOW {
            self.bits = [0; 4];
            return;
        }
        let words = usize::from(n / 64);
        let b = u32::from(n % 64);
        for i in (0..4).rev() {
            let mut v = 0;
            if i >= words {
                let src = i - words;
                v = self.bits[src] << b;
                if b > 0 && src >= 1 {
                    v |= self.bits[src - 1] >> (64 - b);
                }
            }
            self.bits[i] = v;
        }
    }

    /// Highest sequence number received so far.
    pub(crate) fn latest(&self) -> Option<u16> {
        self.latest
    }

    /// Classifies `seq` without recording it.
    pub(crate) fn check(&self, seq: u16) -> Received {
        match self.latest {
            None => Received::New,
            Some(latest) if seq_newer(seq, latest) => Received::New,
            Some(latest) => {
                let age = latest.wrapping_sub(seq);
                if age >= RECV_WINDOW {
                    Received::TooOld
                } else if self.get(age) {
                    Received::Duplicate
                } else {
                    Received::New
                }
            }
        }
    }

    /// Records `seq`. Returns the same classification as [`check`](Self::check).
    pub(crate) fn insert(&mut self, seq: u16) -> Received {
        let result = self.check(seq);
        if result != Received::New {
            return result;
        }
        match self.latest {
            Some(latest) if !seq_newer(seq, latest) => self.set(latest.wrapping_sub(seq)),
            Some(latest) => {
                self.shift(seq.wrapping_sub(latest));
                self.latest = Some(seq);
                self.set(0);
            }
            None => {
                self.latest = Some(seq);
                self.set(0);
            }
        }
        result
    }

    /// `(ack, ack_bits)` for the data header, if anything was received.
    pub(crate) fn ack(&self) -> Option<(u16, u16)> {
        let latest = self.latest?;
        let mut bits = 0u16;
        for i in 0..16u16 {
            if self.get(i + 1) {
                bits |= 1 << i;
            }
        }
        Some((latest, bits))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_wraps() {
        assert!(seq_newer(1, 0));
        assert!(seq_newer(0, 65535));
        assert!(!seq_newer(65535, 0));
        assert!(!seq_newer(5, 5));
        assert!(seq_newer(32767, 0));
        assert!(!seq_newer(32768, 0));
    }

    #[test]
    fn dedup_and_ack_bits() {
        let mut w = RecvWindow::default();
        assert_eq!(w.ack(), None);
        assert_eq!(w.insert(10), Received::New);
        assert_eq!(w.insert(10), Received::Duplicate);
        assert_eq!(w.insert(12), Received::New);
        assert_eq!(w.insert(11), Received::New);
        assert_eq!(w.insert(8), Received::New);
        // 12 latest; 11 -> bit0, 10 -> bit1, 9 missing, 8 -> bit3
        assert_eq!(w.ack(), Some((12, 0b1011)));
        assert_eq!(w.insert(12 + 300), Received::New);
        assert_eq!(w.insert(12), Received::TooOld);
        assert_eq!(w.ack(), Some((312, 0)));
    }

    #[test]
    fn r3_wraparound_200k_packets() {
        let mut w = RecvWindow::default();
        let mut seq: u16 = 65000;
        for i in 0..200_000u32 {
            // Deliver in pairs swapped to exercise out-of-order insertion across the wrap.
            let a = seq;
            let b = seq.wrapping_add(1);
            assert_eq!(w.insert(b), Received::New, "iteration {i}");
            assert_eq!(w.insert(a), Received::New, "iteration {i}");
            assert_eq!(w.insert(a), Received::Duplicate);
            if i >= 8 {
                assert_eq!(w.ack(), Some((b, 0xffff)));
            }
            seq = seq.wrapping_add(2);
        }
    }

    #[test]
    fn shift_across_words() {
        let mut w = RecvWindow::default();
        w.insert(0);
        for gap in [1u16, 63, 64, 65, 100] {
            let latest = w.ack().unwrap().0;
            w.insert(latest.wrapping_add(gap));
            assert_eq!(
                w.check(latest),
                if gap < RECV_WINDOW {
                    Received::Duplicate
                } else {
                    Received::TooOld
                }
            );
        }
    }
}
