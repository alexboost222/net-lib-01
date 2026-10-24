//! SipHash-2-4 (64-bit output). Used as a MAC for stateless handshake cookies
//! and as a keyed PRF for generating nonces and connection ids.

#[derive(Clone)]
pub(crate) struct SipHasher {
    v0: u64,
    v1: u64,
    v2: u64,
    v3: u64,
    tail: u64,
    ntail: usize,
    length: usize,
}

#[inline]
fn round(v0: &mut u64, v1: &mut u64, v2: &mut u64, v3: &mut u64) {
    *v0 = v0.wrapping_add(*v1);
    *v1 = v1.rotate_left(13);
    *v1 ^= *v0;
    *v0 = v0.rotate_left(32);
    *v2 = v2.wrapping_add(*v3);
    *v3 = v3.rotate_left(16);
    *v3 ^= *v2;
    *v0 = v0.wrapping_add(*v3);
    *v3 = v3.rotate_left(21);
    *v3 ^= *v0;
    *v2 = v2.wrapping_add(*v1);
    *v1 = v1.rotate_left(17);
    *v1 ^= *v2;
    *v2 = v2.rotate_left(32);
}

impl SipHasher {
    pub(crate) fn new(key: [u64; 2]) -> Self {
        let [k0, k1] = key;
        SipHasher {
            v0: k0 ^ 0x736f_6d65_7073_6575,
            v1: k1 ^ 0x646f_7261_6e64_6f6d,
            v2: k0 ^ 0x6c79_6765_6e65_7261,
            v3: k1 ^ 0x7465_6462_7974_6573,
            tail: 0,
            ntail: 0,
            length: 0,
        }
    }

    fn compress(&mut self, m: u64) {
        self.v3 ^= m;
        round(&mut self.v0, &mut self.v1, &mut self.v2, &mut self.v3);
        round(&mut self.v0, &mut self.v1, &mut self.v2, &mut self.v3);
        self.v0 ^= m;
    }

    pub(crate) fn write(&mut self, bytes: &[u8]) {
        self.length += bytes.len();
        for &b in bytes {
            self.tail |= u64::from(b) << (8 * self.ntail);
            self.ntail += 1;
            if self.ntail == 8 {
                let m = self.tail;
                self.compress(m);
                self.tail = 0;
                self.ntail = 0;
            }
        }
    }

    pub(crate) fn finish(mut self) -> u64 {
        let b = ((self.length as u64 & 0xff) << 56) | self.tail;
        self.compress(b);
        self.v2 ^= 0xff;
        for _ in 0..4 {
            round(&mut self.v0, &mut self.v1, &mut self.v2, &mut self.v3);
        }
        self.v0 ^ self.v1 ^ self.v2 ^ self.v3
    }
}

/// Keyed pseudo-random generator: SipHash-2-4 in counter mode.
/// Output is unpredictable as long as the key is secret.
pub(crate) struct Rng {
    key: [u64; 2],
    counter: u64,
}

impl Rng {
    pub(crate) fn new(key: [u64; 2]) -> Self {
        Rng { key, counter: 0 }
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        let mut h = SipHasher::new(self.key);
        h.write(&self.counter.to_le_bytes());
        self.counter = self.counter.wrapping_add(1);
        h.finish()
    }

    pub(crate) fn next_nonzero_u32(&mut self) -> u32 {
        loop {
            let v = self.next_u64() as u32;
            if v != 0 {
                return v;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::Hasher;
    use std::vec::Vec;

    fn reference(key: [u64; 2], msg: &[u8]) -> u64 {
        #[allow(deprecated)]
        let mut h = std::hash::SipHasher::new_with_keys(key[0], key[1]);
        h.write(msg);
        h.finish()
    }

    #[test]
    fn matches_paper_vectors() {
        let key = [
            u64::from_le_bytes([0, 1, 2, 3, 4, 5, 6, 7]),
            u64::from_le_bytes([8, 9, 10, 11, 12, 13, 14, 15]),
        ];
        let h = SipHasher::new(key);
        assert_eq!(h.finish(), 0x726f_db47_dd0e_0e31);
        let msg: Vec<u8> = (0..15).collect();
        let mut h = SipHasher::new(key);
        h.write(&msg);
        assert_eq!(h.finish(), 0xa129_ca61_49be_45e5);
    }

    #[test]
    fn matches_std_siphash24() {
        let mut rng = Rng::new([1, 2]);
        for len in 0..100usize {
            let msg: Vec<u8> = (0..len).map(|_| rng.next_u64() as u8).collect();
            let key = [rng.next_u64(), rng.next_u64()];
            let mut h = SipHasher::new(key);
            // Split writes to exercise the tail buffer.
            let (a, b) = msg.split_at(len / 3);
            h.write(a);
            h.write(b);
            assert_eq!(h.finish(), reference(key, &msg), "len {len}");
        }
    }
}
