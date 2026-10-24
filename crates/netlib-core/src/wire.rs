//! Byte-level encoding of datagrams. See `docs/WIRE_FORMAT.md`.

use core::net::SocketAddr;

pub(crate) const WIRE_VERSION: u16 = 0;

pub(crate) const TYPE_DATA: u8 = 0;
pub(crate) const TYPE_CONNECT_REQUEST: u8 = 1;
pub(crate) const TYPE_CHALLENGE: u8 = 2;
pub(crate) const TYPE_CHALLENGE_RESPONSE: u8 = 3;
pub(crate) const TYPE_ACCEPT: u8 = 4;
pub(crate) const TYPE_REJECT: u8 = 5;
pub(crate) const TYPE_DISCONNECT: u8 = 6;

pub(crate) const FLAG_ACK: u8 = 0x10;
pub(crate) const FLAG_PING: u8 = 0x20;
const KNOWN_DATA_FLAGS: u8 = FLAG_ACK | FLAG_PING;

/// `CONNECT_REQUEST` is padded to this size so that every pre-validation reply is smaller.
pub(crate) const CONNECT_REQUEST_SIZE: usize = 32;
/// Data header with an ack block: type/flags + seq + ack + ack_bits.
pub(crate) const DATA_HEADER_MAX: usize = 7;

pub(crate) const REJECT_BY_APP: u8 = 0;
pub(crate) const REJECT_SERVER_FULL: u8 = 1;
pub(crate) const REJECT_VERSION_MISMATCH: u8 = 2;

/// The output buffer is too small.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Full;

pub(crate) struct Writer<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> Writer<'a> {
    pub(crate) fn new(buf: &'a mut [u8]) -> Self {
        Writer { buf, pos: 0 }
    }

    pub(crate) fn pos(&self) -> usize {
        self.pos
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub(crate) fn bytes(&mut self, b: &[u8]) -> Result<(), Full> {
        let end = self.pos.checked_add(b.len()).ok_or(Full)?;
        self.buf.get_mut(self.pos..end).ok_or(Full)?.copy_from_slice(b);
        self.pos = end;
        Ok(())
    }

    pub(crate) fn u8(&mut self, v: u8) -> Result<(), Full> {
        self.bytes(&[v])
    }

    pub(crate) fn u16(&mut self, v: u16) -> Result<(), Full> {
        self.bytes(&v.to_le_bytes())
    }

    pub(crate) fn u32(&mut self, v: u32) -> Result<(), Full> {
        self.bytes(&v.to_le_bytes())
    }

    pub(crate) fn u64(&mut self, v: u64) -> Result<(), Full> {
        self.bytes(&v.to_le_bytes())
    }

    pub(crate) fn varint(&mut self, mut v: u64) -> Result<(), Full> {
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                return self.u8(byte);
            }
            self.u8(byte | 0x80)?;
        }
    }

    pub(crate) fn zeros(&mut self, n: usize) -> Result<(), Full> {
        let end = self.pos.checked_add(n).ok_or(Full)?;
        self.buf.get_mut(self.pos..end).ok_or(Full)?.fill(0);
        self.pos = end;
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    pub(crate) fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let s = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(s)
    }

    pub(crate) fn rest(&mut self) -> &'a [u8] {
        let s = &self.buf[self.pos..];
        self.pos = self.buf.len();
        s
    }

    fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.bytes(N)?.try_into().ok()
    }

    pub(crate) fn u8(&mut self) -> Option<u8> {
        Some(self.array::<1>()?[0])
    }

    pub(crate) fn u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.array()?))
    }

    pub(crate) fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.array()?))
    }

    pub(crate) fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.array()?))
    }

    /// Reads a canonical LEB128 varint of at most 10 bytes.
    pub(crate) fn varint(&mut self) -> Option<u64> {
        let mut v: u64 = 0;
        for i in 0..10 {
            let b = self.u8()?;
            let bits = u64::from(b & 0x7f);
            if i == 9 && bits > 1 {
                return None;
            }
            v |= bits << (7 * i);
            if b & 0x80 == 0 {
                // Reject non-minimal encodings so every value has one representation.
                if i > 0 && b == 0 {
                    return None;
                }
                return Some(v);
            }
        }
        None
    }

    pub(crate) fn varint_usize(&mut self) -> Option<usize> {
        usize::try_from(self.varint()?).ok()
    }
}

pub(crate) const fn varint_len(v: u64) -> usize {
    let bits = 64 - (v | 1).leading_zeros() as usize;
    bits.div_ceil(7)
}

/// Encodes an address into a fixed-size buffer for MAC computation.
pub(crate) fn encode_addr(addr: &SocketAddr, out: &mut [u8; 19]) -> usize {
    match addr {
        SocketAddr::V4(a) => {
            out[0] = 4;
            out[1..5].copy_from_slice(&a.ip().octets());
            out[5..7].copy_from_slice(&a.port().to_le_bytes());
            7
        }
        SocketAddr::V6(a) => {
            out[0] = 6;
            out[1..17].copy_from_slice(&a.ip().octets());
            out[17..19].copy_from_slice(&a.port().to_le_bytes());
            19
        }
    }
}

/// Every packet type except `DATA`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Control<'a> {
    ConnectRequest {
        protocol_id: u32,
        version: u16,
        nonce: u64,
    },
    Challenge {
        nonce: u64,
        timestamp: u32,
        mac: u64,
    },
    ChallengeResponse {
        nonce: u64,
        timestamp: u32,
        mac: u64,
        data: &'a [u8],
    },
    Accept {
        nonce: u64,
        conn_id: u32,
        session_key: u64,
    },
    Reject {
        nonce: u64,
        reason: u8,
        data: &'a [u8],
    },
    Disconnect {
        conn_id: u32,
        data: &'a [u8],
    },
}

impl Control<'_> {
    pub(crate) fn encode(&self, out: &mut [u8]) -> Result<usize, Full> {
        let mut w = Writer::new(out);
        match *self {
            Control::ConnectRequest {
                protocol_id,
                version,
                nonce,
            } => {
                w.u8(TYPE_CONNECT_REQUEST)?;
                w.u32(protocol_id)?;
                w.u16(version)?;
                w.u64(nonce)?;
                w.zeros(CONNECT_REQUEST_SIZE - w.pos())?;
            }
            Control::Challenge { nonce, timestamp, mac } => {
                w.u8(TYPE_CHALLENGE)?;
                w.u64(nonce)?;
                w.u32(timestamp)?;
                w.u64(mac)?;
            }
            Control::ChallengeResponse {
                nonce,
                timestamp,
                mac,
                data,
            } => {
                w.u8(TYPE_CHALLENGE_RESPONSE)?;
                w.u64(nonce)?;
                w.u32(timestamp)?;
                w.u64(mac)?;
                w.varint(data.len() as u64)?;
                w.bytes(data)?;
            }
            Control::Accept {
                nonce,
                conn_id,
                session_key,
            } => {
                w.u8(TYPE_ACCEPT)?;
                w.u64(nonce)?;
                w.u32(conn_id)?;
                w.u64(session_key)?;
            }
            Control::Reject { nonce, reason, data } => {
                w.u8(TYPE_REJECT)?;
                w.u64(nonce)?;
                w.u8(reason)?;
                w.varint(data.len() as u64)?;
                w.bytes(data)?;
            }
            Control::Disconnect { conn_id, data } => {
                w.u8(TYPE_DISCONNECT)?;
                w.u32(conn_id)?;
                w.varint(data.len() as u64)?;
                w.bytes(data)?;
            }
        }
        Ok(w.pos())
    }

    /// Parses a control packet. Returns `None` for `DATA`, unknown types and malformed input.
    pub(crate) fn decode(buf: &[u8]) -> Option<Control<'_>> {
        let mut r = Reader::new(buf);
        let ty = r.u8()?;
        let c = match ty {
            TYPE_CONNECT_REQUEST => {
                if buf.len() < CONNECT_REQUEST_SIZE {
                    return None;
                }
                let c = Control::ConnectRequest {
                    protocol_id: r.u32()?,
                    version: r.u16()?,
                    nonce: r.u64()?,
                };
                r.rest();
                c
            }
            TYPE_CHALLENGE => Control::Challenge {
                nonce: r.u64()?,
                timestamp: r.u32()?,
                mac: r.u64()?,
            },
            TYPE_CHALLENGE_RESPONSE => {
                let nonce = r.u64()?;
                let timestamp = r.u32()?;
                let mac = r.u64()?;
                let len = r.varint_usize()?;
                Control::ChallengeResponse {
                    nonce,
                    timestamp,
                    mac,
                    data: r.bytes(len)?,
                }
            }
            TYPE_ACCEPT => Control::Accept {
                nonce: r.u64()?,
                conn_id: r.u32()?,
                session_key: r.u64()?,
            },
            TYPE_REJECT => {
                let nonce = r.u64()?;
                let reason = r.u8()?;
                let len = r.varint_usize()?;
                Control::Reject {
                    nonce,
                    reason,
                    data: r.bytes(len)?,
                }
            }
            TYPE_DISCONNECT => {
                let conn_id = r.u32()?;
                let len = r.varint_usize()?;
                Control::Disconnect {
                    conn_id,
                    data: r.bytes(len)?,
                }
            }
            _ => return None,
        };
        // Trailing garbage makes the packet invalid.
        r.is_empty().then_some(c)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DataHeader {
    pub(crate) flags: u8,
    pub(crate) seq: u16,
    /// `(ack, ack_bits)` when `FLAG_ACK` is set.
    pub(crate) ack: Option<(u16, u16)>,
}

impl DataHeader {
    pub(crate) fn encode(&self, w: &mut Writer<'_>) -> Result<(), Full> {
        let mut flags = self.flags & !FLAG_ACK;
        if self.ack.is_some() {
            flags |= FLAG_ACK;
        }
        w.u8(flags | TYPE_DATA)?;
        w.u16(self.seq)?;
        if let Some((ack, bits)) = self.ack {
            w.u16(ack)?;
            w.u16(bits)?;
        }
        Ok(())
    }

    pub(crate) fn decode(r: &mut Reader<'_>) -> Option<DataHeader> {
        let b0 = r.u8()?;
        if b0 & 0x0f != TYPE_DATA || b0 & 0xf0 & !KNOWN_DATA_FLAGS != 0 {
            return None;
        }
        let flags = b0 & 0xf0;
        let seq = r.u16()?;
        let ack = if flags & FLAG_ACK != 0 {
            Some((r.u16()?, r.u16()?))
        } else {
            None
        };
        Some(DataHeader { flags, seq, ack })
    }
}

/// Header of a message block: channel, tail flag and message count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockHeader {
    pub(crate) channel: u8,
    pub(crate) tail: bool,
    pub(crate) count: usize,
}

impl BlockHeader {
    pub(crate) const fn encoded_len(count: usize) -> usize {
        if count <= 7 { 1 } else { 1 + varint_len(count as u64) }
    }

    pub(crate) fn encode(&self, w: &mut Writer<'_>) -> Result<(), Full> {
        debug_assert!(self.channel < 16 && self.count > 0);
        let small = if self.count <= 7 { self.count as u8 } else { 0 };
        w.u8((small << 5) | (u8::from(self.tail) << 4) | self.channel)?;
        if small == 0 {
            w.varint(self.count as u64)?;
        }
        Ok(())
    }

    pub(crate) fn decode(r: &mut Reader<'_>) -> Option<BlockHeader> {
        let b = r.u8()?;
        let channel = b & 0x0f;
        let tail = b & 0x10 != 0;
        let small = usize::from(b >> 5);
        let count = if small == 0 {
            let c = r.varint_usize()?;
            if c <= 7 {
                return None;
            }
            c
        } else {
            small
        };
        Some(BlockHeader { channel, tail, count })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip_and_len() {
        let mut buf = [0u8; 10];
        for v in [0u64, 1, 127, 128, 16383, 16384, u32::MAX as u64, u64::MAX] {
            let mut w = Writer::new(&mut buf);
            w.varint(v).unwrap();
            let n = w.pos();
            assert_eq!(n, varint_len(v), "{v}");
            let mut r = Reader::new(&buf[..n]);
            assert_eq!(r.varint(), Some(v));
            assert!(r.is_empty());
        }
    }

    #[test]
    fn varint_rejects_overlong_and_non_minimal() {
        assert_eq!(Reader::new(&[0x80, 0x00]).varint(), None);
        assert_eq!(Reader::new(&[0xff; 11]).varint(), None);
        assert_eq!(Reader::new(&[0x80]).varint(), None);
    }

    #[test]
    fn b1_data_header_size() {
        let mut buf = [0u8; 16];
        let mut w = Writer::new(&mut buf);
        DataHeader {
            flags: 0,
            seq: 7,
            ack: None,
        }
        .encode(&mut w)
        .unwrap();
        assert_eq!(w.pos(), 3);
        assert!(w.pos() <= 3, "B1: unreliable header must be <= 3 bytes");
    }

    #[test]
    fn b2_data_header_with_ack_size() {
        let mut buf = [0u8; 16];
        let mut w = Writer::new(&mut buf);
        let h = DataHeader {
            flags: FLAG_PING,
            seq: 65535,
            ack: Some((65000, 0xbeef)),
        };
        h.encode(&mut w).unwrap();
        let n = w.pos();
        assert!(n <= 8, "B2: header with ack must be <= 8 bytes");
        let mut r = Reader::new(&buf[..n]);
        let d = DataHeader::decode(&mut r).unwrap();
        assert_eq!(d.seq, 65535);
        assert_eq!(d.ack, Some((65000, 0xbeef)));
        assert_eq!(d.flags, FLAG_PING | FLAG_ACK);
    }

    #[test]
    fn data_header_rejects_unknown_flags() {
        assert!(DataHeader::decode(&mut Reader::new(&[0x40, 0, 0])).is_none());
        assert!(DataHeader::decode(&mut Reader::new(&[0x01, 0, 0])).is_none());
    }

    #[test]
    fn block_header_roundtrip() {
        let mut buf = [0u8; 8];
        for count in [1usize, 7, 8, 127, 128, 1000] {
            for tail in [false, true] {
                let h = BlockHeader {
                    channel: 15,
                    tail,
                    count,
                };
                let mut w = Writer::new(&mut buf);
                h.encode(&mut w).unwrap();
                assert_eq!(w.pos(), BlockHeader::encoded_len(count));
                let n = w.pos();
                assert_eq!(BlockHeader::decode(&mut Reader::new(&buf[..n])), Some(h));
            }
        }
    }

    #[test]
    fn control_roundtrip() {
        let data = [1u8, 2, 3];
        let packets = [
            Control::ConnectRequest {
                protocol_id: 0xdead_beef,
                version: WIRE_VERSION,
                nonce: 42,
            },
            Control::Challenge {
                nonce: 1,
                timestamp: 2,
                mac: 3,
            },
            Control::ChallengeResponse {
                nonce: 1,
                timestamp: 2,
                mac: 3,
                data: &data,
            },
            Control::Accept {
                nonce: 1,
                conn_id: 9,
                session_key: 10,
            },
            Control::Reject {
                nonce: 1,
                reason: REJECT_BY_APP,
                data: &data,
            },
            Control::Disconnect {
                conn_id: 5,
                data: &data,
            },
        ];
        let mut buf = [0u8; 64];
        for p in packets {
            let n = p.encode(&mut buf).unwrap();
            assert_eq!(Control::decode(&buf[..n]), Some(p.clone()));
            // Truncated input never parses.
            for cut in 0..n {
                assert_eq!(Control::decode(&buf[..cut]), None, "{p:?} cut {cut}");
            }
        }
    }

    #[test]
    fn s1_pre_validation_replies_are_smaller_than_request() {
        let mut buf = [0u8; 64];
        let req = Control::ConnectRequest {
            protocol_id: 1,
            version: 0,
            nonce: 1,
        }
        .encode(&mut buf)
        .unwrap();
        assert_eq!(req, CONNECT_REQUEST_SIZE);
        let challenge = Control::Challenge {
            nonce: u64::MAX,
            timestamp: u32::MAX,
            mac: u64::MAX,
        }
        .encode(&mut buf)
        .unwrap();
        let reject = Control::Reject {
            nonce: u64::MAX,
            reason: REJECT_SERVER_FULL,
            data: &[],
        }
        .encode(&mut buf)
        .unwrap();
        assert!(challenge <= req && reject <= req);
    }
}
