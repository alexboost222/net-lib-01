use core::net::SocketAddr;

/// Identifies a peer within a [`Manager`](crate::Manager). Ids are not reused while
/// the manager lives, so a stale id never addresses a different peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerId {
    pub(crate) index: u32,
    pub(crate) generation: u32,
}

impl PeerId {
    /// Packs the id into a `u64`, e.g. for FFI.
    pub fn to_bits(self) -> u64 {
        (u64::from(self.generation) << 32) | u64::from(self.index)
    }

    /// Inverse of [`to_bits`](Self::to_bits).
    pub fn from_bits(bits: u64) -> Self {
        PeerId {
            index: bits as u32,
            generation: (bits >> 32) as u32,
        }
    }
}

/// Why a peer went away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisconnectReason<'a> {
    /// No packets were received for `Config::timeout`.
    Timeout,
    /// The remote side disconnected.
    Remote {
        /// Data passed to the remote `disconnect` call.
        data: &'a [u8],
    },
    /// `Manager::disconnect` was called locally.
    Local,
    /// The connection attempt was not answered within `Config::connect_timeout`.
    ConnectTimeout,
    /// The server rejected the connection.
    Rejected {
        /// Data passed to the server's `reject` call.
        data: &'a [u8],
    },
    /// The server has no free slots.
    ServerFull,
    /// The server speaks a different wire format version.
    VersionMismatch,
    /// The remote client started a new connection from the same address.
    Reconnected,
}

/// Something the application should react to. Returned by `Manager::poll_event`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event<'a> {
    /// A client passed address validation and wants to connect.
    /// Answer with `Manager::accept` or `Manager::reject`.
    ConnectRequest {
        /// The pending peer.
        peer: PeerId,
        /// Its address.
        addr: SocketAddr,
        /// Data passed to the client's `connect` call.
        data: &'a [u8],
    },
    /// A connection was established (on either side).
    Connected {
        /// The peer.
        peer: PeerId,
        /// Its address.
        addr: SocketAddr,
    },
    /// A connection ended. The peer id is invalid from now on.
    Disconnected {
        /// The peer.
        peer: PeerId,
        /// Why.
        reason: DisconnectReason<'a>,
    },
    /// A message arrived.
    Message {
        /// The sender.
        peer: PeerId,
        /// Channel index.
        channel: u8,
        /// Payload.
        data: &'a [u8],
    },
}
