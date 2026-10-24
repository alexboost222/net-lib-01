use alloc::vec;
use alloc::vec::Vec;
use core::time::Duration;

/// How messages of a channel are delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeliveryMode {
    /// No guarantees: messages may be lost or reordered. Duplicates are filtered.
    Unreliable,
    /// Like `Unreliable`, but a message older than one already delivered on the channel is dropped.
    UnreliableSequenced,
    /// Every message is delivered exactly once, in any order.
    ReliableUnordered,
    /// Every message is delivered exactly once, in send order.
    ReliableOrdered,
    /// The latest message is guaranteed to arrive; older ones may be skipped.
    ReliableSequenced,
}

impl DeliveryMode {
    /// The mode retransmits lost messages.
    pub fn is_reliable(self) -> bool {
        matches!(
            self,
            DeliveryMode::ReliableUnordered | DeliveryMode::ReliableOrdered | DeliveryMode::ReliableSequenced
        )
    }
}

/// Configuration of one channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelConfig {
    /// Delivery guarantees.
    pub mode: DeliveryMode,
    /// Base send priority; higher is sent first when bandwidth is short.
    pub priority: u8,
}

impl ChannelConfig {
    /// A channel with the given mode and default priority (128).
    pub fn new(mode: DeliveryMode) -> Self {
        ChannelConfig { mode, priority: 128 }
    }
}

/// Configuration of a [`Manager`](crate::Manager).
#[derive(Debug, Clone)]
pub struct Config {
    /// Application id. Peers with a different id cannot connect to each other.
    pub protocol_id: u32,
    /// Maximum UDP payload size in bytes.
    pub mtu: usize,
    /// Accept incoming connections (server role).
    pub accept_incoming: bool,
    /// Maximum number of incoming peers, pending ones included.
    pub max_incoming: usize,
    /// Channels, indexed by their position. At most 16.
    pub channels: Vec<ChannelConfig>,
    /// Interval between handshake retransmissions.
    pub connect_retry: Duration,
    /// Time until an unanswered connection attempt fails.
    pub connect_timeout: Duration,
    /// Time a handshake cookie stays valid.
    pub cookie_lifetime: Duration,
    /// Maximum interval between PING-flagged packets; also the keepalive interval.
    pub ping_interval: Duration,
    /// Time without incoming packets until a connection is dropped.
    pub timeout: Duration,
    /// Lower bound of the retransmission timeout.
    pub min_rto: Duration,
    /// Upper bound of the retransmission timeout.
    pub max_rto: Duration,
    /// Maximum size of application data in connect, reject and disconnect packets.
    pub max_connect_data: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            protocol_id: 0,
            mtu: 1200,
            accept_incoming: false,
            max_incoming: 64,
            channels: vec![ChannelConfig::new(DeliveryMode::Unreliable)],
            connect_retry: Duration::from_millis(250),
            connect_timeout: Duration::from_secs(5),
            cookie_lifetime: Duration::from_secs(10),
            ping_interval: Duration::from_secs(1),
            timeout: Duration::from_secs(10),
            min_rto: Duration::from_millis(30),
            max_rto: Duration::from_secs(2),
            max_connect_data: 256,
        }
    }
}

/// Smallest supported MTU (IPv4 minimum reassembly size).
pub const MIN_MTU: usize = 576;
/// Largest supported MTU (maximum UDP payload over IPv4).
pub const MAX_MTU: usize = 65507;
/// Maximum number of channels.
pub const MAX_CHANNELS: usize = 16;

/// Invalid [`Config`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// `mtu` is outside `MIN_MTU..=MAX_MTU`.
    Mtu,
    /// No channels or more than `MAX_CHANNELS`.
    ChannelCount,
    /// The delivery mode is not implemented yet.
    UnsupportedMode(DeliveryMode),
    /// A timing parameter is zero or inconsistent.
    Timing,
    /// `max_connect_data` does not fit into a minimal-MTU packet.
    ConnectData,
}

impl core::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ConfigError::Mtu => write!(f, "mtu must be in {MIN_MTU}..={MAX_MTU}"),
            ConfigError::ChannelCount => write!(f, "between 1 and {MAX_CHANNELS} channels required"),
            ConfigError::UnsupportedMode(m) => write!(f, "delivery mode {m:?} is not supported yet"),
            ConfigError::Timing => write!(f, "invalid timing parameters"),
            ConfigError::ConnectData => write!(f, "max_connect_data is too large"),
        }
    }
}

impl core::error::Error for ConfigError {}

impl Config {
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if !(MIN_MTU..=MAX_MTU).contains(&self.mtu) {
            return Err(ConfigError::Mtu);
        }
        if self.channels.is_empty() || self.channels.len() > MAX_CHANNELS {
            return Err(ConfigError::ChannelCount);
        }
        if let Some(c) = self.channels.iter().find(|c| c.mode.is_reliable()) {
            return Err(ConfigError::UnsupportedMode(c.mode));
        }
        let zero = Duration::ZERO;
        if self.connect_retry == zero
            || self.connect_timeout == zero
            || self.ping_interval == zero
            || self.timeout <= self.ping_interval
            || self.cookie_lifetime < Duration::from_secs(1)
            || self.min_rto > self.max_rto
        {
            return Err(ConfigError::Timing);
        }
        // Largest control packet: type + nonce + timestamp + mac + varint len + data.
        if 1 + 8 + 4 + 8 + 3 + self.max_connect_data > MIN_MTU {
            return Err(ConfigError::ConnectData);
        }
        Ok(())
    }

    /// Largest message accepted by `send` on a channel with the given mode.
    pub fn max_message_size(&self, mode: DeliveryMode) -> usize {
        // A message alone in a packet sits in a tail block without a length prefix.
        let _ = mode;
        self.mtu - crate::wire::DATA_HEADER_MAX - 1
    }
}
