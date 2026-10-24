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
    /// Base send priority; higher is sent first when a packet or the bandwidth budget
    /// is short. Waiting messages gain priority over time (`Config::starvation_time`).
    pub priority: u8,
    /// Unreliable channels only: a message not sent within this time is dropped.
    /// Matters only when the bandwidth budget holds messages back.
    pub ttl: Option<Duration>,
}

impl ChannelConfig {
    /// A channel with the given mode, priority 128 and, for unreliable modes, a TTL of 1 s.
    pub fn new(mode: DeliveryMode) -> Self {
        ChannelConfig {
            mode,
            priority: 128,
            ttl: (!mode.is_reliable()).then_some(Duration::from_secs(1)),
        }
    }

    /// Sets the priority.
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Sets the TTL of unreliable messages.
    pub fn with_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.ttl = ttl;
        self
    }
}

/// Per-message overrides for [`Manager::send_with`](crate::Manager::send_with).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SendOptions {
    /// Overrides the channel priority.
    pub priority: Option<u8>,
    /// Unreliable only: overrides the channel TTL.
    pub ttl: Option<Duration>,
    /// Unreliable only: a queued, not yet sent message of the channel with the same key is
    /// replaced (e.g. the key is an entity id and the message its latest state).
    pub replace_key: Option<u32>,
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
    /// Largest ack delay reported by the remote side that is subtracted from RTT samples.
    /// Larger reports are capped, which limits how much a peer can lower its measured RTT.
    pub max_ack_delay: Duration,
    /// Maximum size of application data in connect, reject and disconnect packets.
    pub max_connect_data: usize,
    /// Largest message on a reliable channel; larger than one packet means fragmentation.
    pub max_reliable_message: usize,
    /// Reliable messages per channel waiting for an ack before `send` fails with `QueueFull`.
    pub max_reliable_queue: usize,
    /// Bandwidth budget per peer in bytes per second (UDP payload); `None` is unlimited.
    /// Acks and pings are always sent.
    pub bandwidth: Option<u32>,
    /// Waiting time after which any queued message reaches the top priority (255), so low
    /// priority traffic cannot starve.
    pub starvation_time: Duration,
    /// Client side: after this long without packets from the server, outgoing packets carry
    /// migration fields (12 bytes) in case our address changed unnoticed.
    pub migration_probe: Duration,
    /// Interval of the periodic per-peer statistics log event; `None` disables it.
    pub stats_interval: Option<Duration>,
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
            max_ack_delay: Duration::from_millis(100),
            max_connect_data: 256,
            max_reliable_message: 64 * 1024,
            max_reliable_queue: 4096,
            bandwidth: None,
            starvation_time: Duration::from_millis(500),
            migration_probe: Duration::from_millis(1500),
            stats_interval: Some(Duration::from_secs(5)),
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
    /// A timing parameter is zero or inconsistent.
    Timing,
    /// `max_connect_data` does not fit into a minimal-MTU packet.
    ConnectData,
    /// Reliable limits, starvation time or bandwidth are out of range.
    ReliableLimits,
}

impl core::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ConfigError::Mtu => write!(f, "mtu must be in {MIN_MTU}..={MAX_MTU}"),
            ConfigError::ChannelCount => write!(f, "between 1 and {MAX_CHANNELS} channels required"),
            ConfigError::Timing => write!(f, "invalid timing parameters"),
            ConfigError::ConnectData => write!(f, "max_connect_data is too large"),
            ConfigError::ReliableLimits => write!(f, "invalid reliable message limits"),
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
        let zero = Duration::ZERO;
        if self.connect_retry == zero
            || self.connect_timeout == zero
            || self.ping_interval == zero
            || self.timeout <= self.ping_interval
            || self.cookie_lifetime < Duration::from_secs(1)
            || self.min_rto > self.max_rto
            || self.max_ack_delay > crate::wire::MAX_WIRE_ACK_DELAY
            || self.migration_probe <= self.ping_interval
        {
            return Err(ConfigError::Timing);
        }
        // Largest control packet: type + nonce + timestamp + mac + varint len + data.
        if 1 + 8 + 4 + 8 + 3 + self.max_connect_data > MIN_MTU {
            return Err(ConfigError::ConnectData);
        }
        if self.max_reliable_message > crate::channel::MAX_FRAGMENTED_TOTAL
            || self.max_reliable_queue == 0
            || self.starvation_time == Duration::ZERO
            || self.bandwidth == Some(0)
        {
            return Err(ConfigError::ReliableLimits);
        }
        Ok(())
    }

    /// Largest message accepted by `send` on a channel with the given mode.
    pub fn max_message_size(&self, mode: DeliveryMode) -> usize {
        if mode.is_reliable() {
            self.max_reliable_message
        } else {
            // A message alone in a packet sits in a tail block without a length prefix.
            self.mtu - crate::wire::DATA_HEADER_MAX - 1
        }
    }

    /// Largest reliable message sent without fragmentation: it fits an empty packet with
    /// header, block header, base seq, a 3-byte length and a 1-byte tag.
    pub(crate) fn single_reliable_limit(&self) -> usize {
        self.mtu - crate::wire::DATA_HEADER_MAX - 1 - 2 - 3 - 1
    }
}
