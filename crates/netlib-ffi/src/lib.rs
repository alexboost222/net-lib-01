//! C ABI of netlib.
//!
//! The API is poll based: no callbacks, no threads owned by the caller, only blittable
//! structs. This keeps it usable from C, C# (including IL2CPP ahead-of-time builds) and other
//! languages. All functions must be called from one thread per manager.
//!
//! Conventions:
//! - functions return `NETLIB_OK` (0) or a negative `NETLIB_ERR_*` code;
//! - peers are `u64` ids;
//! - addresses are UTF-8, NUL-terminated strings such as `"127.0.0.1:7777"` or `"[::1]:7777"`;
//! - event data stays valid until the next call to `netlib_poll` or `netlib_manager_destroy`.
#![allow(clippy::missing_safety_doc)]

use std::collections::VecDeque;
use std::ffi::{CStr, c_char};
use std::net::SocketAddr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use netlib::log::{Level, LogSink, Record, Value};
use netlib::{
    BindError, ChannelConfig, Config, ConnectError, DeliveryMode, DisconnectReason, DriverConfig, Event, Manager,
    PeerError, PeerId, SendError, SendOptions,
};

/// Success.
pub const NETLIB_OK: i32 = 0;
/// A pointer was null, a string was not UTF-8 or a value was out of range.
pub const NETLIB_ERR_INVALID_ARGUMENT: i32 = -1;
/// A socket operation failed.
pub const NETLIB_ERR_IO: i32 = -2;
/// The configuration is invalid.
pub const NETLIB_ERR_CONFIG: i32 = -3;
/// The peer does not exist or is not connected.
pub const NETLIB_ERR_NOT_CONNECTED: i32 = -4;
/// No channel with this index.
pub const NETLIB_ERR_INVALID_CHANNEL: i32 = -5;
/// The message is larger than the channel allows.
pub const NETLIB_ERR_TOO_LARGE: i32 = -6;
/// The reliable queue of the channel is full.
pub const NETLIB_ERR_QUEUE_FULL: i32 = -7;
/// Send options do not apply to this channel.
pub const NETLIB_ERR_INVALID_OPTIONS: i32 = -8;
/// The peer does not exist.
pub const NETLIB_ERR_UNKNOWN_PEER: i32 = -9;
/// The peer is not in a state that allows the operation.
pub const NETLIB_ERR_INVALID_STATE: i32 = -10;
/// Connect, reject or disconnect data is too large.
pub const NETLIB_ERR_DATA_TOO_LARGE: i32 = -11;
/// A peer with this address already exists.
pub const NETLIB_ERR_ALREADY_EXISTS: i32 = -12;
/// An internal error; the manager should be destroyed.
pub const NETLIB_ERR_INTERNAL: i32 = -13;

/// `NetlibChannelConfig::mode` values.
pub const NETLIB_UNRELIABLE: u8 = 0;
/// See [`NETLIB_UNRELIABLE`].
pub const NETLIB_UNRELIABLE_SEQUENCED: u8 = 1;
/// See [`NETLIB_UNRELIABLE`].
pub const NETLIB_RELIABLE_UNORDERED: u8 = 2;
/// See [`NETLIB_UNRELIABLE`].
pub const NETLIB_RELIABLE_ORDERED: u8 = 3;
/// See [`NETLIB_UNRELIABLE`].
pub const NETLIB_RELIABLE_SEQUENCED: u8 = 4;

/// `NetlibEvent::kind` values.
pub const NETLIB_EVENT_CONNECT_REQUEST: u32 = 1;
/// See [`NETLIB_EVENT_CONNECT_REQUEST`].
pub const NETLIB_EVENT_CONNECTED: u32 = 2;
/// See [`NETLIB_EVENT_CONNECT_REQUEST`].
pub const NETLIB_EVENT_DISCONNECTED: u32 = 3;
/// See [`NETLIB_EVENT_CONNECT_REQUEST`].
pub const NETLIB_EVENT_MESSAGE: u32 = 4;

/// `NetlibEvent::reason` values for disconnects.
pub const NETLIB_REASON_TIMEOUT: u32 = 1;
/// See [`NETLIB_REASON_TIMEOUT`].
pub const NETLIB_REASON_REMOTE: u32 = 2;
/// See [`NETLIB_REASON_TIMEOUT`].
pub const NETLIB_REASON_LOCAL: u32 = 3;
/// See [`NETLIB_REASON_TIMEOUT`].
pub const NETLIB_REASON_CONNECT_TIMEOUT: u32 = 4;
/// See [`NETLIB_REASON_TIMEOUT`].
pub const NETLIB_REASON_REJECTED: u32 = 5;
/// See [`NETLIB_REASON_TIMEOUT`].
pub const NETLIB_REASON_SERVER_FULL: u32 = 6;
/// See [`NETLIB_REASON_TIMEOUT`].
pub const NETLIB_REASON_VERSION_MISMATCH: u32 = 7;
/// See [`NETLIB_REASON_TIMEOUT`].
pub const NETLIB_REASON_RECONNECTED: u32 = 8;

/// `NetlibLogProperty::kind` values.
pub const NETLIB_LOG_U64: u8 = 0;
/// See [`NETLIB_LOG_U64`].
pub const NETLIB_LOG_I64: u8 = 1;
/// See [`NETLIB_LOG_U64`]; durations are milliseconds.
pub const NETLIB_LOG_F64: u8 = 2;
/// See [`NETLIB_LOG_U64`]; the value is in `u` (0 or 1).
pub const NETLIB_LOG_BOOL: u8 = 3;
/// See [`NETLIB_LOG_U64`]; text and addresses.
pub const NETLIB_LOG_STR: u8 = 4;

/// Log records waiting in native memory at most; newer ones are dropped and counted.
const LOG_QUEUE_LIMIT: usize = 8192;

enum OwnedValue {
    U64(u64),
    I64(i64),
    F64(f64),
    Bool(bool),
    Str(String),
}

struct OwnedRecord {
    level: Level,
    id: u32,
    time_us: u64,
    template: &'static str,
    props: Vec<(&'static str, OwnedValue)>,
}

#[derive(Default)]
struct LogQueue {
    records: VecDeque<OwnedRecord>,
    dropped: u64,
}

/// Copies log records into a queue that `netlib_poll` hands to `netlib_next_log`.
struct QueueSink {
    queue: Arc<Mutex<LogQueue>>,
    min: Level,
}

impl LogSink for QueueSink {
    fn enabled(&self, level: Level) -> bool {
        level >= self.min
    }

    fn log(&mut self, r: &Record<'_>) {
        let mut q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        if q.records.len() >= LOG_QUEUE_LIMIT {
            q.dropped += 1;
            return;
        }
        let props = r
            .properties
            .iter()
            .map(|p| {
                let v = match p.value {
                    Value::U64(v) => OwnedValue::U64(v),
                    Value::I64(v) => OwnedValue::I64(v),
                    Value::F64(v) => OwnedValue::F64(v),
                    Value::Bool(v) => OwnedValue::Bool(v),
                    Value::Str(v) => OwnedValue::Str(v.to_string()),
                    Value::Addr(a) => OwnedValue::Str(a.to_string()),
                    Value::Duration(d) => OwnedValue::F64(netlib::log::duration_ms(d)),
                };
                (p.name, v)
            })
            .collect();
        q.records.push_back(OwnedRecord {
            level: r.template.level,
            id: r.template.id,
            time_us: r.time_us,
            template: r.template.text,
            props,
        });
    }
}

/// One property of a log record. Strings are UTF-8 without a terminator.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct NetlibLogProperty {
    /// PascalCase name.
    pub name: *const u8,
    /// Length of `name`.
    pub name_len: u32,
    /// One of the `NETLIB_LOG_*` constants.
    pub kind: u8,
    /// `NETLIB_LOG_U64` and `NETLIB_LOG_BOOL` values.
    pub u: u64,
    /// `NETLIB_LOG_I64` values.
    pub i: i64,
    /// `NETLIB_LOG_F64` values.
    pub f: f64,
    /// `NETLIB_LOG_STR` values.
    pub str: *const u8,
    /// Length of `str`.
    pub str_len: u32,
}

/// A structured log record: a message template and its property values.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct NetlibLogRecord {
    /// 0 trace, 1 debug, 2 info, 3 warn, 4 error.
    pub level: u8,
    /// Stable id of the template (see docs/LOG_EVENTS.md).
    pub event_id: u32,
    /// Microseconds since the manager was created.
    pub time_us: u64,
    /// Message template, UTF-8.
    pub template_: *const u8,
    /// Length of `template_`.
    pub template_len: u32,
    /// Properties in template order; valid until the next `netlib_next_log` or `netlib_poll`.
    pub properties: *const NetlibLogProperty,
    /// Number of properties.
    pub property_count: u32,
}

/// Opaque manager handle.
pub struct NetlibManager {
    manager: Manager,
    logs: Option<Arc<Mutex<LogQueue>>>,
    log_batch: VecDeque<OwnedRecord>,
    log_current: Option<OwnedRecord>,
    log_props: Vec<NetlibLogProperty>,
}

/// One channel.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct NetlibChannelConfig {
    /// One of the `NETLIB_UNRELIABLE*` / `NETLIB_RELIABLE*` constants.
    pub mode: u8,
    /// Base priority, 0..=255; higher goes first.
    pub priority: u8,
    /// Unreliable channels: TTL of queued messages in ms; 0 means no TTL.
    pub ttl_ms: u32,
}

/// Manager configuration. Fill it with `netlib_config_default` and change what you need.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct NetlibConfig {
    /// Application id; peers with different ids cannot connect.
    pub protocol_id: u32,
    /// Maximum UDP payload in bytes.
    pub mtu: u32,
    /// Non-zero to accept incoming connections (server).
    pub accept_incoming: u8,
    /// Non-zero to receive on a background thread (exact arrival times).
    pub receive_thread: u8,
    /// Maximum number of incoming peers.
    pub max_incoming: u32,
    /// Channels; `channel_count` entries. Null with a count of 0 means one unreliable channel.
    pub channels: *const NetlibChannelConfig,
    /// Number of entries in `channels`, at most 16.
    pub channel_count: u32,
    /// Time without packets until a connection drops, ms.
    pub timeout_ms: u32,
    /// Keepalive and ping interval, ms.
    pub ping_interval_ms: u32,
    /// Time until an unanswered connection attempt fails, ms.
    pub connect_timeout_ms: u32,
    /// Bandwidth budget per peer in bytes per second; 0 is unlimited.
    pub bandwidth: u32,
    /// Waiting time after which a queued message reaches the top priority, ms.
    pub starvation_time_ms: u32,
    /// Largest reliable message in bytes.
    pub max_reliable_message: u32,
    /// Unacknowledged reliable messages per channel before sends fail with QUEUE_FULL.
    pub max_reliable_queue: u32,
}

/// Per-message send options.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct NetlibSendOptions {
    /// Priority override; negative uses the channel priority.
    pub priority: i16,
    /// Unreliable only: TTL override in ms; 0 uses the channel TTL.
    pub ttl_ms: u32,
    /// Unreliable only: non-zero to replace a queued message with the same `replace_key`.
    pub has_replace_key: u8,
    /// See `has_replace_key`.
    pub replace_key: u32,
}

/// A socket address.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct NetlibAddress {
    /// 4 or 6; 0 if absent.
    pub family: u8,
    /// IPv4 in the first 4 bytes, or IPv6.
    pub ip: [u8; 16],
    /// Port.
    pub port: u16,
}

/// An event; see `netlib_next_event`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct NetlibEvent {
    /// One of the `NETLIB_EVENT_*` constants.
    pub kind: u32,
    /// For `NETLIB_EVENT_DISCONNECTED`: one of the `NETLIB_REASON_*` constants.
    pub reason: u32,
    /// The peer.
    pub peer: u64,
    /// For messages: the channel.
    pub channel: u8,
    /// Message, connect, reject or disconnect data; valid until the next `netlib_poll`.
    pub data: *const u8,
    /// Length of `data`.
    pub data_len: u32,
    /// For connect requests and connections: the remote address.
    pub addr: NetlibAddress,
}

/// Counters of a peer.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct NetlibPeerStats {
    /// Smoothed network RTT in microseconds; 0 until measured.
    pub rtt_us: u64,
    /// Datagrams sent.
    pub packets_sent: u64,
    /// Datagrams received.
    pub packets_received: u64,
    /// Bytes sent.
    pub bytes_sent: u64,
    /// Bytes received.
    pub bytes_received: u64,
    /// Sent packets declared lost.
    pub packets_lost: u64,
    /// Reliable messages or pieces sent again.
    pub messages_retransmitted: u64,
    /// Messages delivered to the application.
    pub messages_received: u64,
}

fn guard(f: impl FnOnce() -> i32) -> i32 {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or(NETLIB_ERR_INTERNAL)
}

fn send_error(e: SendError) -> i32 {
    match e {
        SendError::NotConnected => NETLIB_ERR_NOT_CONNECTED,
        SendError::InvalidChannel => NETLIB_ERR_INVALID_CHANNEL,
        SendError::TooLarge => NETLIB_ERR_TOO_LARGE,
        SendError::QueueFull => NETLIB_ERR_QUEUE_FULL,
        SendError::InvalidOptions => NETLIB_ERR_INVALID_OPTIONS,
    }
}

fn peer_error(e: PeerError) -> i32 {
    match e {
        PeerError::UnknownPeer => NETLIB_ERR_UNKNOWN_PEER,
        PeerError::InvalidState => NETLIB_ERR_INVALID_STATE,
        PeerError::DataTooLarge => NETLIB_ERR_DATA_TOO_LARGE,
    }
}

fn result(r: Result<(), PeerError>) -> i32 {
    r.map_or_else(peer_error, |()| NETLIB_OK)
}

fn address(a: SocketAddr) -> NetlibAddress {
    let mut out = NetlibAddress {
        port: a.port(),
        ..NetlibAddress::default()
    };
    match a {
        SocketAddr::V4(v4) => {
            out.family = 4;
            out.ip[..4].copy_from_slice(&v4.ip().octets());
        }
        SocketAddr::V6(v6) => {
            out.family = 6;
            out.ip.copy_from_slice(&v6.ip().octets());
        }
    }
    out
}

/// # Safety
/// `s` must be null or a valid NUL-terminated string.
unsafe fn string<'a>(s: *const c_char) -> Option<&'a str> {
    if s.is_null() {
        return None;
    }
    // SAFETY: non-null and NUL-terminated per the caller's contract.
    unsafe { CStr::from_ptr(s) }.to_str().ok()
}

/// # Safety
/// `data` must be null with `len == 0`, or valid for `len` bytes.
unsafe fn bytes<'a>(data: *const u8, len: u32) -> Option<&'a [u8]> {
    if len == 0 {
        return Some(&[]);
    }
    if data.is_null() {
        return None;
    }
    // SAFETY: non-null and valid for `len` bytes per the caller's contract.
    Some(unsafe { std::slice::from_raw_parts(data, len as usize) })
}

/// # Safety
/// `m` must be null or a pointer returned by `netlib_manager_create` and not yet destroyed.
unsafe fn manager<'a>(m: *mut NetlibManager) -> Option<&'a mut Manager> {
    // SAFETY: valid or null per the caller's contract; we hand out the only reference.
    unsafe { m.as_mut() }.map(|m| &mut m.manager)
}

#[cfg(test)]
mod log_tests {
    use super::*;

    #[test]
    fn l_logs_cross_the_c_abi() {
        let c = {
            let mut c = std::mem::MaybeUninit::<NetlibConfig>::uninit();
            // SAFETY: valid out pointer.
            unsafe { netlib_config_default(c.as_mut_ptr()) };
            // SAFETY: initialised above.
            unsafe { c.assume_init() }
        };
        let addr = std::ffi::CString::new("127.0.0.1:0").unwrap();
        let target = std::ffi::CString::new("127.0.0.1:9").unwrap();
        let mut m = std::ptr::null_mut();
        let mut peer = 0;
        // SAFETY: valid pointers; the record is read before the next call.
        unsafe {
            assert_eq!(netlib_manager_create(&c, addr.as_ptr(), &mut m), NETLIB_OK);
            assert_eq!(netlib_set_log_level(m, 1), NETLIB_OK);
            assert_eq!(
                netlib_connect(m, target.as_ptr(), std::ptr::null(), 0, &mut peer),
                NETLIB_OK
            );
            assert_eq!(netlib_poll(m), NETLIB_OK);
            let mut r = std::mem::zeroed::<NetlibLogRecord>();
            assert_eq!(netlib_next_log(m, &mut r), 1);
            let template =
                std::str::from_utf8(std::slice::from_raw_parts(r.template_, r.template_len as usize)).unwrap();
            assert_eq!(template, "Connecting to {RemoteEndpoint} as peer {PeerId}");
            assert_eq!(r.event_id, netlib::log::event_id(template));
            assert_eq!(r.level, 2);
            let props = std::slice::from_raw_parts(r.properties, r.property_count as usize);
            assert_eq!(props.len(), 2);
            assert_eq!(props[0].kind, NETLIB_LOG_STR);
            let ep = std::slice::from_raw_parts(props[0].str, props[0].str_len as usize);
            assert_eq!(ep, b"127.0.0.1:9");
            assert_eq!((props[1].kind, props[1].u), (NETLIB_LOG_U64, peer));
            assert_eq!(netlib_next_log(m, &mut r), 0);
            assert_eq!(netlib_set_log_level(m, -1), NETLIB_OK);
            netlib_manager_destroy(m);
        }
    }
}

/// Fills `out` with the default configuration (client, one unreliable channel).
///
/// # Safety
/// `out` must be valid for writes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_config_default(out: *mut NetlibConfig) -> i32 {
    let d = Config::default();
    let ms = |d: Duration| u32::try_from(d.as_millis()).unwrap_or(u32::MAX);
    let c = NetlibConfig {
        protocol_id: d.protocol_id,
        mtu: d.mtu as u32,
        accept_incoming: 0,
        receive_thread: 1,
        max_incoming: d.max_incoming as u32,
        channels: std::ptr::null(),
        channel_count: 0,
        timeout_ms: ms(d.timeout),
        ping_interval_ms: ms(d.ping_interval),
        connect_timeout_ms: ms(d.connect_timeout),
        bandwidth: 0,
        starvation_time_ms: ms(d.starvation_time),
        max_reliable_message: d.max_reliable_message as u32,
        max_reliable_queue: d.max_reliable_queue as u32,
    };
    // SAFETY: `out` is valid for writes per the contract.
    match unsafe { out.as_mut() } {
        Some(o) => {
            *o = c;
            NETLIB_OK
        }
        None => NETLIB_ERR_INVALID_ARGUMENT,
    }
}

/// # Safety
/// `c.channels` must be null or valid for `c.channel_count` entries.
unsafe fn to_config(c: &NetlibConfig) -> Option<Config> {
    let channels = if c.channel_count == 0 {
        Config::default().channels
    } else {
        if c.channels.is_null() || c.channel_count > 16 {
            return None;
        }
        // SAFETY: non-null and valid for `channel_count` entries per the contract.
        let raw = unsafe { std::slice::from_raw_parts(c.channels, c.channel_count as usize) };
        let mut out = Vec::with_capacity(raw.len());
        for ch in raw {
            let mode = match ch.mode {
                NETLIB_UNRELIABLE => DeliveryMode::Unreliable,
                NETLIB_UNRELIABLE_SEQUENCED => DeliveryMode::UnreliableSequenced,
                NETLIB_RELIABLE_UNORDERED => DeliveryMode::ReliableUnordered,
                NETLIB_RELIABLE_ORDERED => DeliveryMode::ReliableOrdered,
                NETLIB_RELIABLE_SEQUENCED => DeliveryMode::ReliableSequenced,
                _ => return None,
            };
            let ttl = (ch.ttl_ms > 0).then(|| Duration::from_millis(u64::from(ch.ttl_ms)));
            out.push(ChannelConfig::new(mode).with_priority(ch.priority).with_ttl(ttl));
        }
        out
    };
    let ms = |v: u32| Duration::from_millis(u64::from(v));
    Some(Config {
        protocol_id: c.protocol_id,
        mtu: c.mtu as usize,
        accept_incoming: c.accept_incoming != 0,
        max_incoming: c.max_incoming as usize,
        channels,
        timeout: ms(c.timeout_ms),
        ping_interval: ms(c.ping_interval_ms),
        connect_timeout: ms(c.connect_timeout_ms),
        bandwidth: (c.bandwidth > 0).then_some(c.bandwidth),
        starvation_time: ms(c.starvation_time_ms),
        max_reliable_message: c.max_reliable_message as usize,
        max_reliable_queue: c.max_reliable_queue as usize,
        ..Config::default()
    })
}

/// Creates a manager bound to `bind_addr` (use port 0 for clients).
///
/// # Safety
/// `config` must point to a valid `NetlibConfig`, `bind_addr` must be a NUL-terminated
/// string and `out` must be valid for writes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_manager_create(
    config: *const NetlibConfig,
    bind_addr: *const c_char,
    out: *mut *mut NetlibManager,
) -> i32 {
    guard(|| {
        // SAFETY: per the contract.
        let (Some(config), Some(addr), false) =
            (unsafe { config.as_ref() }, unsafe { string(bind_addr) }, out.is_null())
        else {
            return NETLIB_ERR_INVALID_ARGUMENT;
        };
        // SAFETY: `config.channels` is valid per the contract.
        let Some(cfg) = (unsafe { to_config(config) }) else {
            return NETLIB_ERR_INVALID_ARGUMENT;
        };
        let driver = DriverConfig {
            receive_thread: config.receive_thread != 0,
        };
        match Manager::bind_with(addr, cfg, driver) {
            Ok(manager) => {
                let ptr = Box::into_raw(Box::new(NetlibManager {
                    manager,
                    logs: None,
                    log_batch: VecDeque::new(),
                    log_current: None,
                    log_props: Vec::new(),
                }));
                // SAFETY: `out` is non-null and valid for writes per the contract.
                unsafe { *out = ptr };
                NETLIB_OK
            }
            Err(BindError::Config(_)) => NETLIB_ERR_CONFIG,
            Err(BindError::Io(_) | BindError::Random) => NETLIB_ERR_IO,
        }
    })
}

/// Destroys a manager. Pending datagrams are not sent; disconnect peers first.
///
/// # Safety
/// `m` must be null or a pointer from `netlib_manager_create`, destroyed only once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_manager_destroy(m: *mut NetlibManager) {
    if !m.is_null() {
        // SAFETY: created by `Box::into_raw` in `netlib_manager_create`, destroyed once.
        drop(unsafe { Box::from_raw(m) });
    }
}

/// Receives, runs timers and collects events. Call at the start of every tick.
///
/// # Safety
/// `m` must be a live manager.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_poll(m: *mut NetlibManager) -> i32 {
    guard(|| {
        // SAFETY: per the contract.
        let Some(h) = (unsafe { m.as_mut() }) else {
            return NETLIB_ERR_INVALID_ARGUMENT;
        };
        let r = h.manager.poll().map_or(NETLIB_ERR_IO, |()| NETLIB_OK);
        h.log_current = None;
        h.log_batch.clear();
        if let Some(q) = &h.logs {
            let mut q = q.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::swap(&mut q.records, &mut h.log_batch);
        }
        r
    })
}

/// Enables structured logs from `level` up (0 trace .. 4 error), or disables them with a
/// negative level. Records are collected by `netlib_poll` and read with `netlib_next_log`.
///
/// # Safety
/// `m` must be a live manager.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_set_log_level(m: *mut NetlibManager, level: i32) -> i32 {
    guard(|| {
        // SAFETY: per the contract.
        let Some(h) = (unsafe { m.as_mut() }) else {
            return NETLIB_ERR_INVALID_ARGUMENT;
        };
        let level = match level {
            l if l < 0 => None,
            0 => Some(Level::Trace),
            1 => Some(Level::Debug),
            2 => Some(Level::Info),
            3 => Some(Level::Warn),
            4 => Some(Level::Error),
            _ => return NETLIB_ERR_INVALID_ARGUMENT,
        };
        match level {
            None => {
                h.manager.set_log_sink(None);
                h.logs = None;
            }
            Some(min) => {
                let queue = h.logs.get_or_insert_with(Arc::default).clone();
                h.manager.set_log_sink(Some(Box::new(QueueSink { queue, min })));
            }
        }
        NETLIB_OK
    })
}

/// Takes the next log record collected by `netlib_poll`. Returns 1 if `out` was filled, 0 if
/// there are no more. The record stays valid until the next call or `netlib_poll`.
///
/// # Safety
/// `m` must be a live manager and `out` valid for writes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_next_log(m: *mut NetlibManager, out: *mut NetlibLogRecord) -> i32 {
    guard(|| {
        // SAFETY: per the contract.
        let (Some(h), Some(out)) = (unsafe { m.as_mut() }, unsafe { out.as_mut() }) else {
            return NETLIB_ERR_INVALID_ARGUMENT;
        };
        h.log_current = h.log_batch.pop_front();
        let Some(r) = &h.log_current else {
            return 0;
        };
        h.log_props.clear();
        for (name, v) in &r.props {
            let mut p = NetlibLogProperty {
                name: name.as_ptr(),
                name_len: name.len() as u32,
                kind: NETLIB_LOG_U64,
                u: 0,
                i: 0,
                f: 0.0,
                str: std::ptr::null(),
                str_len: 0,
            };
            match v {
                OwnedValue::U64(x) => p.u = *x,
                OwnedValue::I64(x) => {
                    p.kind = NETLIB_LOG_I64;
                    p.i = *x;
                }
                OwnedValue::F64(x) => {
                    p.kind = NETLIB_LOG_F64;
                    p.f = *x;
                }
                OwnedValue::Bool(x) => {
                    p.kind = NETLIB_LOG_BOOL;
                    p.u = u64::from(*x);
                }
                OwnedValue::Str(s) => {
                    p.kind = NETLIB_LOG_STR;
                    p.str = s.as_ptr();
                    p.str_len = s.len() as u32;
                }
            }
            h.log_props.push(p);
        }
        *out = NetlibLogRecord {
            level: r.level as u8,
            event_id: r.id,
            time_us: r.time_us,
            template_: r.template.as_ptr(),
            template_len: r.template.len() as u32,
            properties: h.log_props.as_ptr(),
            property_count: h.log_props.len() as u32,
        };
        1
    })
}

/// Takes the next event collected by `netlib_poll`. Returns 1 if `out` was filled, 0 if
/// there are no more events.
///
/// # Safety
/// `m` must be a live manager and `out` valid for writes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_next_event(m: *mut NetlibManager, out: *mut NetlibEvent) -> i32 {
    guard(|| {
        // SAFETY: per the contract.
        let (Some(m), Some(out)) = (unsafe { manager(m) }, unsafe { out.as_mut() }) else {
            return NETLIB_ERR_INVALID_ARGUMENT;
        };
        let Some(e) = m.poll_event() else {
            return 0;
        };
        let mut ev = NetlibEvent {
            kind: 0,
            reason: 0,
            peer: 0,
            channel: 0,
            data: std::ptr::null(),
            data_len: 0,
            addr: NetlibAddress::default(),
        };
        let set_data = |ev: &mut NetlibEvent, d: &[u8]| {
            ev.data = d.as_ptr();
            ev.data_len = d.len() as u32;
        };
        match e {
            Event::ConnectRequest { peer, addr, data } => {
                ev.kind = NETLIB_EVENT_CONNECT_REQUEST;
                ev.peer = peer.to_bits();
                ev.addr = address(addr);
                set_data(&mut ev, data);
            }
            Event::Connected { peer, addr } => {
                ev.kind = NETLIB_EVENT_CONNECTED;
                ev.peer = peer.to_bits();
                ev.addr = address(addr);
            }
            Event::Disconnected { peer, reason } => {
                ev.kind = NETLIB_EVENT_DISCONNECTED;
                ev.peer = peer.to_bits();
                ev.reason = match reason {
                    DisconnectReason::Timeout => NETLIB_REASON_TIMEOUT,
                    DisconnectReason::Remote { data } => {
                        set_data(&mut ev, data);
                        NETLIB_REASON_REMOTE
                    }
                    DisconnectReason::Local => NETLIB_REASON_LOCAL,
                    DisconnectReason::ConnectTimeout => NETLIB_REASON_CONNECT_TIMEOUT,
                    DisconnectReason::Rejected { data } => {
                        set_data(&mut ev, data);
                        NETLIB_REASON_REJECTED
                    }
                    DisconnectReason::ServerFull => NETLIB_REASON_SERVER_FULL,
                    DisconnectReason::VersionMismatch => NETLIB_REASON_VERSION_MISMATCH,
                    DisconnectReason::Reconnected => NETLIB_REASON_RECONNECTED,
                };
            }
            Event::Message { peer, channel, data } => {
                ev.kind = NETLIB_EVENT_MESSAGE;
                ev.peer = peer.to_bits();
                ev.channel = channel;
                set_data(&mut ev, data);
            }
        }
        // The data points into the manager's event buffer, which is only reused by the
        // next `netlib_poll`.
        *out = ev;
        1
    })
}

/// Packs queued messages into datagrams and sends them. Call at the end of every tick.
///
/// # Safety
/// `m` must be a live manager.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_flush(m: *mut NetlibManager) -> i32 {
    guard(|| {
        // SAFETY: per the contract.
        let Some(m) = (unsafe { manager(m) }) else {
            return NETLIB_ERR_INVALID_ARGUMENT;
        };
        m.flush();
        NETLIB_OK
    })
}

/// Starts connecting to `addr`; the peer id is written to `out_peer`.
///
/// # Safety
/// `m` must be a live manager, `addr` a NUL-terminated string, `data` valid for `len`
/// bytes (or null with 0) and `out_peer` valid for writes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_connect(
    m: *mut NetlibManager,
    addr: *const c_char,
    data: *const u8,
    len: u32,
    out_peer: *mut u64,
) -> i32 {
    guard(|| {
        // SAFETY: per the contract.
        let (Some(m), Some(addr), Some(data), Some(out)) = (
            unsafe { manager(m) },
            unsafe { string(addr) },
            unsafe { bytes(data, len) },
            unsafe { out_peer.as_mut() },
        ) else {
            return NETLIB_ERR_INVALID_ARGUMENT;
        };
        match m.connect(addr, data) {
            Ok(Ok(peer)) => {
                *out = peer.to_bits();
                NETLIB_OK
            }
            Ok(Err(ConnectError::AlreadyExists)) => NETLIB_ERR_ALREADY_EXISTS,
            Ok(Err(ConnectError::DataTooLarge)) => NETLIB_ERR_DATA_TOO_LARGE,
            Err(_) => NETLIB_ERR_INVALID_ARGUMENT,
        }
    })
}

/// Accepts a pending connection.
///
/// # Safety
/// `m` must be a live manager.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_accept(m: *mut NetlibManager, peer: u64) -> i32 {
    guard(|| {
        // SAFETY: per the contract.
        let Some(m) = (unsafe { manager(m) }) else {
            return NETLIB_ERR_INVALID_ARGUMENT;
        };
        result(m.accept(PeerId::from_bits(peer)))
    })
}

/// Rejects a pending connection with optional data.
///
/// # Safety
/// `m` must be a live manager and `data` valid for `len` bytes (or null with 0).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_reject(m: *mut NetlibManager, peer: u64, data: *const u8, len: u32) -> i32 {
    guard(|| {
        // SAFETY: per the contract.
        let (Some(m), Some(data)) = (unsafe { manager(m) }, unsafe { bytes(data, len) }) else {
            return NETLIB_ERR_INVALID_ARGUMENT;
        };
        result(m.reject(PeerId::from_bits(peer), data))
    })
}

/// Closes a connection with optional data for the remote side.
///
/// # Safety
/// `m` must be a live manager and `data` valid for `len` bytes (or null with 0).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_disconnect(m: *mut NetlibManager, peer: u64, data: *const u8, len: u32) -> i32 {
    guard(|| {
        // SAFETY: per the contract.
        let (Some(m), Some(data)) = (unsafe { manager(m) }, unsafe { bytes(data, len) }) else {
            return NETLIB_ERR_INVALID_ARGUMENT;
        };
        result(m.disconnect(PeerId::from_bits(peer), data))
    })
}

/// Queues a message with the channel defaults.
///
/// # Safety
/// `m` must be a live manager and `data` valid for `len` bytes (or null with 0).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_send(m: *mut NetlibManager, peer: u64, channel: u8, data: *const u8, len: u32) -> i32 {
    // SAFETY: forwarded contract.
    unsafe { netlib_send_with(m, peer, channel, data, len, std::ptr::null()) }
}

/// Queues a message with per-message options (`options` may be null).
///
/// # Safety
/// `m` must be a live manager, `data` valid for `len` bytes (or null with 0) and
/// `options` null or valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_send_with(
    m: *mut NetlibManager,
    peer: u64,
    channel: u8,
    data: *const u8,
    len: u32,
    options: *const NetlibSendOptions,
) -> i32 {
    guard(|| {
        // SAFETY: per the contract.
        let (Some(m), Some(data)) = (unsafe { manager(m) }, unsafe { bytes(data, len) }) else {
            return NETLIB_ERR_INVALID_ARGUMENT;
        };
        // SAFETY: null or valid per the contract.
        let o = unsafe { options.as_ref() }.copied().unwrap_or(NetlibSendOptions {
            priority: -1,
            ..NetlibSendOptions::default()
        });
        let options = SendOptions {
            priority: u8::try_from(o.priority).ok(),
            ttl: (o.ttl_ms > 0).then(|| Duration::from_millis(u64::from(o.ttl_ms))),
            replace_key: (o.has_replace_key != 0).then_some(o.replace_key),
        };
        m.send_with(PeerId::from_bits(peer), channel, data, options)
            .map_or_else(send_error, |()| NETLIB_OK)
    })
}

/// Fills `out` with counters of a peer.
///
/// # Safety
/// `m` must be a live manager and `out` valid for writes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_peer_stats(m: *mut NetlibManager, peer: u64, out: *mut NetlibPeerStats) -> i32 {
    guard(|| {
        // SAFETY: per the contract.
        let (Some(m), Some(out)) = (unsafe { manager(m) }, unsafe { out.as_mut() }) else {
            return NETLIB_ERR_INVALID_ARGUMENT;
        };
        let peer = PeerId::from_bits(peer);
        let Some(s) = m.peer_stats(peer) else {
            return NETLIB_ERR_UNKNOWN_PEER;
        };
        *out = NetlibPeerStats {
            rtt_us: m.peer_rtt(peer).map_or(0, |d| d.as_micros() as u64),
            packets_sent: s.packets_sent,
            packets_received: s.packets_received,
            bytes_sent: s.bytes_sent,
            bytes_received: s.bytes_received,
            packets_lost: s.packets_lost,
            messages_retransmitted: s.messages_retransmitted,
            messages_received: s.messages_received,
        };
        NETLIB_OK
    })
}

/// Writes the bound address to `out`.
///
/// # Safety
/// `m` must be a live manager and `out` valid for writes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_local_address(m: *mut NetlibManager, out: *mut NetlibAddress) -> i32 {
    guard(|| {
        // SAFETY: per the contract.
        let (Some(m), Some(out)) = (unsafe { manager(m) }, unsafe { out.as_mut() }) else {
            return NETLIB_ERR_INVALID_ARGUMENT;
        };
        match m.local_addr() {
            Ok(a) => {
                *out = address(a);
                NETLIB_OK
            }
            Err(_) => NETLIB_ERR_IO,
        }
    })
}

/// Recreates the socket (e.g. when the app returns from background); connections continue.
///
/// # Safety
/// `m` must be a live manager.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn netlib_rebind(m: *mut NetlibManager) -> i32 {
    guard(|| {
        // SAFETY: per the contract.
        let Some(m) = (unsafe { manager(m) }) else {
            return NETLIB_ERR_INVALID_ARGUMENT;
        };
        m.rebind().map_or(NETLIB_ERR_IO, |()| NETLIB_OK)
    })
}

/// Version of the library as a NUL-terminated string.
#[unsafe(no_mangle)]
pub extern "C" fn netlib_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr().cast()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::ptr;

    fn create(server: bool, addr: &str, channels: &[NetlibChannelConfig]) -> *mut NetlibManager {
        let mut c = std::mem::MaybeUninit::<NetlibConfig>::uninit();
        // SAFETY: valid out pointer.
        assert_eq!(unsafe { netlib_config_default(c.as_mut_ptr()) }, NETLIB_OK);
        // SAFETY: initialised by netlib_config_default.
        let mut c = unsafe { c.assume_init() };
        c.protocol_id = 42;
        c.accept_incoming = u8::from(server);
        c.channels = channels.as_ptr();
        c.channel_count = channels.len() as u32;
        let addr = CString::new(addr).unwrap();
        let mut m = ptr::null_mut();
        // SAFETY: valid pointers.
        assert_eq!(unsafe { netlib_manager_create(&c, addr.as_ptr(), &mut m) }, NETLIB_OK);
        m
    }

    #[test]
    fn x2_c_abi_roundtrip_over_localhost() {
        let channels = [
            NetlibChannelConfig {
                mode: NETLIB_UNRELIABLE,
                priority: 128,
                ttl_ms: 0,
            },
            NetlibChannelConfig {
                mode: NETLIB_RELIABLE_ORDERED,
                priority: 200,
                ttl_ms: 0,
            },
        ];
        let server = create(true, "127.0.0.1:0", &channels);
        let client = create(false, "127.0.0.1:0", &channels);
        let mut a = NetlibAddress::default();
        // SAFETY: live manager, valid out pointer.
        assert_eq!(unsafe { netlib_local_address(server, &mut a) }, NETLIB_OK);
        let target = CString::new(format!("127.0.0.1:{}", a.port)).unwrap();
        let mut peer = 0u64;
        // SAFETY: valid pointers.
        let r = unsafe { netlib_connect(client, target.as_ptr(), b"hi".as_ptr(), 2, &mut peer) };
        assert_eq!(r, NETLIB_OK);
        let mut got = Vec::new();
        let mut connected = false;
        for _ in 0..200 {
            for (m, is_server) in [(server, true), (client, false)] {
                // SAFETY: live managers, valid pointers; event data is read before the next poll.
                unsafe {
                    assert_eq!(netlib_poll(m), NETLIB_OK);
                    let mut ev = std::mem::zeroed::<NetlibEvent>();
                    while netlib_next_event(m, &mut ev) == 1 {
                        match ev.kind {
                            NETLIB_EVENT_CONNECT_REQUEST => {
                                assert_eq!(std::slice::from_raw_parts(ev.data, ev.data_len as usize), b"hi");
                                assert_eq!(netlib_accept(m, ev.peer), NETLIB_OK);
                            }
                            NETLIB_EVENT_CONNECTED if !is_server => connected = true,
                            NETLIB_EVENT_MESSAGE => {
                                got.push((
                                    ev.channel,
                                    std::slice::from_raw_parts(ev.data, ev.data_len as usize).to_vec(),
                                ));
                            }
                            _ => {}
                        }
                    }
                    if connected && !is_server && got.is_empty() {
                        assert_eq!(netlib_send(m, peer, 0, b"u".as_ptr(), 1), NETLIB_OK);
                        assert_eq!(netlib_send(m, peer, 1, b"r".as_ptr(), 1), NETLIB_OK);
                        assert_eq!(netlib_send(m, peer, 9, b"x".as_ptr(), 1), NETLIB_ERR_INVALID_CHANNEL);
                    }
                    assert_eq!(netlib_flush(m), NETLIB_OK);
                }
            }
            if got.len() >= 2 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        got.sort();
        assert_eq!(got, vec![(0, b"u".to_vec()), (1, b"r".to_vec())]);
        let mut stats = NetlibPeerStats::default();
        // SAFETY: live manager, valid pointer.
        assert_eq!(unsafe { netlib_peer_stats(client, peer, &mut stats) }, NETLIB_OK);
        assert!(stats.packets_sent > 0);
        // SAFETY: created above, destroyed once.
        unsafe {
            assert_eq!(netlib_disconnect(client, peer, ptr::null(), 0), NETLIB_OK);
            netlib_manager_destroy(client);
            netlib_manager_destroy(server);
        }
    }

    #[test]
    fn x2_invalid_arguments_are_rejected() {
        // SAFETY: null pointers are allowed and rejected.
        unsafe {
            assert_eq!(netlib_poll(ptr::null_mut()), NETLIB_ERR_INVALID_ARGUMENT);
            assert_eq!(netlib_config_default(ptr::null_mut()), NETLIB_ERR_INVALID_ARGUMENT);
            netlib_manager_destroy(ptr::null_mut());
            let v = CStr::from_ptr(netlib_version());
            assert_eq!(v.to_str().unwrap(), env!("CARGO_PKG_VERSION"));
        }
        let bad = [NetlibChannelConfig {
            mode: 99,
            priority: 0,
            ttl_ms: 0,
        }];
        let mut c = std::mem::MaybeUninit::<NetlibConfig>::uninit();
        // SAFETY: valid out pointer.
        unsafe { netlib_config_default(c.as_mut_ptr()) };
        // SAFETY: initialised above.
        let mut c = unsafe { c.assume_init() };
        c.channels = bad.as_ptr();
        c.channel_count = 1;
        let addr = CString::new("127.0.0.1:0").unwrap();
        let mut m = ptr::null_mut();
        // SAFETY: valid pointers.
        let r = unsafe { netlib_manager_create(&c, addr.as_ptr(), &mut m) };
        assert_eq!(r, NETLIB_ERR_INVALID_ARGUMENT);
        c.channel_count = 0;
        c.mtu = 10;
        // SAFETY: valid pointers.
        let r = unsafe { netlib_manager_create(&c, addr.as_ptr(), &mut m) };
        assert_eq!(r, NETLIB_ERR_CONFIG);
    }
}
