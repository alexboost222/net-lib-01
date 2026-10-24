//! Shared settings of the echo examples.
#![allow(dead_code)]

use std::time::Duration;

use netlib::{ChannelConfig, Config, DeliveryMode};

pub const PROTOCOL_ID: u32 = 0x4e45_5401;
pub const TICK: Duration = Duration::from_micros(15_625); // 64 Hz

pub fn config(server: bool) -> Config {
    Config {
        protocol_id: PROTOCOL_ID,
        accept_incoming: server,
        channels: vec![
            ChannelConfig::new(DeliveryMode::Unreliable),
            ChannelConfig::new(DeliveryMode::UnreliableSequenced),
        ],
        ..Config::default()
    }
}

/// `NETLIB_LOG=debug` (or trace/info/warn) writes text logs to stderr; `NETLIB_LOG_FORMAT=clef`
/// writes CLEF instead.
pub fn logging(m: &mut netlib::Manager) {
    use netlib::log::Level;
    use netlib::logging::{ClefSink, TextSink, WallClock};
    let level = match std::env::var("NETLIB_LOG")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "trace" => Level::Trace,
        "debug" => Level::Debug,
        "info" => Level::Info,
        "warn" => Level::Warn,
        _ => return,
    };
    let clock = WallClock::new(m.epoch());
    if std::env::var("NETLIB_LOG_FORMAT").is_ok_and(|f| f == "clef") {
        m.set_log_sink(Some(Box::new(ClefSink::new(std::io::stderr(), level, clock))));
    } else {
        m.set_log_sink(Some(Box::new(TextSink::new(std::io::stderr(), level, clock))));
    }
}
