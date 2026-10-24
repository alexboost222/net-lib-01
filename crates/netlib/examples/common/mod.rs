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
