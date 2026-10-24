//! Sans-IO core of netlib: a UDP transport for fast-paced multiplayer games.
//!
//! The core owns no sockets, threads or clocks. The caller feeds it received datagrams and
//! the current [`Time`], and takes outgoing datagrams and [`Event`]s back. The `netlib` crate
//! wraps it with a `std::net::UdpSocket`; tests drive it on virtual time.
//!
//! The wire format is described in `docs/WIRE_FORMAT.md`.
#![no_std]
#![cfg_attr(docsrs, feature(doc_cfg))]

extern crate alloc;
#[cfg(test)]
extern crate std;

mod config;
mod event;
mod manager;
mod rtt;
mod seq;
mod siphash;
mod time;
mod wire;

pub use config::{ChannelConfig, Config, ConfigError, DeliveryMode, MAX_CHANNELS, MAX_MTU, MIN_MTU};
pub use event::{DisconnectReason, Event, PeerId};
pub use manager::{ConnectError, Manager, ManagerStats, PeerError, PeerState, PeerStats, SendError};
pub use time::Time;
