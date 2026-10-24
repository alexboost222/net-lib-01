//! Echo server: accepts every client and sends each message back on the same channel.
//!
//! cargo run -p netlib --example echo_server -- [bind-addr] [--no-thread]   (default 0.0.0.0:7777)

mod common;

use std::time::Instant;

use netlib::{DriverConfig, Event, Manager};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let no_thread = std::env::args().any(|a| a == "--no-thread");
    let bind = std::env::args()
        .skip(1)
        .find(|a| !a.starts_with("--"))
        .unwrap_or_else(|| "0.0.0.0:7777".into());
    let driver = DriverConfig {
        receive_thread: !no_thread,
    };
    let mut m = Manager::bind_with(&bind, common::config(true), driver)?;
    common::logging(&mut m);
    println!("echo server on {}", m.local_addr()?);
    let mut echo = Vec::new();
    let mut next = Instant::now();
    loop {
        m.poll()?;
        while let Some(e) = m.poll_event() {
            match e {
                Event::ConnectRequest { peer, addr, .. } => {
                    println!("connect request from {addr}");
                    m.accept(peer)?;
                }
                Event::Connected { addr, .. } => println!("{addr} connected"),
                Event::Disconnected { peer, reason } => println!("{peer:?} disconnected: {reason:?}"),
                Event::Message { peer, channel, data } => echo.push((peer, channel, data.to_vec())),
            }
        }
        for (peer, channel, data) in echo.drain(..) {
            let _ = m.send(peer, channel, &data);
        }
        m.flush();
        next += common::TICK;
        std::thread::sleep(next.saturating_duration_since(Instant::now()));
    }
}
