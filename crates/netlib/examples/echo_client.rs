//! Echo client: sends a timestamped message every tick and reports the echo round trip.
//!
//! cargo run -p netlib --example echo_client -- [server-addr] [seconds] [--no-thread]
//! (default 127.0.0.1:7777, 5 s)

mod common;

use std::time::{Duration, Instant};

use netlib::{DriverConfig, Event, Manager, PeerState};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let no_thread = std::env::args().any(|a| a == "--no-thread");
    let mut args = std::env::args().skip(1).filter(|a| !a.starts_with("--"));
    let server = args.next().unwrap_or_else(|| "127.0.0.1:7777".into());
    let seconds: u64 = args.next().map(|s| s.parse()).transpose()?.unwrap_or(5);
    let driver = DriverConfig {
        receive_thread: !no_thread,
    };
    let mut m = Manager::bind_with("0.0.0.0:0", common::config(false), driver)?;
    let peer = m.connect(&server, b"echo")??;
    println!("connecting to {server} from {}", m.local_addr()?);

    let start = Instant::now();
    let mut next = start;
    let (mut sent, mut received) = (0u64, 0u64);
    let mut rtts: Vec<Duration> = Vec::new();
    let mut connected_at = None;
    while start.elapsed() < Duration::from_secs(seconds) + Duration::from_secs(5) {
        m.poll()?;
        while let Some(e) = m.poll_event() {
            match e {
                Event::Connected { addr, .. } => {
                    println!("connected to {addr} in {:?}", start.elapsed());
                    connected_at = Some(Instant::now());
                }
                Event::Disconnected { reason, .. } => {
                    println!("disconnected: {reason:?}");
                    return Ok(());
                }
                Event::Message { data, .. } if data.len() >= 8 => {
                    let t = u64::from_le_bytes(data[..8].try_into()?);
                    rtts.push(Duration::from_micros(m.now().as_micros() - t));
                    received += 1;
                }
                _ => {}
            }
        }
        if let Some(at) = connected_at {
            if at.elapsed() >= Duration::from_secs(seconds) {
                break;
            }
            if m.peer_state(peer) == Some(PeerState::Connected) {
                let mut msg = m.now().as_micros().to_le_bytes().to_vec();
                msg.resize(48, 0); // a typical small game message
                m.send(peer, 0, &msg)?;
                sent += 1;
            }
        }
        m.flush();
        next += common::TICK;
        std::thread::sleep(next.saturating_duration_since(Instant::now()));
    }
    rtts.sort();
    let pct = |p: usize| {
        rtts.get(rtts.len().saturating_sub(1) * p / 100)
            .copied()
            .unwrap_or_default()
    };
    println!("sent {sent}, echoed {received}");
    println!("echo rtt p50 {:?}, p99 {:?}, max {:?}", pct(50), pct(99), pct(100));
    println!("transport rtt {:?}", m.peer_rtt(peer));
    println!("{:#?}", m.peer_stats(peer));
    m.disconnect(peer, b"done")?;
    Ok(())
}
