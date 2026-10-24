//! Runs a server with N clients on the emulated network and prints what happened.
//!
//! cargo run -p netlib-sim --example sim -- --loss 10 --latency 100 --jitter 20 --clients 8 --seconds 20
//!
//! Every client sends a 48-byte state on the sequenced channel and an 8-byte event on the
//! unreliable channel each tick and a 24-byte reliable ordered event every 8th tick; the server
//! sends every client one 32-byte state per player and a reliable event every 8th tick.

use std::time::Duration;

use netlib_core::{ChannelConfig, Config, DeliveryMode, Event, PeerState};
use netlib_sim::{Link, Sim};

const TICK: Duration = Duration::from_micros(15_625); // 64 Hz

fn arg(args: &[String], name: &str, default: f64) -> f64 {
    args.windows(2)
        .find(|w| w[0] == name)
        .map(|w| w[1].parse().unwrap_or_else(|_| panic!("{name} expects a number")))
        .unwrap_or(default)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let link = Link {
        loss: arg(&args, "--loss", 0.0) / 100.0,
        latency: Duration::from_secs_f64(arg(&args, "--latency", 50.0) / 2000.0),
        jitter: Duration::from_secs_f64(arg(&args, "--jitter", 0.0) / 1000.0),
        duplicate: arg(&args, "--duplicate", 0.0) / 100.0,
        bandwidth: None,
    };
    let clients = arg(&args, "--clients", 4.0) as usize;
    let seconds = arg(&args, "--seconds", 10.0);
    let seed = arg(&args, "--seed", 1.0) as u64;
    println!("link: {link:?} (latency is one-way: --latency is RTT)");

    let config = Config {
        protocol_id: 1,
        accept_incoming: true,
        channels: vec![
            ChannelConfig::new(DeliveryMode::Unreliable),
            ChannelConfig::new(DeliveryMode::UnreliableSequenced),
            ChannelConfig::new(DeliveryMode::ReliableOrdered),
        ],
        ..Config::default()
    };
    let mut sim = Sim::new(seed, link);
    let server = sim.add_node(config.clone()).unwrap();
    let addr = sim.addr(server);
    for _ in 0..clients {
        let c = sim
            .add_node(Config {
                accept_incoming: false,
                ..config.clone()
            })
            .unwrap();
        let now = sim.now;
        sim.manager(c).connect(now, addr, b"sim").unwrap();
    }
    sim.transmit();

    let ticks = (seconds * 64.0) as u64;
    let mut disconnects = 0;
    for tick in 0..ticks {
        sim.tick(TICK, |i, m, now| {
            while let Some(e) = m.poll_event() {
                match e {
                    Event::ConnectRequest { peer, .. } => m.accept(now, peer).unwrap(),
                    Event::Disconnected { reason, .. } => {
                        println!("node {i}: disconnected {reason:?}");
                        disconnects += 1;
                    }
                    _ => {}
                }
            }
            let peers: Vec<_> = m
                .peers()
                .filter(|&p| m.peer_state(p) == Some(PeerState::Connected))
                .collect();
            for p in peers {
                if tick % 8 == 0 {
                    m.send(p, 2, &[0; 24]).unwrap();
                }
                if i == server {
                    for _ in 0..clients {
                        m.send(p, 1, &[0; 32]).unwrap();
                    }
                } else {
                    m.send(p, 1, &[0; 48]).unwrap();
                    m.send(p, 0, &[0; 8]).unwrap();
                }
            }
        });
    }

    println!(
        "\n{:>6} {:>9} {:>9} {:>9} {:>10} {:>9} {:>8} {:>8} {:>8}",
        "node", "sent", "recv", "msgs in", "bytes out", "rtt ms", "stale", "lost", "resent"
    );
    for (i, n) in sim.nodes.iter().enumerate() {
        for p in n.manager.peers() {
            let s = n.manager.peer_stats(p).unwrap();
            let rtt = n.manager.peer_rtt(p).map_or(f64::NAN, |d| d.as_secs_f64() * 1000.0);
            let name = if i == server {
                format!("srv→{}", p.to_bits() & 0xffff)
            } else {
                format!("c{i}")
            };
            println!(
                "{name:>6} {:>9} {:>9} {:>9} {:>10} {:>9.1} {:>8} {:>8} {:>8}",
                s.packets_sent,
                s.packets_received,
                s.messages_received,
                s.bytes_sent,
                rtt,
                s.messages_stale,
                s.packets_lost,
                s.messages_retransmitted
            );
        }
    }
    let s = &sim.stats;
    println!(
        "\nnetwork: {} datagrams, {} lost ({:.1}%), {} duplicated; {} disconnects",
        s.sent,
        s.lost,
        100.0 * s.lost as f64 / s.sent.max(1) as f64,
        s.duplicated,
        disconnects
    );
}
