//! R6: one seed, one outcome.

use std::time::Duration;

use netlib_core::{ChannelConfig, Config, DeliveryMode, Event};
use netlib_sim::{Link, Sim};

const TICK: Duration = Duration::from_micros(15_625);

/// Runs a lossy two-client session and returns a trace of everything observable.
fn trace(seed: u64) -> Vec<String> {
    let link = Link {
        loss: 0.1,
        latency: Duration::from_millis(40),
        jitter: Duration::from_millis(30),
        duplicate: 0.05,
        bandwidth: Some(200_000),
    };
    let mut sim = Sim::new(seed, link);
    let config = Config {
        protocol_id: 1,
        accept_incoming: true,
        channels: vec![
            ChannelConfig::new(DeliveryMode::Unreliable),
            ChannelConfig::new(DeliveryMode::UnreliableSequenced),
        ],
        ..Config::default()
    };
    let server = sim.add_node(config.clone()).unwrap();
    let addr = sim.addr(server);
    for _ in 0..2 {
        let c = sim
            .add_node(Config {
                accept_incoming: false,
                ..config.clone()
            })
            .unwrap();
        let now = sim.now;
        sim.manager(c).connect(now, addr, &[]).unwrap();
    }
    sim.transmit();
    let mut out = Vec::new();
    for tick in 0..64 * 20u32 {
        sim.tick(TICK, |i, m, now| {
            while let Some(e) = m.poll_event() {
                out.push(format!("{tick} {i} {e:?}"));
                if let Event::ConnectRequest { peer, .. } = e {
                    m.accept(now, peer).unwrap();
                }
            }
            let peers: Vec<_> = m.peers().collect();
            for p in peers {
                let _ = m.send(
                    p,
                    (tick % 2) as u8,
                    &tick.to_le_bytes().repeat(1 + (tick % 50) as usize),
                );
            }
        });
    }
    for (i, n) in sim.nodes.iter().enumerate() {
        for p in n.manager.peers() {
            out.push(format!("{i} {:?} {:?}", n.manager.peer_stats(p), n.manager.peer_rtt(p)));
        }
    }
    out.push(format!("{:?}", sim.stats));
    out
}

#[test]
fn r6_same_seed_same_run() {
    let a = trace(42);
    let b = trace(42);
    assert!(a.len() > 1000, "the scenario produces traffic: {}", a.len());
    assert_eq!(a, b);
}

#[test]
fn r6_different_seed_different_run() {
    assert_ne!(trace(1), trace(2));
}
