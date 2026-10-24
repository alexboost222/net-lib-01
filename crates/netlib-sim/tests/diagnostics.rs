//! L6 / L8: every emulated failure leaves a trail in the logs that explains it, and no
//! secret reaches the logs.

use std::net::SocketAddr;
use std::time::Duration;

use netlib_core::log::{Level, RateLimited};
use netlib_core::{ChannelConfig, Config, DeliveryMode, Event, PeerId};
use netlib_sim::{Link, LogCapture, LogEntry, Sim};

const TICK: Duration = Duration::from_micros(15_625);

fn config(server: bool) -> Config {
    Config {
        protocol_id: 11,
        accept_incoming: server,
        channels: vec![
            ChannelConfig::new(DeliveryMode::UnreliableSequenced),
            ChannelConfig::new(DeliveryMode::ReliableOrdered),
        ],
        stats_interval: Some(Duration::from_secs(1)),
        ..Config::default()
    }
}

struct Scenario {
    sim: Sim,
    server_log: LogCapture,
    client_log: LogCapture,
    cpeer: PeerId,
    speer: PeerId,
}

impl Scenario {
    fn new(seed: u64, link: Link, client: Config) -> Self {
        let mut sim = Sim::new(seed, link);
        sim.add_node(config(true)).unwrap();
        sim.add_node(client).unwrap();
        let server_log = LogCapture::new(Level::Debug);
        let client_log = LogCapture::new(Level::Debug);
        sim.manager(0).set_log_sink(Some(Box::new(server_log.clone())));
        sim.manager(1).set_log_sink(Some(Box::new(client_log.clone())));
        let addr = sim.addr(0);
        let now = sim.now;
        let cpeer = sim.manager(1).connect(now, addr, &[]).unwrap();
        sim.transmit();
        let mut speer = None;
        for _ in 0..64 * 2 {
            sim.tick(TICK, |i, m, now| {
                while let Some(e) = m.poll_event() {
                    match e {
                        Event::ConnectRequest { peer, .. } => m.accept(now, peer).unwrap(),
                        Event::Connected { peer, .. } if i == 0 => speer = Some(peer),
                        _ => {}
                    }
                }
            });
        }
        Scenario {
            sim,
            server_log,
            client_log,
            cpeer,
            speer: speer.expect("connected"),
        }
    }

    fn traffic(&mut self, ticks: usize) {
        let (cpeer, speer) = (self.cpeer, self.speer);
        for _ in 0..ticks {
            self.sim.tick(TICK, |i, m, _| {
                while m.poll_event().is_some() {}
                let p = if i == 0 { speer } else { cpeer };
                let _ = m.send(p, 0, &[1; 32]);
                let _ = m.send(p, 1, &[2; 24]);
            });
        }
    }
}

/// Templates of the events in `log` that mention `peer` (or all when `None`).
fn trail(log: &[LogEntry], peer: Option<PeerId>) -> Vec<&'static str> {
    let id = peer.map(|p| p.to_bits().to_string());
    log.iter()
        .filter(|e| id.as_deref().is_none_or(|id| e.prop("PeerId") == Some(id)))
        .map(|e| e.template)
        .collect()
}

/// `needles` appear in `hay` in this order (not necessarily adjacent).
fn in_order(hay: &[&str], needles: &[&str]) -> bool {
    let mut it = hay.iter();
    needles.iter().all(|n| it.any(|h| h.contains(n)))
}

fn assert_trail(log: &[LogEntry], peer: Option<PeerId>, needles: &[&str]) {
    let t = trail(log, peer);
    assert!(in_order(&t, needles), "expected {needles:#?} in order, got:\n{t:#?}");
}

#[test]
fn l6_timeout_is_explained() {
    let mut s = Scenario::new(1, Link::default(), config(false));
    s.traffic(32);
    s.sim.nodes[1].partitioned = true;
    s.traffic(64 * 11);
    let log = s.server_log.entries();
    assert_trail(
        &log,
        Some(s.speer),
        &[
            "connected from",
            "timed out after {Silence} ms",
            "disconnected: {Reason}",
        ],
    );
    let timeout = log.iter().find(|e| e.template.contains("timed out after")).unwrap();
    assert_eq!(timeout.level, Level::Warn);
    assert!(timeout.prop("Silence").unwrap().parse::<f64>().unwrap() >= 10_000.0);
    let disc = log.iter().rfind(|e| e.template.contains("disconnected:")).unwrap();
    assert_eq!(disc.prop("Reason"), Some("timeout"));
}

#[test]
fn l6_heavy_loss_is_explained() {
    let link = Link {
        loss: 0.3,
        latency: Duration::from_millis(30),
        ..Link::default()
    };
    let mut s = Scenario::new(2, link, config(false));
    s.traffic(64 * 5);
    let log = s.client_log.entries();
    let t = trail(&log, Some(s.cpeer));
    assert!(
        t.iter().any(|e| e.contains("lost with {Pieces} reliable pieces")),
        "{t:#?}"
    );
    assert!(t.iter().any(|e| e.starts_with("Retransmitted bytes")));
    // The periodic statistics report the loss rate.
    let stats: Vec<f64> = log
        .iter()
        .filter_map(|e| e.prop("LossPercent").map(|v| v.parse().unwrap()))
        .collect();
    assert!(stats.len() >= 3, "{stats:?}");
    assert!(stats.iter().any(|&l| l > 15.0), "loss rate visible in stats: {stats:?}");
}

#[test]
fn l6_address_change_is_explained() {
    let mut s = Scenario::new(3, Link::default(), config(false));
    s.traffic(32);
    let new: SocketAddr = "10.9.0.9:4444".parse().unwrap();
    s.sim.set_addr(1, new);
    s.traffic(64);
    let log = s.server_log.entries();
    assert_trail(
        &log,
        None,
        &[
            "asked it to identify",
            "moved from {OldEndpoint} to {NewEndpoint}",
            "validated",
        ],
    );
    let moved = log.iter().find(|e| e.template.contains("moved from")).unwrap();
    assert_eq!(moved.prop("NewEndpoint"), Some("10.9.0.9:4444"));
    assert_trail(&s.client_log.entries(), Some(s.cpeer), &["does not know our address"]);
}

#[test]
fn l6_queue_overflow_is_explained() {
    let client = Config {
        max_reliable_queue: 16,
        ..config(false)
    };
    let mut s = Scenario::new(4, Link::default(), client);
    s.sim.nodes[1].partitioned = true;
    for _ in 0..20 {
        let _ = s.sim.manager(1).send(s.cpeer, 1, &[0; 8]);
    }
    let log = s.client_log.entries();
    let full = log
        .iter()
        .find(|e| e.template.contains("is full"))
        .expect("queue full logged");
    assert_eq!(full.level, Level::Warn);
    assert_eq!(full.prop("Queued"), Some("16"));
    assert_eq!(full.prop("Channel"), Some("1"));
}

#[test]
fn l6_budget_pressure_is_explained() {
    let client = Config {
        bandwidth: Some(1_000),
        ..config(false)
    };
    let mut s = Scenario::new(5, Link::default(), client);
    // ~4.5 KB/s offered against a 1 KB/s budget (plus a 2.4 KB burst allowance).
    s.traffic(64 * 4);
    let log = s.client_log.entries();
    assert_trail(&log, Some(s.cpeer), &["Budget held back {Held}"]);
    let out: Vec<u64> = log
        .iter()
        .filter_map(|e| e.prop("BytesOutPerSec").map(|v| v.parse().unwrap()))
        .collect();
    let last = *out.last().unwrap();
    assert!(last <= 1_100, "stats show the budget: {out:?}");
}

#[test]
fn l6_handshake_flood_is_explained_and_rate_limited() {
    let mut sim = Sim::new(6, Link::default());
    sim.add_node(config(true)).unwrap();
    let capture = LogCapture::new(Level::Debug);
    sim.manager(0)
        .set_log_sink(Some(Box::new(RateLimited::new(capture.clone(), 20))));
    let to = sim.addr(0);
    for i in 0..3000u32 {
        let from: SocketAddr = format!("10.{}.{}.{}:1000", (i >> 16) & 255, (i >> 8) & 255, i & 255)
            .parse()
            .unwrap();
        let mut p = vec![3u8];
        p.extend_from_slice(&u64::from(i).to_le_bytes());
        p.extend_from_slice(&[0; 12]);
        p.push(0);
        sim.inject(0, from, to, p);
        if i % 100 == 0 {
            sim.advance(Duration::from_millis(50));
        }
    }
    sim.advance(Duration::from_secs(2));
    // One more after the window closed reports the suppressed count.
    sim.inject(0, "10.200.0.1:1".parse().unwrap(), to, {
        let mut p = vec![3u8];
        p.extend_from_slice(&[0; 21]);
        p
    });
    sim.advance(Duration::from_millis(20));
    let log = capture.entries();
    let cookies = log.iter().filter(|e| e.template.contains("handshake cookie")).count();
    assert!((20..200).contains(&cookies), "rate limited to 20/s: {cookies}");
    let suppressed = log
        .iter()
        .find(|e| e.template.starts_with("Suppressed"))
        .expect("suppression reported");
    assert!(suppressed.prop("Count").unwrap().parse::<u64>().unwrap() > 100);
}

#[test]
fn l8_no_secrets_in_logs() {
    let mut s = Scenario::new(7, Link::default(), config(false));
    s.traffic(64);
    let key = s.sim.nodes[0].manager.__session_key(s.speer).unwrap();
    s.sim.set_addr(1, "10.9.9.9:1".parse().unwrap());
    s.traffic(64);
    let mut all = s.server_log.entries();
    all.extend(s.client_log.entries());
    assert!(!all.is_empty());
    let needles = [key.to_string(), format!("{key:x}"), format!("{key:016x}")];
    for e in &all {
        for (name, value) in &e.properties {
            let n = name.to_ascii_lowercase();
            assert!(
                !["key", "token", "mac", "secret", "cookie", "nonce"]
                    .iter()
                    .any(|s| n.contains(s)),
                "secret-like property {name} in {:?}",
                e.template
            );
            assert!(
                !needles.iter().any(|k| value.contains(k.as_str())),
                "session key in {name}"
            );
        }
        assert!(!needles.iter().any(|k| e.message.contains(k.as_str())));
    }
}
