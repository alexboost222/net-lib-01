//! Packing and prioritisation (PK1-PK4, PR1-PR5).

use std::time::{Duration, Instant};

use netlib_core::{ChannelConfig, Config, DeliveryMode, Event, Manager, PeerId, PeerState, SendOptions};
use netlib_sim::{Link, Sim, SimRng};

const TICK: Duration = Duration::from_micros(15_625);

fn channels() -> Vec<ChannelConfig> {
    vec![
        ChannelConfig::new(DeliveryMode::Unreliable).with_priority(50),
        ChannelConfig::new(DeliveryMode::UnreliableSequenced).with_priority(200),
        ChannelConfig::new(DeliveryMode::ReliableUnordered),
        ChannelConfig::new(DeliveryMode::ReliableOrdered).with_priority(100),
        ChannelConfig::new(DeliveryMode::ReliableSequenced),
    ]
}

fn pair(seed: u64, link: Link, client: Config) -> (Sim, PeerId, PeerId) {
    let mut sim = Sim::new(seed, link);
    sim.add_node(Config {
        protocol_id: 5,
        accept_incoming: true,
        channels: channels(),
        ..Config::default()
    })
    .unwrap();
    sim.add_node(Config {
        protocol_id: 5,
        channels: channels(),
        ..client
    })
    .unwrap();
    let addr = sim.addr(0);
    let now = sim.now;
    let peer = sim.manager(1).connect(now, addr, &[]).unwrap();
    sim.transmit();
    let mut speer = None;
    for _ in 0..64 * 5 {
        sim.tick(TICK, |i, m, now| {
            while let Some(e) = m.poll_event() {
                match e {
                    Event::ConnectRequest { peer, .. } => m.accept(now, peer).unwrap(),
                    Event::Connected { peer, .. } if i == 0 => speer = Some(peer),
                    _ => {}
                }
            }
        });
        if let Some(s) = speer {
            if sim.nodes[1].manager.peer_state(peer) == Some(PeerState::Connected) {
                // Let handshake acks and pings settle.
                for _ in 0..8 {
                    sim.tick(TICK, |_, m, _| while m.poll_event().is_some() {});
                }
                return (sim, peer, s);
            }
        }
    }
    panic!("no connection");
}

fn flush_sizes(m: &mut Manager, now: netlib_core::Time) -> Vec<usize> {
    m.flush(now);
    let mut buf = vec![0; 1500];
    let mut sizes = Vec::new();
    while let Some((_, n)) = m.poll_transmit(&mut buf) {
        sizes.push(n);
    }
    sizes
}

#[test]
fn pk1_pk2_random_messages_pack_tightly() {
    let mut rng = SimRng::new(11);
    let (mut sim, peer, _) = pair(11, Link::default(), Config::default());
    let mtu = sim.nodes[1].manager.config().mtu;
    for round in 0..200 {
        let n = 1 + rng.next_u64() % 120;
        let mut payload = 0usize;
        for _ in 0..n {
            // Channel 4 (ReliableSequenced) keeps only its latest message, so it is left out.
            let ch = (rng.next_u64() % 4) as u8;
            let len = 1 + (rng.next_u64() % 300) as usize;
            payload += len;
            let priority = Some((rng.next_u64() % 256) as u8);
            sim.manager(1)
                .send_with(
                    peer,
                    ch,
                    &vec![0; len],
                    SendOptions {
                        priority,
                        ..SendOptions::default()
                    },
                )
                .unwrap();
        }
        let now = sim.now;
        let sizes = flush_sizes(sim.manager(1), now);
        let total: usize = sizes.iter().sum();
        // PK1: as many datagrams as the bytes need, plus at most one.
        let optimum = total.div_ceil(mtu);
        assert!(
            sizes.len() <= optimum + 1,
            "round {round}: {} datagrams for {total} bytes",
            sizes.len()
        );
        // PK2: every datagram but the last is at least 95% full.
        let mut sorted = sizes.clone();
        sorted.sort();
        for &s in &sorted[1..] {
            assert!(s * 100 >= mtu * 95, "round {round}: datagram of {s} bytes in {sizes:?}");
        }
        assert!(
            total >= payload,
            "round {round}: {total} bytes for {payload} bytes of payload, {sizes:?}"
        );
        assert!(
            total - payload <= sizes.len() * 11 + n as usize * 3,
            "round {round}: overhead"
        );
        // Deliver and ack so the reliable queues drain.
        sim.transmit();
        for _ in 0..4 {
            sim.tick(TICK, |_, m, _| while m.poll_event().is_some() {});
        }
    }
}

#[test]
fn pk3_retransmits_ride_with_new_data() {
    let link = Link {
        loss: 0.1,
        latency: Duration::from_millis(30),
        ..Link::default()
    };
    let (mut sim, peer, speer) = pair(12, link, Config::default());
    let before = sim.nodes[1].manager.peer_stats(peer).unwrap().clone();
    let ticks = 64 * 10;
    for _ in 0..ticks {
        sim.tick(TICK, |i, m, _| {
            while m.poll_event().is_some() {}
            let p = if i == 0 { speer } else { peer };
            m.send(p, 3, &[1; 30]).unwrap();
            m.send(p, 1, &[2; 40]).unwrap();
        });
    }
    let s = sim.nodes[1].manager.peer_stats(peer).unwrap();
    let packets = s.packets_sent - before.packets_sent;
    let extra = packets - ticks as u64 - (s.ping_acks_sent - before.ping_acks_sent);
    assert!(s.messages_retransmitted - before.messages_retransmitted > 30, "{s:?}");
    // Retransmissions and acks did not need packets of their own.
    assert_eq!(extra, 0, "{packets} packets in {ticks} ticks: {s:?}");
    assert_eq!(s.standalone_acks_sent, before.standalone_acks_sent);
}

#[test]
fn pk4_fragments_fill_the_space_left_by_small_messages() {
    let (mut sim, peer, _) = pair(13, Link::default(), Config::default());
    let mtu = sim.nodes[1].manager.config().mtu;
    for i in 0..10u8 {
        sim.manager(1).send(peer, 1, &[i; 70]).unwrap();
    }
    sim.manager(1).send(peer, 3, &vec![9; 20_000]).unwrap();
    let now = sim.now;
    let sizes = flush_sizes(sim.manager(1), now);
    assert!(sizes[..sizes.len() - 1].iter().all(|&n| n >= mtu - 4), "{sizes:?}");
    let total: usize = sizes.iter().sum();
    assert_eq!(sizes.len(), total.div_ceil(mtu));
}

#[test]
fn pk4_fragmented_messages_survive_heavy_loss() {
    let link = Link {
        loss: 0.25,
        latency: Duration::from_millis(40),
        jitter: Duration::from_millis(20),
        duplicate: 0.05,
        ..Link::default()
    };
    let (mut sim, peer, _) = pair(14, link, Config::default());
    let msgs: Vec<Vec<u8>> = (0..20u32)
        .map(|i| {
            (0..(2000 + i as usize * 1500))
                .map(|b| (b as u32 * 7 + i) as u8)
                .collect()
        })
        .collect();
    let mut got = Vec::new();
    for t in 0..64 * 30 {
        sim.tick(TICK, |i, m, _| {
            while let Some(e) = m.poll_event() {
                if let Event::Message { channel: 3, data, .. } = e {
                    got.push(data.to_vec());
                }
            }
            if i == 1 && t % 16 == 0 && t / 16 < msgs.len() {
                m.send(peer, 3, &msgs[t / 16]).unwrap();
            }
        });
    }
    assert_eq!(got, msgs);
}

/// Traffic offered per tick by the client in a budget test.
struct Load {
    budget: u32,
    /// One message of this size on channel 1 (UnreliableSequenced, priority 200), replacing
    /// the previous unsent one: the latest state of one entity.
    high: usize,
    /// `low_count` messages of `low` bytes on channel 0 (Unreliable, priority 50).
    low: usize,
    low_count: usize,
    low_ttl: Duration,
    ticks: usize,
}

/// Runs `load`; returns per-channel delivery delays in ticks and the bytes the client sent.
fn budget_run(load: Load) -> (Vec<usize>, Vec<usize>, u64) {
    let (mut sim, peer, _) = pair(
        15,
        Link::default(),
        Config {
            bandwidth: Some(load.budget),
            ..Config::default()
        },
    );
    let start_bytes = sim.nodes[1].manager.peer_stats(peer).unwrap().bytes_sent;
    let (mut hi, mut lo) = (Vec::new(), Vec::new());
    let msg = |t: usize, len: usize| {
        let mut m = (t as u32).to_le_bytes().to_vec();
        m.resize(len, 0);
        m
    };
    for t in 0..load.ticks {
        sim.tick(TICK, |i, m, _| {
            while let Some(e) = m.poll_event() {
                if let Event::Message { channel, data, .. } = e {
                    let sent = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
                    if channel == 1 { &mut hi } else { &mut lo }.push(t - sent);
                }
            }
            if i == 1 {
                let low = SendOptions {
                    ttl: Some(load.low_ttl),
                    ..SendOptions::default()
                };
                for _ in 0..load.low_count {
                    m.send_with(peer, 0, &msg(t, load.low), low).unwrap();
                }
                let high = SendOptions {
                    replace_key: Some(1),
                    ..SendOptions::default()
                };
                m.send_with(peer, 1, &msg(t, load.high), high).unwrap();
            }
        });
    }
    let bytes = sim.nodes[1].manager.peer_stats(peer).unwrap().bytes_sent - start_bytes;
    (hi, lo, bytes)
}

#[test]
fn pr1_high_priority_goes_first_under_a_budget() {
    // Budget 32 KB/s = 500 bytes per tick; offered 300 (high) + 600 (low) per tick. The low
    // priority TTL (300 ms) is below starvation_time (500 ms): low messages are dropped
    // rather than allowed to age past the high priority ones.
    let (hi, lo, _) = budget_run(Load {
        budget: 32_000,
        high: 300,
        low: 100,
        low_count: 6,
        low_ttl: Duration::from_millis(300),
        ticks: 64 * 10,
    });
    let late = hi.iter().filter(|&&d| d > 1).count();
    assert!(hi.len() >= 64 * 10 - 4, "high priority delivered: {}", hi.len());
    assert!(late == 0, "{late} high priority messages were delayed");
    // Low priority traffic gets the rest of the budget.
    assert!(lo.len() > 64 * 10, "low priority delivered: {}", lo.len());
}

#[test]
fn pr2_low_priority_is_not_starved() {
    // High priority alone exceeds the budget (600 bytes/tick vs 500); low priority messages
    // never expire.
    let (hi, lo, _) = budget_run(Load {
        budget: 32_000,
        high: 600,
        low: 40,
        low_count: 1,
        low_ttl: Duration::from_secs(60),
        ticks: 64 * 10,
    });
    assert!(!hi.is_empty());
    // starvation_time is 500 ms = 32 ticks: after that a waiting message is top priority.
    let worst = *lo.iter().max().unwrap();
    assert!(worst <= 32 + 4, "low priority waited {worst} ticks");
    assert!(lo.len() >= 64 * 10 - 36, "low priority delivered {}", lo.len());
}

#[test]
fn pr3_ttl_and_replace_key() {
    let (mut sim, peer, _) = pair(
        16,
        Link::default(),
        Config {
            bandwidth: Some(8_000),
            ..Config::default()
        },
    );
    let mut got: Vec<(u8, Vec<u8>)> = Vec::new();
    // Exhaust the budget, then queue messages that cannot go out at once.
    sim.manager(1).send(peer, 3, &vec![0; 4000]).unwrap();
    let ttl = |ms| SendOptions {
        ttl: Some(Duration::from_millis(ms)),
        ..SendOptions::default()
    };
    sim.manager(1).send_with(peer, 0, b"expires", ttl(20)).unwrap();
    for v in 0..10u8 {
        let key = SendOptions {
            replace_key: Some(1),
            priority: Some(10),
            ttl: Some(Duration::from_secs(5)),
        };
        sim.manager(1).send_with(peer, 0, &[b'k', v], key).unwrap();
    }
    assert!(
        sim.manager(1).send_with(peer, 3, b"x", ttl(5)).is_err(),
        "no TTL on reliable"
    );
    for _ in 0..64 * 2 {
        sim.tick(TICK, |_, m, _| {
            while let Some(e) = m.poll_event() {
                if let Event::Message { channel, data, .. } = e {
                    got.push((channel, data.to_vec()));
                }
            }
        });
    }
    let unreliable: Vec<&Vec<u8>> = got.iter().filter(|(c, _)| *c == 0).map(|(_, d)| d).collect();
    assert_eq!(
        unreliable,
        vec![&vec![b'k', 9]],
        "only the latest keyed message, nothing expired"
    );
    let s = sim.nodes[1].manager.peer_stats(peer).unwrap();
    assert_eq!((s.messages_expired, s.messages_replaced), (1, 9));
}

#[test]
fn pr4_bandwidth_budget_is_respected() {
    for budget in [16_000u32, 64_000, 200_000] {
        let secs = 10;
        // Offer about twice the budget.
        let per_tick = budget as usize * 2 / 64;
        let (_, _, bytes) = budget_run(Load {
            budget,
            high: 600,
            low: 500,
            low_count: per_tick / 500,
            low_ttl: Duration::from_millis(300),
            ticks: 64 * secs,
        });
        let rate = bytes as f64 / secs as f64;
        let err = (rate - f64::from(budget)).abs() / f64::from(budget);
        assert!(err < 0.05, "budget {budget}: {rate:.0} B/s ({:.1}% off)", err * 100.0);
    }
}

#[test]
fn pr5_scheduler_cost_scales_with_queue_size() {
    let (mut sim, peer, _) = pair(17, Link::default(), Config::default());
    let mut per_msg = Vec::new();
    for n in [2_000usize, 16_000] {
        for i in 0..n {
            let priority = Some((i * 37 % 256) as u8);
            sim.manager(1)
                .send_with(
                    peer,
                    (i % 2) as u8,
                    &[1; 20],
                    SendOptions {
                        priority,
                        ..SendOptions::default()
                    },
                )
                .unwrap();
        }
        let now = sim.now;
        let t = Instant::now();
        let sizes = flush_sizes(sim.manager(1), now);
        per_msg.push(t.elapsed().as_secs_f64() / n as f64);
        assert!(sizes.len() >= n * 21 / 1200);
    }
    // n log n sorting plus linear packing: 8x the messages may cost at most ~3x per message.
    assert!(per_msg[1] < per_msg[0] * 3.0 + 1e-6, "{per_msg:?}");
    assert!(per_msg[1] < 20e-6, "{:.2} us per message", per_msg[1] * 1e6);
}
