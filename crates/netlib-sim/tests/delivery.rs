//! Delivery mode contracts (R1-R4) on the emulated network.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use netlib_core::{ChannelConfig, Config, DeliveryMode, Event, Manager, PeerId, PeerState, PeerStats};
use netlib_sim::{Link, Sim, SimRng};

const TICK: Duration = Duration::from_micros(15_625);
const MODES: [DeliveryMode; 5] = [
    DeliveryMode::Unreliable,
    DeliveryMode::UnreliableSequenced,
    DeliveryMode::ReliableUnordered,
    DeliveryMode::ReliableOrdered,
    DeliveryMode::ReliableSequenced,
];

fn config(server: bool) -> Config {
    Config {
        protocol_id: 9,
        accept_incoming: server,
        channels: MODES.iter().map(|&m| ChannelConfig::new(m)).collect(),
        ..Config::default()
    }
}

/// A message: id, channel, then bytes derived from the id so corruption is detected.
fn payload(id: u32, channel: u8, len: usize) -> Vec<u8> {
    let mut p = id.to_le_bytes().to_vec();
    p.push(channel);
    p.extend((0..len.saturating_sub(5)).map(|i| (id as usize + i) as u8));
    p
}

fn parse(channel: u8, data: &[u8]) -> u32 {
    assert!(data.len() >= 5, "short message");
    let id = u32::from_le_bytes(data[..4].try_into().unwrap());
    assert_eq!(data[4], channel, "message arrived on the wrong channel");
    assert!(
        data[5..].iter().enumerate().all(|(i, &b)| b == (id as usize + i) as u8),
        "corrupted payload"
    );
    id
}

/// Connects a client (node 1) to a server (node 0); returns `(client's peer, server's peer)`.
fn connect(sim: &mut Sim) -> (PeerId, PeerId) {
    let addr = sim.addr(0);
    let now = sim.now;
    let peer = sim.manager(1).connect(now, addr, &[]).unwrap();
    sim.transmit();
    let mut speer = None;
    for _ in 0..64 * 10 {
        sim.tick(TICK, |i, m, now| {
            while let Some(e) = m.poll_event() {
                match e {
                    Event::ConnectRequest { peer, .. } => m.accept(now, peer).unwrap(),
                    Event::Connected { peer, .. } => {
                        if i == 0 {
                            speer = Some(peer);
                        }
                    }
                    e => panic!("unexpected {e:?}"),
                }
            }
        });
        if let Some(s) = speer {
            if sim.nodes[1].manager.peer_state(peer) == Some(PeerState::Connected) {
                return (peer, s);
            }
        }
    }
    panic!("no connection");
}

#[derive(Default)]
struct Stream {
    /// Ids sent per channel, in send order.
    sent: Vec<Vec<u32>>,
    /// Ids received per channel, in delivery order.
    received: Vec<Vec<u32>>,
}

/// Bidirectional traffic on all five channels.
struct Run {
    sim: Sim,
    peers: [PeerId; 2],
    streams: [Stream; 2],
    next_id: u32,
    rng: SimRng,
}

impl Run {
    fn new(seed: u64, link: Link) -> Self {
        let mut sim = Sim::new(seed, link);
        sim.add_node(config(true)).unwrap();
        sim.add_node(config(false)).unwrap();
        let (c, s) = connect(&mut sim);
        let fresh = || Stream {
            sent: vec![Vec::new(); 5],
            received: vec![Vec::new(); 5],
        };
        Run {
            sim,
            peers: [s, c],
            streams: [fresh(), fresh()],
            next_id: 0,
            rng: SimRng::new(seed ^ 0xabcdef),
        }
    }

    /// Runs `ticks` ticks; while `sending`, each side sends on every channel with
    /// probability 1/2, with occasional fragmented messages on reliable channels.
    fn ticks(&mut self, ticks: usize, sending: bool) {
        for _ in 0..ticks {
            let mut plan: Vec<(usize, u8, Vec<u8>, u32)> = Vec::new();
            if sending {
                for node in 0..2 {
                    for ch in 0..5u8 {
                        if self.rng.next_u64() % 2 == 0 {
                            continue;
                        }
                        let big = MODES[ch as usize].is_reliable() && self.rng.next_u64() % 40 == 0;
                        let len = if big {
                            1200 + (self.rng.next_u64() % 6000) as usize
                        } else {
                            5 + (self.rng.next_u64() % 120) as usize
                        };
                        self.next_id += 1;
                        plan.push((node, ch, payload(self.next_id, ch, len), self.next_id));
                    }
                }
            }
            let peers = self.peers;
            let streams = &mut self.streams;
            self.sim.tick(TICK, |i, m, _| {
                while let Some(e) = m.poll_event() {
                    match e {
                        Event::Message { channel, data, .. } => {
                            streams[i].received[channel as usize].push(parse(channel, data));
                        }
                        e => panic!("node {i}: unexpected {e:?}"),
                    }
                }
                for (node, ch, data, id) in plan.iter().filter(|p| p.0 == i) {
                    m.send(peers[*node], *ch, data).unwrap();
                    streams[*node].sent[*ch as usize].push(*id);
                }
            });
        }
    }

    fn stats(&self, node: usize) -> PeerStats {
        self.sim.nodes[node]
            .manager
            .peer_stats(self.peers[node])
            .unwrap()
            .clone()
    }

    /// Checks every channel contract in both directions.
    fn verify(&self, context: &str) {
        for from in 0..2 {
            let to = 1 - from;
            for (ch, mode) in MODES.iter().enumerate() {
                let sent = &self.streams[from].sent[ch];
                let got = &self.streams[to].received[ch];
                let sent_set: HashSet<u32> = sent.iter().copied().collect();
                let got_set: HashSet<u32> = got.iter().copied().collect();
                let ctx = format!("{context}: {from}->{to} {mode:?}");
                assert_eq!(got_set.len(), got.len(), "{ctx}: duplicates");
                assert!(got_set.is_subset(&sent_set), "{ctx}: unknown messages");
                let increasing = got.windows(2).all(|w| w[0] < w[1]);
                match mode {
                    DeliveryMode::Unreliable => {}
                    DeliveryMode::UnreliableSequenced => assert!(increasing, "{ctx}: order"),
                    DeliveryMode::ReliableUnordered => assert_eq!(got_set, sent_set, "{ctx}: missing"),
                    DeliveryMode::ReliableOrdered => assert_eq!(got, sent, "{ctx}: order or loss"),
                    DeliveryMode::ReliableSequenced => {
                        assert!(increasing, "{ctx}: order");
                        assert_eq!(got.last(), sent.last(), "{ctx}: latest message missing");
                    }
                }
            }
        }
    }
}

#[test]
fn r1_delivery_contracts_under_loss_and_reordering() {
    let link = Link {
        loss: 0.1,
        latency: Duration::from_millis(60),
        jitter: Duration::from_millis(40),
        duplicate: 0.05,
        bandwidth: None,
    };
    let mut run = Run::new(1, link);
    run.ticks(64 * 20, true);
    run.ticks(64 * 10, false);
    run.verify("lossy");
    // Lossy delivery really happened, and reliable channels really recovered.
    let s = run.stats(1);
    assert!(s.messages_retransmitted > 0 && s.packets_lost > 0, "{s:?}");
    let ordered = &run.streams[1].received[3];
    assert!(ordered.len() > 400, "{}", ordered.len());
    assert!(
        run.streams[0].received[0].len() < run.streams[1].sent[0].len(),
        "unreliable lost nothing"
    );
}

#[test]
fn r1_perfect_link_delivers_everything_in_order() {
    let mut run = Run::new(2, Link::default());
    run.ticks(64 * 5, true);
    run.ticks(64, false);
    run.verify("perfect");
    for from in 0..2 {
        for ch in 0..5 {
            if ch == 4 {
                continue; // sequenced may legitimately skip superseded messages
            }
            assert_eq!(
                run.streams[from].sent[ch],
                run.streams[1 - from].received[ch],
                "channel {ch}"
            );
        }
    }
    assert_eq!(run.stats(0).messages_retransmitted, 0);
}

#[test]
fn r2_random_links_property() {
    let mut rng = SimRng::new(2026);
    for case in 0..128 {
        let link = Link {
            loss: rng.next_f64() * 0.2,
            latency: Duration::from_millis(rng.next_u64() % 151),
            jitter: Duration::from_millis(rng.next_u64() % 51),
            duplicate: rng.next_f64() * 0.05,
            bandwidth: None,
        };
        let seed = rng.next_u64();
        let mut run = Run::new(seed, link.clone());
        run.ticks(64 * 6, true);
        run.ticks(64 * 12, false);
        run.verify(&format!("case {case} seed {seed} {link:?}"));
    }
}

#[test]
fn r3_packet_and_message_sequence_wraparound() {
    // 70k ticks of 1 ms with one reliable ordered and one sequenced message each:
    // both the packet seq and the message seq wrap past 65535.
    let link = Link {
        loss: 0.05,
        latency: Duration::from_millis(5),
        jitter: Duration::from_millis(2),
        ..Link::default()
    };
    let mut sim = Sim::new(3, link);
    sim.add_node(config(true)).unwrap();
    sim.add_node(config(false)).unwrap();
    let (cpeer, _) = connect(&mut sim);
    let mut ordered = Vec::new();
    let mut sequenced = Vec::new();
    let total = 70_000u32;
    for t in 0..total + 2000 {
        sim.tick(Duration::from_millis(1), |i, m, _| {
            while let Some(e) = m.poll_event() {
                if let Event::Message { channel, data, .. } = e {
                    let id = parse(channel, data);
                    if channel == 3 {
                        ordered.push(id)
                    } else {
                        sequenced.push(id)
                    }
                }
            }
            if i == 1 && t < total {
                m.send(cpeer, 3, &payload(t, 3, 12)).unwrap();
                m.send(cpeer, 1, &payload(t, 1, 12)).unwrap();
            }
        });
    }
    assert_eq!(ordered, (0..total).collect::<Vec<_>>());
    assert!(sequenced.windows(2).all(|w| w[0] < w[1]));
    // Jitter (2 ms) exceeds the tick (1 ms), so many sequenced messages are legitimately stale.
    assert!(sequenced.len() > (total as usize) / 2, "{}", sequenced.len());
    let s = sim.nodes[1].manager.peer_stats(cpeer).unwrap();
    assert!(s.packets_sent > 65_536, "packet seq wrapped: {}", s.packets_sent);
}

/// Client sends one reliable ordered message per tick; returns its stats after draining.
fn reliable_stream(seed: u64, link: Link, partition: Option<(usize, usize)>) -> (PeerStats, usize, usize) {
    let mut sim = Sim::new(seed, link);
    sim.add_node(config(true)).unwrap();
    sim.add_node(config(false)).unwrap();
    let (cpeer, _) = connect(&mut sim);
    let mut got = 0usize;
    let sent = 64 * 20;
    for t in 0..sent + 64 * 15 {
        if let Some((from, to)) = partition {
            let cut = (from..to).contains(&t);
            sim.nodes[0].partitioned = cut;
            sim.nodes[1].partitioned = cut;
        }
        sim.tick(TICK, |i, m, _| {
            while let Some(e) = m.poll_event() {
                if let Event::Message { .. } = e {
                    got += 1;
                }
            }
            if i == 1 && t < sent {
                m.send(cpeer, 3, &[0; 40]).unwrap();
            }
        });
    }
    (sim.nodes[1].manager.peer_stats(cpeer).unwrap().clone(), sent, got)
}

#[test]
fn r4_no_retransmits_on_a_clean_slow_link() {
    let link = Link {
        latency: Duration::from_millis(150),
        ..Link::default()
    };
    let (s, sent, got) = reliable_stream(4, link, None);
    assert_eq!(got, sent);
    assert_eq!(s.messages_retransmitted, 0, "{s:?}");
}

#[test]
fn r4_retransmits_track_real_loss() {
    let link = Link {
        loss: 0.1,
        latency: Duration::from_millis(50),
        jitter: Duration::from_millis(10),
        ..Link::default()
    };
    let (s, sent, got) = reliable_stream(5, link, None);
    assert_eq!(got, sent);
    let ratio = s.messages_retransmitted as f64 / sent as f64;
    // ~10% of transmissions are lost; retransmissions are lost too and acks may be lost.
    assert!((0.05..0.3).contains(&ratio), "retransmit ratio {ratio:.3}: {s:?}");
}

#[test]
fn r4_reordering_causes_few_spurious_retransmits() {
    let link = Link {
        latency: Duration::from_millis(50),
        jitter: Duration::from_millis(30),
        ..Link::default()
    };
    let (s, sent, got) = reliable_stream(6, link, None);
    assert_eq!(got, sent);
    let ratio = s.messages_retransmitted as f64 / sent as f64;
    assert!(ratio < 0.15, "spurious retransmit ratio {ratio:.3}: {s:?}");
}

#[test]
fn r4_outage_does_not_cause_a_retransmit_storm() {
    let link = Link {
        latency: Duration::from_millis(50),
        ..Link::default()
    };
    // 3 s outage in the middle of the stream: ~190 messages in flight or queued.
    let (s, sent, got) = reliable_stream(7, link, Some((64 * 5, 64 * 8)));
    assert_eq!(got, sent);
    // With exponential backoff each stuck message is resent only a few times.
    assert!(s.messages_retransmitted < 64 * 3 * 6, "{s:?}");
}

#[test]
fn fragmented_message_packs_into_full_packets() {
    let mut sim = Sim::new(8, Link::default());
    sim.add_node(config(true)).unwrap();
    sim.add_node(config(false)).unwrap();
    let (cpeer, _) = connect(&mut sim);
    let max = sim.nodes[1]
        .manager
        .config()
        .max_message_size(DeliveryMode::ReliableOrdered);
    let msg = payload(1, 3, max);
    sim.manager(1).send(cpeer, 3, &msg).unwrap();
    assert!(sim.manager(1).send(cpeer, 3, &payload(2, 3, max + 1)).is_err());
    let now = sim.now;
    sim.manager(1).flush(now);
    let mut buf = vec![0; 1500];
    let mut sizes = Vec::new();
    while let Some((_, n)) = sim.manager(1).poll_transmit(&mut buf) {
        sizes.push(n);
    }
    let total: usize = sizes.iter().sum();
    let mtu = sim.nodes[1].manager.config().mtu;
    // Fragments have a fixed size of mtu - 19 bytes (adaptive sizing comes with M3).
    assert_eq!(sizes.len(), max.div_ceil(mtu - 19), "{sizes:?}");
    assert!(sizes[..sizes.len() - 1].iter().all(|&n| n >= mtu - 12), "{sizes:?}");
    assert!(total - max < sizes.len() * 20, "fragment overhead {}", total - max);
}

#[test]
fn b3_reliable_message_overhead() {
    let mut sim = Sim::new(9, Link::default());
    sim.add_node(config(true)).unwrap();
    sim.add_node(config(false)).unwrap();
    let (cpeer, _) = connect(&mut sim);
    let now = sim.now;
    sim.manager(1).flush(now);
    while sim.manager(1).poll_transmit(&mut [0; 1500]).is_some() {}
    for i in 0..10 {
        sim.manager(1).send(cpeer, 3, &[i; 40]).unwrap();
    }
    let now = sim.now;
    sim.manager(1).flush(now);
    let mut buf = vec![0; 1500];
    let (_, n) = sim.manager(1).poll_transmit(&mut buf).unwrap();
    assert!(sim.manager(1).poll_transmit(&mut buf).is_none());
    // header (8) + block (1) + base seq (2) + 10 x (len 1 + tag 1) - tail len (1)
    assert!(n - 400 < 8 + 1 + 2 + 20, "overhead {}", n - 400);
}

#[test]
fn reliable_queue_limit() {
    let mut sim = Sim::new(10, Link::default());
    sim.add_node(config(true)).unwrap();
    sim.add_node(Config {
        max_reliable_queue: 8,
        ..config(false)
    })
    .unwrap();
    let (cpeer, _) = connect(&mut sim);
    let m = sim.manager(1);
    for i in 0..8 {
        m.send(cpeer, 3, &[i]).unwrap();
    }
    assert_eq!(m.send(cpeer, 3, &[9]), Err(netlib_core::SendError::QueueFull));
    // A sequenced channel only keeps the latest message.
    for i in 0..100 {
        m.send(cpeer, 4, &[i]).unwrap();
    }
    // Once acked, the queue drains.
    let mut seen = HashMap::new();
    for _ in 0..32 {
        sim.tick(TICK, |_, m, _| {
            while let Some(e) = m.poll_event() {
                if let Event::Message { channel, .. } = e {
                    *seen.entry(channel).or_insert(0) += 1;
                }
            }
        });
    }
    assert_eq!(seen.get(&3), Some(&8));
    assert_eq!(seen.get(&4), Some(&1));
    sim.manager(1).send(cpeer, 3, &[9]).unwrap();
    let _: &Manager = &sim.nodes[0].manager;
}
