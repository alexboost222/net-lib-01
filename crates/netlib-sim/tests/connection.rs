//! Connection lifecycle scenarios on the emulated network.
//! Test names start with the id of the acceptance criterion they cover (see docs/PLAN.md).

use std::net::SocketAddr;
use std::time::Duration;

use netlib_core::{ChannelConfig, Config, DeliveryMode, DisconnectReason, Event, Manager, PeerId, PeerState};
use netlib_sim::{Link, Sim};

const TICK: Duration = Duration::from_micros(15_625); // 64 Hz
const PROTOCOL: u32 = 0x4e45_5401;

/// An owned copy of an event, for assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Ev {
    Request(PeerId, Vec<u8>),
    Connected(PeerId),
    Disconnected(PeerId, String),
    Message(PeerId, u8, Vec<u8>),
}

fn drain(m: &mut Manager) -> Vec<Ev> {
    let mut out = Vec::new();
    while let Some(e) = m.poll_event() {
        out.push(match e {
            Event::ConnectRequest { peer, data, .. } => Ev::Request(peer, data.to_vec()),
            Event::Connected { peer, .. } => Ev::Connected(peer),
            Event::Disconnected { peer, reason } => Ev::Disconnected(peer, format!("{reason:?}")),
            Event::Message { peer, channel, data } => Ev::Message(peer, channel, data.to_vec()),
        });
    }
    out
}

fn server_config() -> Config {
    Config {
        protocol_id: PROTOCOL,
        accept_incoming: true,
        channels: vec![
            ChannelConfig::new(DeliveryMode::Unreliable),
            ChannelConfig::new(DeliveryMode::UnreliableSequenced),
        ],
        ..Config::default()
    }
}

fn client_config() -> Config {
    Config {
        accept_incoming: false,
        ..server_config()
    }
}

/// Runs ticks, auto-accepting connection requests on node 0, and collects events per node.
fn run(sim: &mut Sim, ticks: usize, log: &mut [Vec<Ev>]) {
    for _ in 0..ticks {
        sim.tick(TICK, |i, m, now| {
            for e in drain(m) {
                if let Ev::Request(p, _) = &e {
                    m.accept(now, *p).unwrap();
                }
                log[i].push(e);
            }
        });
    }
}

fn connected_pair(seed: u64, link: Link) -> (Sim, PeerId, Vec<Vec<Ev>>) {
    let mut sim = Sim::new(seed, link);
    let server = sim.add_node(server_config()).unwrap();
    let client = sim.add_node(client_config()).unwrap();
    let addr = sim.addr(server);
    let now = sim.now;
    let peer = sim.manager(client).connect(now, addr, b"hello").unwrap();
    sim.transmit();
    let mut log = vec![Vec::new(), Vec::new()];
    // Up to connect_timeout; stop shortly after both sides see the connection.
    for _ in 0..64 * 5 {
        run(&mut sim, 1, &mut log);
        let done = |l: &Vec<Ev>| l.iter().any(|e| matches!(e, Ev::Connected(_)));
        if done(&log[0]) && done(&log[1]) {
            break;
        }
    }
    run(&mut sim, 8, &mut log);
    (sim, peer, log)
}

fn server_peer(log: &[Vec<Ev>]) -> PeerId {
    log[0]
        .iter()
        .find_map(|e| match e {
            Ev::Connected(p) => Some(*p),
            _ => None,
        })
        .expect("server saw a connection")
}

#[test]
fn r5_handshake_connects_and_delivers_connect_data() {
    let (sim, peer, log) = connected_pair(1, Link::default());
    assert!(matches!(&log[0][0], Ev::Request(_, d) if d == b"hello"));
    assert!(log[0].contains(&Ev::Connected(server_peer(&log))));
    assert_eq!(log[1], vec![Ev::Connected(peer)]);
    assert_eq!(sim.nodes[1].manager.peer_state(peer), Some(PeerState::Connected));
}

#[test]
fn r5_handshake_survives_30_percent_loss() {
    for seed in 0..50 {
        let link = Link {
            loss: 0.3,
            ..Link::default()
        };
        let (_, peer, log) = connected_pair(seed, link);
        assert!(log[1].contains(&Ev::Connected(peer)), "seed {seed}: {:?}", log[1]);
    }
}

#[test]
fn r5_connect_timeout_when_nobody_answers() {
    let mut sim = Sim::new(3, Link::default());
    let client = sim.add_node(client_config()).unwrap();
    let nowhere: SocketAddr = "10.9.9.9:9000".parse().unwrap();
    let now = sim.now;
    let peer = sim.manager(client).connect(now, nowhere, &[]).unwrap();
    sim.transmit();
    let mut log = vec![Vec::new()];
    // connect_timeout is 5 s
    run(&mut sim, 64 * 5 - 2, &mut log);
    assert!(log[0].is_empty());
    run(&mut sim, 4, &mut log);
    assert_eq!(log[0], vec![Ev::Disconnected(peer, "ConnectTimeout".into())]);
    assert!(sim.stats.sent >= 19, "retries every 250 ms: {}", sim.stats.sent);
}

#[test]
fn r5_idle_connection_is_kept_alive() {
    let (mut sim, peer, mut log) = connected_pair(4, Link::default());
    run(&mut sim, 64 * 60, &mut log);
    assert_eq!(sim.nodes[1].manager.peer_state(peer), Some(PeerState::Connected));
    assert!(log.iter().flatten().all(|e| !matches!(e, Ev::Disconnected(..))));
    // A keepalive per second in each direction, plus acks for them.
    let sent = sim.nodes[1].manager.peer_stats(peer).unwrap().packets_sent;
    assert!(
        (60..=200).contains(&sent),
        "client sent {sent} packets in 60 idle seconds"
    );
    assert!(sim.nodes[1].manager.peer_rtt(peer).is_some());
}

#[test]
fn r5_timeout_after_partition() {
    let (mut sim, peer, mut log) = connected_pair(5, Link::default());
    let speer = server_peer(&log);
    // Make the last received packet recent on both sides, then cut the link.
    for _ in 0..4 {
        sim.tick(TICK, |i, m, _| {
            m.send(if i == 0 { speer } else { peer }, 0, &[1]).unwrap();
        });
    }
    sim.nodes[0].partitioned = true;
    sim.nodes[1].partitioned = true;
    run(&mut sim, 64 * 10 - 4, &mut log);
    assert!(log.iter().flatten().all(|e| !matches!(e, Ev::Disconnected(..))));
    run(&mut sim, 8, &mut log);
    assert!(log[0].contains(&Ev::Disconnected(speer, "Timeout".into())));
    assert!(log[1].contains(&Ev::Disconnected(peer, "Timeout".into())));
}

#[test]
fn r5_short_outage_does_not_disconnect() {
    let (mut sim, peer, mut log) = connected_pair(6, Link::default());
    sim.nodes[1].partitioned = true;
    run(&mut sim, 64 * 8, &mut log);
    sim.nodes[1].partitioned = false;
    run(&mut sim, 64 * 20, &mut log);
    assert_eq!(sim.nodes[1].manager.peer_state(peer), Some(PeerState::Connected));
    assert!(log.iter().flatten().all(|e| !matches!(e, Ev::Disconnected(..))));
}

#[test]
fn r5_disconnect_delivers_data_to_remote() {
    let (mut sim, peer, mut log) = connected_pair(7, Link::default());
    let speer = server_peer(&log);
    sim.manager(1).disconnect(peer, b"bye").unwrap();
    assert_eq!(drain(sim.manager(1)), vec![Ev::Disconnected(peer, "Local".into())]);
    assert_eq!(sim.nodes[1].manager.peer_state(peer), None);
    sim.transmit();
    run(&mut sim, 4, &mut log);
    let expected = format!("{:?}", DisconnectReason::Remote { data: b"bye" });
    assert!(log[0].contains(&Ev::Disconnected(speer, expected)), "{:?}", log[0]);
    assert_eq!(sim.nodes[0].manager.peers().count(), 0);
}

#[test]
fn r5_reject_delivers_data_to_client() {
    let mut sim = Sim::new(8, Link::default());
    let server = sim.add_node(server_config()).unwrap();
    let client = sim.add_node(client_config()).unwrap();
    let addr = sim.addr(server);
    let now = sim.now;
    let peer = sim.manager(client).connect(now, addr, b"bad key").unwrap();
    sim.transmit();
    let mut client_log = Vec::new();
    for _ in 0..32 {
        sim.tick(TICK, |i, m, _| {
            for e in drain(m) {
                match (i, &e) {
                    (0, Ev::Request(p, _)) => m.reject(*p, b"denied").unwrap(),
                    (1, _) => client_log.push(e),
                    _ => {}
                }
            }
        });
    }
    let expected = format!("{:?}", DisconnectReason::Rejected { data: b"denied" });
    assert_eq!(client_log, vec![Ev::Disconnected(peer, expected)]);
    assert_eq!(sim.nodes[0].manager.peers().count(), 0);
}

#[test]
fn r5_server_full_rejects_extra_clients() {
    let mut sim = Sim::new(9, Link::default());
    let server = sim
        .add_node(Config {
            max_incoming: 2,
            ..server_config()
        })
        .unwrap();
    let addr = sim.addr(server);
    let mut peers = Vec::new();
    for _ in 0..3 {
        let c = sim.add_node(client_config()).unwrap();
        let now = sim.now;
        peers.push((c, sim.manager(c).connect(now, addr, &[]).unwrap()));
    }
    sim.transmit();
    let mut log = vec![Vec::new(); 4];
    run(&mut sim, 64, &mut log);
    let connected = (1..4)
        .filter(|&i| log[i].iter().any(|e| matches!(e, Ev::Connected(_))))
        .count();
    let full = (1..4)
        .filter(|&i| {
            log[i]
                .iter()
                .any(|e| matches!(e, Ev::Disconnected(_, r) if r == "ServerFull"))
        })
        .count();
    assert_eq!((connected, full), (2, 1));
}

#[test]
fn r5_wrong_protocol_id_is_ignored() {
    let mut sim = Sim::new(10, Link::default());
    let server = sim.add_node(server_config()).unwrap();
    let client = sim
        .add_node(Config {
            protocol_id: PROTOCOL + 1,
            ..client_config()
        })
        .unwrap();
    let addr = sim.addr(server);
    let now = sim.now;
    let peer = sim.manager(client).connect(now, addr, &[]).unwrap();
    sim.transmit();
    let mut log = vec![Vec::new(), Vec::new()];
    run(&mut sim, 64 * 6, &mut log);
    assert!(log[0].is_empty());
    assert_eq!(log[1], vec![Ev::Disconnected(peer, "ConnectTimeout".into())]);
    assert_eq!(sim.nodes[0].manager.stats().challenges_sent, 0);
}

#[test]
fn r5_client_restart_from_same_address_replaces_old_peer() {
    let (mut sim, _, mut log) = connected_pair(11, Link::default());
    let old = server_peer(&log);
    // A fresh manager on the same address: the old session is gone on the client side.
    let addr = sim.addr(0);
    let now = sim.now;
    sim.nodes[1].manager = Manager::new(client_config(), [9; 32], now).unwrap();
    let peer = sim.manager(1).connect(now, addr, &[]).unwrap();
    sim.transmit();
    let mut log2 = vec![Vec::new(), Vec::new()];
    run(&mut sim, 32, &mut log2);
    log[0].extend(log2[0].clone());
    assert!(
        log2[0].contains(&Ev::Disconnected(old, "Reconnected".into())),
        "{:?}",
        log2[0]
    );
    assert!(log2[1].contains(&Ev::Connected(peer)));
    assert_eq!(sim.nodes[0].manager.peers().count(), 1);
}

#[test]
fn unreliable_messages_roundtrip() {
    let (mut sim, peer, mut log) = connected_pair(12, Link::default());
    let speer = server_peer(&log);
    sim.manager(1).send(peer, 0, b"ping").unwrap();
    sim.manager(1).send(peer, 1, b"pos").unwrap();
    sim.manager(1).send(peer, 0, &[]).unwrap();
    let now = sim.now;
    sim.manager(1).flush(now);
    sim.transmit();
    log[0].clear();
    run(&mut sim, 4, &mut log);
    let msgs: Vec<_> = log[0]
        .iter()
        .filter(|e| matches!(e, Ev::Message(..)))
        .cloned()
        .collect();
    assert_eq!(
        msgs,
        vec![
            Ev::Message(speer, 0, b"ping".to_vec()),
            Ev::Message(speer, 0, vec![]),
            Ev::Message(speer, 1, b"pos".to_vec()),
        ]
    );
    // One datagram carried all three messages.
    assert_eq!(sim.nodes[0].manager.peer_stats(speer).unwrap().messages_received, 3);
}

#[test]
fn messages_larger_than_one_packet_are_split_across_datagrams() {
    let (mut sim, peer, mut log) = connected_pair(13, Link::default());
    let speer = server_peer(&log);
    let max = sim.nodes[1].manager.config().max_message_size(DeliveryMode::Unreliable);
    let sizes = [max, 1, 500, 700, max, 3, 0, 1100];
    for (i, &n) in sizes.iter().enumerate() {
        sim.manager(1).send(peer, (i % 2) as u8, &vec![i as u8; n]).unwrap();
    }
    assert!(sim.manager(1).send(peer, 0, &vec![0; max + 1]).is_err());
    log[0].clear();
    run(&mut sim, 4, &mut log);
    let mut got: Vec<(u8, usize)> = log[0]
        .iter()
        .filter_map(|e| match e {
            Ev::Message(p, ch, d) if *p == speer => Some((*ch, d.len())),
            _ => None,
        })
        .collect();
    let mut want: Vec<(u8, usize)> = sizes.iter().enumerate().map(|(i, &n)| ((i % 2) as u8, n)).collect();
    got.sort();
    want.sort();
    assert_eq!(got, want);
}

#[test]
fn sequenced_channel_drops_stale_messages() {
    let link = Link {
        jitter: Duration::from_millis(80),
        ..Link::default()
    };
    let (mut sim, peer, mut log) = connected_pair(14, link);
    let speer = server_peer(&log);
    log[0].clear();
    for tick in 0..640u32 {
        sim.tick(TICK, |i, m, _| {
            if i == 1 {
                m.send(peer, 1, &tick.to_le_bytes()).unwrap();
            }
            for e in drain(m) {
                log[i].push(e);
            }
        });
    }
    run(&mut sim, 16, &mut log); // drain packets still in flight
    let values: Vec<u32> = log[0]
        .iter()
        .filter_map(|e| match e {
            Ev::Message(_, 1, d) => Some(u32::from_le_bytes(d[..4].try_into().unwrap())),
            _ => None,
        })
        .collect();
    assert!(
        values.windows(2).all(|w| w[0] < w[1]),
        "sequenced delivery must be monotonic"
    );
    let stale = sim.nodes[0].manager.peer_stats(speer).unwrap().messages_stale;
    assert!(stale > 0, "jitter of 80 ms at 64 Hz must reorder some packets");
    assert_eq!(values.len() as u64 + stale, 640);
}

#[test]
fn b4_no_standalone_acks_with_bidirectional_traffic() {
    let (mut sim, peer, mut log) = connected_pair(15, Link::default());
    let speer = server_peer(&log);
    let before = (
        sim.nodes[0].manager.peer_stats(speer).unwrap().standalone_acks_sent,
        sim.nodes[1].manager.peer_stats(peer).unwrap().standalone_acks_sent,
    );
    for _ in 0..640 {
        sim.tick(TICK, |i, m, _| {
            let target = if i == 0 { speer } else { peer };
            m.send(target, 0, &[0u8; 40]).unwrap();
            log[i].extend(drain(m));
        });
    }
    let after = (
        sim.nodes[0].manager.peer_stats(speer).unwrap().standalone_acks_sent,
        sim.nodes[1].manager.peer_stats(peer).unwrap().standalone_acks_sent,
    );
    assert_eq!(before, after, "acks must ride on data packets");
}

#[test]
fn rtt_is_measured_from_acks() {
    let link = Link {
        latency: Duration::from_millis(50),
        ..Link::default()
    };
    let (mut sim, peer, mut log) = connected_pair(16, link);
    let speer = server_peer(&log);
    for _ in 0..256 {
        sim.tick(TICK, |i, m, _| {
            let target = if i == 0 { speer } else { peer };
            m.send(target, 0, &[1]).unwrap();
            log[i].extend(drain(m));
        });
    }
    // The ack delay (up to one tick) is reported and subtracted: only wire time remains.
    let ms = Duration::from_millis(1);
    for (node, p) in [(1, peer), (0, speer)] {
        let rtt = sim.nodes[node].manager.peer_rtt(p).unwrap();
        assert!(rtt >= ms * 100 && rtt <= ms * 101, "node {node}: {rtt:?}");
    }
    // Retransmission timing still accounts for the time acks really take.
    let rto = sim.nodes[1].manager.peer_rto(peer).unwrap();
    assert!(rto > ms * 101, "{rto:?}");
}

#[test]
fn rtt_does_not_depend_on_remote_frame_rate() {
    // Server at 64 Hz, client flushing at 16 fps (a 62.5 ms frame): before ack delay was
    // reported, the server saw up to +62 ms of RTT for this client.
    let link = Link {
        latency: Duration::from_millis(40),
        jitter: Duration::from_millis(4),
        ..Link::default()
    };
    let (mut sim, peer, mut log) = connected_pair(18, link);
    let speer = server_peer(&log);
    for tick in 0..64 * 20u32 {
        sim.advance(TICK);
        for (i, l) in log.iter_mut().enumerate() {
            let now = sim.now;
            let m = sim.manager(i);
            l.extend(drain(m));
            if i == 0 || tick % 4 == 0 {
                m.send(if i == 0 { speer } else { peer }, 0, &[1]).unwrap();
                m.flush(now);
            }
        }
        sim.transmit();
    }
    let ms = Duration::from_millis(1);
    let server_view = sim.nodes[0].manager.peer_rtt(speer).unwrap();
    let client_view = sim.nodes[1].manager.peer_rtt(peer).unwrap();
    // 80 ms of base latency plus on average 4 ms of jitter over the two directions.
    for (who, rtt) in [("server", server_view), ("client", client_view)] {
        assert!(rtt >= ms * 80 && rtt <= ms * 88, "{who}: {rtt:?}");
    }
    assert!(
        server_view.abs_diff(client_view) < ms * 4,
        "{server_view:?} vs {client_view:?}"
    );
}

/// Sizes of the datagrams produced by one flush of `node`.
fn flush_sizes(sim: &mut Sim, node: usize) -> Vec<usize> {
    let now = sim.now;
    let m = sim.manager(node);
    m.flush(now);
    let mut buf = vec![0; 1500];
    let mut sizes = Vec::new();
    while let Some((_, n)) = m.poll_transmit(&mut buf) {
        sizes.push(n);
    }
    sizes
}

#[test]
fn b3_per_message_overhead_in_merged_packet() {
    let (mut sim, peer, _) = connected_pair(17, Link::default());
    flush_sizes(&mut sim, 1);
    // 20 messages of 40 bytes on one channel: header (<= 8) + block (2) + 19 length bytes.
    for _ in 0..20 {
        sim.manager(1).send(peer, 0, &[7; 40]).unwrap();
    }
    let sizes = flush_sizes(&mut sim, 1);
    assert_eq!(sizes.len(), 1);
    let overhead = sizes[0] - 800;
    assert!(overhead <= 8 + 2 + 19, "overhead {overhead}");
    // 1 byte of length up to 127 bytes, 2 bytes from 128.
    for (len, per_msg) in [(127usize, 1usize), (128, 2)] {
        for _ in 0..5 {
            sim.manager(1).send(peer, 0, &vec![1; len]).unwrap();
        }
        let sizes = flush_sizes(&mut sim, 1);
        assert_eq!(sizes.len(), 1);
        let overhead = sizes[0] - 5 * len;
        assert!(overhead <= 8 + 1 + 4 * per_msg, "len {len}: overhead {overhead}");
    }
    // A lone message pays no length prefix: header + block byte only.
    sim.manager(1).send(peer, 1, &[1; 48]).unwrap();
    let sizes = flush_sizes(&mut sim, 1);
    assert!(sizes[0] - 48 <= 8 + 1, "lone message overhead {}", sizes[0] - 48);
}

#[test]
fn s1_ping_replies_are_rate_limited() {
    let (mut sim, peer, log) = connected_pair(19, Link::default());
    let speer = server_peer(&log);
    let before = sim.nodes[0].manager.peer_stats(speer).unwrap().ping_acks_sent;
    // A flood of PING-flagged packets "from the client" with fresh sequence numbers.
    let (from, to) = (sim.addr(1), sim.addr(0));
    for i in 0..1000u16 {
        let seq = 30_000u16.wrapping_add(i);
        let mut p = vec![0x20];
        p.extend_from_slice(&seq.to_le_bytes());
        sim.inject(1, from, to, p);
        sim.advance(Duration::from_millis(1));
    }
    let after = sim.nodes[0].manager.peer_stats(speer).unwrap().ping_acks_sent;
    // One immediate reply per ping_interval / 4 (250 ms) at most: 1 s of flood -> <= 5.
    assert!(after - before <= 5, "{} immediate replies", after - before);
    let _ = peer;
}
