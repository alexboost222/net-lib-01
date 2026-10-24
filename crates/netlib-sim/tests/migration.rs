//! Connection migration (M1-M5): the client's address changes without a disconnect.

use std::net::SocketAddr;
use std::time::Duration;

use netlib_core::{ChannelConfig, Config, DeliveryMode, Event, PeerId, PeerState};
use netlib_sim::{Link, Sim};

const TICK: Duration = Duration::from_micros(15_625);

fn config(server: bool) -> Config {
    Config {
        protocol_id: 4,
        accept_incoming: server,
        channels: vec![
            ChannelConfig::new(DeliveryMode::UnreliableSequenced),
            ChannelConfig::new(DeliveryMode::ReliableOrdered),
        ],
        ..Config::default()
    }
}

struct Session {
    sim: Sim,
    cpeer: PeerId,
    speer: PeerId,
    /// Reliable ids received by the server, unreliable ticks received by the server.
    reliable: Vec<u32>,
    unreliable: Vec<u32>,
    /// Messages received by the client from the server.
    to_client: Vec<u32>,
    disconnects: Vec<String>,
    sent: u32,
    tick: u32,
}

impl Session {
    fn new(seed: u64, link: Link) -> Self {
        let mut sim = Sim::new(seed, link);
        sim.add_node(config(true)).unwrap();
        sim.add_node(config(false)).unwrap();
        let addr = sim.addr(0);
        let now = sim.now;
        let cpeer = sim.manager(1).connect(now, addr, &[]).unwrap();
        sim.transmit();
        let mut speer = None;
        for _ in 0..64 * 3 {
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
        assert_eq!(sim.nodes[1].manager.peer_state(cpeer), Some(PeerState::Connected));
        Session {
            sim,
            cpeer,
            speer: speer.unwrap(),
            reliable: Vec::new(),
            unreliable: Vec::new(),
            to_client: Vec::new(),
            disconnects: Vec::new(),
            sent: 0,
            tick: 0,
        }
    }

    /// Client sends one reliable and one unreliable message per tick; the server answers each tick.
    fn run(&mut self, ticks: u32) {
        for _ in 0..ticks {
            let (cpeer, speer, t) = (self.cpeer, self.speer, self.tick);
            let Session {
                sim,
                reliable,
                unreliable,
                to_client,
                disconnects,
                sent,
                ..
            } = self;
            sim.tick(TICK, |i, m, _| {
                while let Some(e) = m.poll_event() {
                    match e {
                        Event::Message { channel, data, .. } => {
                            let v = u32::from_le_bytes(data[..4].try_into().unwrap());
                            match (i, channel) {
                                (0, 1) => reliable.push(v),
                                (0, _) => unreliable.push(v),
                                _ => to_client.push(v),
                            }
                        }
                        Event::Disconnected { reason, .. } => disconnects.push(format!("{i}: {reason:?}")),
                        _ => {}
                    }
                }
                if i == 1 {
                    m.send(cpeer, 1, &sent.to_le_bytes()).unwrap();
                    m.send(cpeer, 0, &t.to_le_bytes()).unwrap();
                    *sent += 1;
                } else {
                    m.send(speer, 0, &t.to_le_bytes()).unwrap();
                }
            });
            self.tick += 1;
        }
    }

    /// Runs without sending so that in-flight reliable messages arrive.
    fn settle(&mut self) {
        for _ in 0..64 * 2 {
            let Session {
                sim,
                reliable,
                disconnects,
                ..
            } = self;
            sim.tick(TICK, |i, m, _| {
                while let Some(e) = m.poll_event() {
                    match e {
                        Event::Message { channel: 1, data, .. } if i == 0 => {
                            reliable.push(u32::from_le_bytes(data[..4].try_into().unwrap()))
                        }
                        Event::Disconnected { reason, .. } => disconnects.push(format!("{i}: {reason:?}")),
                        _ => {}
                    }
                }
            });
        }
    }

    /// Whether the server received reliable messages 0..sent exactly once, in order.
    fn reliable_intact(&self) -> bool {
        self.reliable.len() == self.sent as usize && self.reliable.iter().enumerate().all(|(i, &v)| v == i as u32)
    }

    fn move_client(&mut self, last_octet: u8) -> SocketAddr {
        let addr: SocketAddr = format!("10.7.0.{last_octet}:5555").parse().unwrap();
        self.sim.set_addr(1, addr);
        addr
    }
}

#[test]
fn m1_nat_rebinding_mid_reliable_stream() {
    let mut s = Session::new(
        1,
        Link {
            latency: Duration::from_millis(30),
            loss: 0.02,
            ..Link::default()
        },
    );
    s.run(64 * 2);
    for k in 0..5 {
        let addr = s.move_client(10 + k);
        s.run(64 * 2);
        assert_eq!(
            s.sim.nodes[0].manager.peer_addr(s.speer),
            Some(addr),
            "server follows move {k}"
        );
    }
    s.run(64 * 3);
    s.settle();
    assert!(s.disconnects.is_empty(), "{:?}", s.disconnects);
    assert!(
        s.reliable_intact(),
        "reliable stream: {} of {}",
        s.reliable.len(),
        s.sent
    );
    let stats = s.sim.nodes[0].manager.stats().clone();
    assert_eq!(stats.migrations, 5, "{stats:?}");
    assert_eq!(stats.failed_path_validations, 0);
    assert!(
        s.to_client.len() as u32 > s.tick * 9 / 10,
        "server -> client keeps flowing"
    );
}

#[test]
fn m2_socket_recreation_is_announced_and_seamless() {
    let mut s = Session::new(
        2,
        Link {
            latency: Duration::from_millis(30),
            ..Link::default()
        },
    );
    s.run(64);
    // The app was in background for 3 s: the OS closed the socket; a new one gets a new port.
    s.sim.nodes[1].partitioned = true;
    s.run(64 * 3);
    s.sim.nodes[1].partitioned = false;
    s.move_client(20);
    s.sim.manager(1).local_address_changed();
    s.run(64 * 3);
    s.settle();
    assert!(s.disconnects.is_empty(), "{:?}", s.disconnects);
    assert!(
        s.reliable_intact(),
        "reliable stream: {} of {}",
        s.reliable.len(),
        s.sent
    );
    assert_eq!(
        s.sim.nodes[0].manager.stats().who_are_you_sent,
        0,
        "no extra round trip needed"
    );
}

#[test]
fn m5_recovery_time() {
    let latency = Duration::from_millis(40);
    for (announced, seed) in [(true, 3), (false, 4)] {
        let mut s = Session::new(
            seed,
            Link {
                latency,
                ..Link::default()
            },
        );
        s.run(64);
        let before = s.unreliable.len();
        let moved_at = s.tick;
        s.move_client(30);
        if announced {
            s.sim.manager(1).local_address_changed();
        }
        s.run(64);
        // Ticks whose unreliable message never arrived: the recovery gap.
        let got: std::collections::HashSet<u32> = s.unreliable[before..].iter().copied().collect();
        let first = (moved_at..s.tick).find(|t| got.contains(t)).unwrap();
        let gap = TICK * (first - moved_at);
        // Announced: the first packet from the new address is accepted (+1 tick for the
        // tick boundary). NAT rebinding: one extra round trip for WHO_ARE_YOU.
        let limit = if announced { TICK } else { latency * 2 + TICK * 2 };
        assert!(gap <= limit, "announced={announced}: gap {gap:?} > {limit:?}");
    }
}

#[test]
fn m3_hijack_without_token_is_rejected() {
    let mut s = Session::new(5, Link::default());
    s.run(32);
    let attacker: SocketAddr = "10.66.6.6:6666".parse().unwrap();
    let to = s.sim.addr(0);
    // Forged migration: right conn_id is unknown, so try many; also a wrong token.
    for conn_id in 0..2000u32 {
        let mut p = vec![0x40, 0xff, 0x7f];
        p.extend_from_slice(&conn_id.to_le_bytes());
        p.extend_from_slice(&0xdead_beef_u64.to_le_bytes());
        p.extend_from_slice(&[0x01, 0x00]); // block: channel 0, one message, len 0
        s.sim.inject(1, attacker, to, p);
    }
    s.run(16);
    let m = &s.sim.nodes[0].manager;
    assert_ne!(m.peer_addr(s.speer), Some(attacker));
    assert_eq!(m.stats().migrations, 0);
    assert!(s.disconnects.is_empty());
}

#[test]
fn m3_replayed_migration_packet_cannot_redirect() {
    // An on-path observer copies a genuine migrating packet and replays it from elsewhere.
    let mut s = Session::new(6, Link::default());
    s.run(32);
    s.move_client(40);
    s.sim.manager(1).local_address_changed();
    // Capture the client's next datagram (it carries migration fields).
    let now = s.sim.now;
    s.sim.manager(1).send(s.cpeer, 0, &[0; 4]).unwrap();
    s.sim.manager(1).flush(now);
    let mut buf = vec![0; 1500];
    let (to, n) = s.sim.manager(1).poll_transmit(&mut buf).unwrap();
    let packet = buf[..n].to_vec();
    assert_eq!(packet[0] & 0x40, 0x40);
    let client = s.sim.addr(1);
    s.sim.inject(1, client, to, packet.clone());
    s.sim.advance(Duration::from_millis(20));
    // Replay from a victim address: same seq, so it is a duplicate and ignored.
    let victim: SocketAddr = "10.99.0.1:1".parse().unwrap();
    s.sim.inject(1, victim, to, packet);
    s.run(64);
    let m = &s.sim.nodes[0].manager;
    assert_eq!(m.peer_addr(s.speer), Some(client));
    assert_eq!(m.stats().invalid_migrations, 1, "{:?}", m.stats());
}

#[test]
fn m4_unvalidated_address_gets_bounded_traffic() {
    // The client "moves" to an address whose replies never come back (one-way path):
    // the server sends at most 3x what it received there, then reverts.
    let mut s = Session::new(7, Link::default());
    s.run(32);
    let server = s.sim.addr(0);
    let original = s.sim.addr(1);
    let bytes_before = s.sim.nodes[0].manager.peer_stats(s.speer).unwrap().bytes_sent;
    s.move_client(50);
    s.sim.manager(1).local_address_changed();
    // Only one packet from the new address reaches the server, then the path breaks.
    let now = s.sim.now;
    s.sim.manager(1).send(s.cpeer, 0, &[0; 4]).unwrap();
    s.sim.manager(1).flush(now);
    let mut buf = vec![0; 1500];
    let (_, n) = s.sim.manager(1).poll_transmit(&mut buf).unwrap();
    while s.sim.manager(1).poll_transmit(&mut [0; 1500]).is_some() {}
    let from = s.sim.addr(1);
    s.sim.inject(1, from, server, buf[..n].to_vec());
    s.sim.nodes[1].partitioned = true;
    s.sim.set_addr(1, original); // back to the old address
    let mut server_out = 0;
    for _ in 0..128 {
        let speer = s.speer;
        s.sim.tick(TICK, |i, m, _| {
            if i == 0 {
                let _ = m.send(speer, 0, &[1; 200]);
            }
            while m.poll_event().is_some() {}
        });
        if s.sim.nodes[0].manager.stats().failed_path_validations > 0 {
            // Reverted during this tick; what followed went to the old, validated address.
            break;
        }
        server_out = s.sim.nodes[0].manager.peer_stats(s.speer).unwrap().bytes_sent - bytes_before;
    }
    let m = &s.sim.nodes[0].manager;
    assert_eq!(m.stats().failed_path_validations, 1, "the move was reverted");
    assert!(
        server_out as usize <= 3 * n + 64,
        "sent {server_out} to an unvalidated path after receiving {n}"
    );
    assert_eq!(m.peer_addr(s.speer), Some(original));
}

#[test]
fn idle_connection_migrates_after_probe() {
    // No traffic but keepalives; the NAT mapping changes; the client notices the silence.
    let mut s = Session::new(8, Link::default());
    s.move_client(60);
    for _ in 0..64 * 5 {
        s.sim.tick(TICK, |_, m, _| while m.poll_event().is_some() {});
    }
    let m = &s.sim.nodes[0].manager;
    assert_eq!(m.peer_addr(s.speer), Some(s.sim.addr(1)));
    assert_eq!(s.sim.nodes[1].manager.peer_state(s.cpeer), Some(PeerState::Connected));
}
