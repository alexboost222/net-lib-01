//! Edges of the public API: id reuse, slot accounting, data limits, counters.

use std::time::Duration;

use netlib_core::{
    ChannelConfig, Config, ConnectError, DeliveryMode, Event, Manager, PeerError, PeerId, PeerState, SendError,
};
use netlib_sim::{Link, Sim};

const TICK: Duration = Duration::from_micros(15_625);

fn config(server: bool) -> Config {
    Config {
        protocol_id: 21,
        accept_incoming: server,
        max_incoming: 1,
        channels: vec![ChannelConfig::new(DeliveryMode::ReliableOrdered)],
        ..Config::default()
    }
}

fn run(sim: &mut Sim, ticks: usize) -> Vec<String> {
    let mut out = Vec::new();
    for _ in 0..ticks {
        sim.tick(TICK, |i, m, now| {
            let mut accept = Vec::new();
            while let Some(e) = m.poll_event() {
                if let Event::ConnectRequest { peer, .. } = e {
                    accept.push(peer);
                }
                out.push(format!("{i} {e:?}"));
            }
            for p in accept {
                m.accept(now, p).unwrap();
            }
        });
    }
    out
}

fn connect(sim: &mut Sim, client: usize) -> PeerId {
    let addr = sim.addr(0);
    let now = sim.now;
    let p = sim.manager(client).connect(now, addr, &[]).unwrap();
    sim.transmit();
    p
}

#[test]
fn stale_peer_ids_never_address_a_new_peer() {
    let mut sim = Sim::new(1, Link::default());
    sim.add_node(config(true)).unwrap();
    sim.add_node(config(false)).unwrap();
    let first = connect(&mut sim, 1);
    run(&mut sim, 32);
    sim.manager(1).disconnect(first, &[]).unwrap();
    run(&mut sim, 8);
    let second = connect(&mut sim, 1);
    run(&mut sim, 32);
    // The slot is reused with a new generation.
    assert_eq!(second.to_bits() as u32, first.to_bits() as u32);
    assert_ne!(second, first);
    let m = sim.manager(1);
    assert_eq!(m.peer_state(first), None);
    assert_eq!(m.peer_state(second), Some(PeerState::Connected));
    assert_eq!(m.send(first, 0, b"x"), Err(SendError::NotConnected));
    assert_eq!(m.disconnect(first, &[]), Err(PeerError::UnknownPeer));
    assert_eq!(m.peers().collect::<Vec<_>>(), vec![second]);
}

#[test]
fn server_slots_are_freed_on_disconnect() {
    let mut sim = Sim::new(2, Link::default());
    sim.add_node(config(true)).unwrap();
    sim.add_node(config(false)).unwrap();
    sim.add_node(config(false)).unwrap();
    let a = connect(&mut sim, 1);
    run(&mut sim, 32);
    let b = connect(&mut sim, 2);
    let log = run(&mut sim, 32);
    assert!(
        log.iter().any(|l| l.starts_with("2 ") && l.contains("ServerFull")),
        "{log:?}"
    );
    assert_eq!(sim.nodes[2].manager.peer_state(b), None);
    // The first client leaves; its slot and connection id are released.
    sim.manager(1).disconnect(a, &[]).unwrap();
    run(&mut sim, 8);
    let b = connect(&mut sim, 2);
    run(&mut sim, 32);
    assert_eq!(sim.nodes[2].manager.peer_state(b), Some(PeerState::Connected));
    assert_eq!(sim.nodes[0].manager.peers().count(), 1);
}

#[test]
fn data_limits_are_inclusive() {
    let mut sim = Sim::new(3, Link::default());
    sim.add_node(config(true)).unwrap();
    sim.add_node(config(false)).unwrap();
    let max = sim.nodes[1].manager.config().max_connect_data;
    let addr = sim.addr(0);
    let now = sim.now;
    let m = sim.manager(1);
    assert_eq!(m.connect(now, addr, &vec![0; max + 1]), Err(ConnectError::DataTooLarge));
    let p = m.connect(now, addr, &vec![0; max]).unwrap();
    assert_eq!(m.connect(now, addr, &[]), Err(ConnectError::AlreadyExists));
    assert_eq!(m.disconnect(p, &vec![0; max + 1]), Err(PeerError::DataTooLarge));
    assert_eq!(m.disconnect(p, &vec![0; max]), Ok(()));
    // Server side reject limits.
    let p = connect(&mut sim, 1);
    let mut pending = None;
    for _ in 0..16 {
        sim.tick(TICK, |i, m, _| {
            while let Some(e) = m.poll_event() {
                if let (0, Event::ConnectRequest { peer, .. }) = (i, e) {
                    pending = Some(peer);
                }
            }
        });
    }
    let pending = pending.unwrap();
    let now = sim.now;
    let s = sim.manager(0);
    assert_eq!(s.reject(pending, &vec![0; max + 1]), Err(PeerError::DataTooLarge));
    assert_eq!(s.accept(now, PeerId::from_bits(u64::MAX)), Err(PeerError::UnknownPeer));
    assert_eq!(s.reject(pending, &vec![0; max]), Ok(()));
    assert_eq!(s.accept(now, pending), Err(PeerError::UnknownPeer));
    let _ = p;
}

#[test]
fn handshake_packets_are_counted() {
    let mut sim = Sim::new(4, Link::default());
    sim.add_node(config(true)).unwrap();
    sim.add_node(config(false)).unwrap();
    let p = connect(&mut sim, 1);
    run(&mut sim, 2);
    let s = sim.nodes[1].manager.peer_stats(p).unwrap();
    // CONNECT_REQUEST (32 bytes) and CHALLENGE_RESPONSE (22 bytes without data).
    assert_eq!(s.packets_sent, 2, "{s:?}");
    assert_eq!(s.bytes_sent, 32 + 22);
}

#[test]
fn pending_peers_are_not_connected_yet() {
    let mut sim = Sim::new(5, Link::default());
    sim.add_node(config(true)).unwrap();
    sim.add_node(config(false)).unwrap();
    connect(&mut sim, 1);
    let mut pending = None;
    for _ in 0..8 {
        sim.tick(TICK, |i, m, _| {
            while let Some(e) = m.poll_event() {
                if let (0, Event::ConnectRequest { peer, .. }) = (i, e) {
                    pending = Some(peer);
                }
            }
        });
    }
    let pending = pending.unwrap();
    let m: &mut Manager = sim.manager(0);
    assert_eq!(m.peer_state(pending), Some(PeerState::Pending));
    assert_eq!(m.send(pending, 0, b"x"), Err(SendError::NotConnected));
    assert_eq!(
        m.disconnect(pending, b"no"),
        Ok(()),
        "disconnecting a pending peer rejects it"
    );
    assert_eq!(m.peer_state(pending), None);
}
