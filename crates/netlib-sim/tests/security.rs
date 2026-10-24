//! Handshake hardening: amplification (S1) and state before address validation (S2).
//! Packets are built byte by byte from docs/WIRE_FORMAT.md, so these tests also pin the format.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use netlib_core::{Config, Event, Manager, Time};
use netlib_sim::SimRng;

const PROTOCOL: u32 = 0x4e45_5401;

fn server(now: Time) -> Manager {
    let config = Config {
        protocol_id: PROTOCOL,
        accept_incoming: true,
        ..Config::default()
    };
    Manager::new(config, [7; 32], now).unwrap()
}

fn connect_request(protocol: u32, version: u16, nonce: u64) -> Vec<u8> {
    let mut p = vec![1u8];
    p.extend_from_slice(&protocol.to_le_bytes());
    p.extend_from_slice(&version.to_le_bytes());
    p.extend_from_slice(&nonce.to_le_bytes());
    p.resize(32, 0);
    p
}

fn challenge_response(nonce: u64, timestamp: u32, mac: u64, data: &[u8]) -> Vec<u8> {
    let mut p = vec![3u8];
    p.extend_from_slice(&nonce.to_le_bytes());
    p.extend_from_slice(&timestamp.to_le_bytes());
    p.extend_from_slice(&mac.to_le_bytes());
    p.push(data.len() as u8);
    p.extend_from_slice(data);
    p
}

/// Parses a CHALLENGE: `(nonce, timestamp, mac)`.
fn parse_challenge(p: &[u8]) -> (u64, u32, u64) {
    assert_eq!((p[0], p.len()), (2, 21));
    (
        u64::from_le_bytes(p[1..9].try_into().unwrap()),
        u32::from_le_bytes(p[9..13].try_into().unwrap()),
        u64::from_le_bytes(p[13..21].try_into().unwrap()),
    )
}

fn addr(i: u32) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::from(0x0a00_0000 + i), 40_000 + (i % 1000) as u16))
}

fn transmits(m: &mut Manager) -> Vec<(SocketAddr, Vec<u8>)> {
    let mut buf = vec![0; 1500];
    let mut out = Vec::new();
    while let Some((to, n)) = m.poll_transmit(&mut buf) {
        out.push((to, buf[..n].to_vec()));
    }
    out
}

#[test]
fn s1_no_amplification_for_unvalidated_addresses() {
    let mut now = Time::from_millis(1000);
    let mut m = server(now);
    let mut rng = SimRng::new(1);
    let mut received: HashMap<SocketAddr, usize> = HashMap::new();
    let mut sent: HashMap<SocketAddr, usize> = HashMap::new();
    for i in 0..20_000u32 {
        let from = addr(rng.next_u64() as u32 % 500);
        let kind = rng.next_u64() % 6;
        let packet = match kind {
            0 => connect_request(PROTOCOL, 0, rng.next_u64()),
            1 => connect_request(PROTOCOL, 1 + (i % 5) as u16, rng.next_u64()),
            2 => challenge_response(rng.next_u64(), 0, rng.next_u64(), b"x"),
            3 => {
                // Random garbage of random size, any type byte.
                let n = 1 + (rng.next_u64() % 1200) as usize;
                (0..n).map(|_| rng.next_u64() as u8).collect()
            }
            4 => {
                // DATA with a valid-looking header from a stranger.
                let mut p = vec![0x30, 1, 0, 0, 0, 0, 0];
                p.extend(std::iter::repeat_n(0x11, (rng.next_u64() % 100) as usize));
                p
            }
            _ => connect_request(PROTOCOL, 0, rng.next_u64())[..20].to_vec(), // short request
        };
        *received.entry(from).or_default() += packet.len();
        m.handle_datagram(now, from, &packet);
        m.update(now);
        m.flush(now);
        for (to, p) in transmits(&mut m) {
            *sent.entry(to).or_default() += p.len();
        }
        now = now + Duration::from_micros(500);
    }
    assert!(!sent.is_empty(), "the server answered valid requests");
    for (a, &out) in &sent {
        let inb = received[a];
        assert!(out <= inb, "{a}: sent {out} bytes in reply to {inb}");
    }
    assert_eq!(m.peers().count(), 0, "no peer without a valid cookie");
}

#[test]
fn s2_flood_of_connect_requests_creates_no_state() {
    let now = Time::from_millis(1000);
    let mut m = server(now);
    for i in 0..10_000 {
        m.handle_datagram(now, addr(i), &connect_request(PROTOCOL, 0, i as u64));
    }
    m.update(now);
    assert_eq!(m.peers().count(), 0);
    assert_eq!(m.stats().challenges_sent, 10_000);
    // The only memory used is the transmit queue, which the driver drains every tick.
    assert_eq!(transmits(&mut m).len(), 10_000);
}

#[test]
fn s2_cookie_is_bound_to_address_nonce_and_time() {
    let start = Time::from_millis(1000);
    let mut m = server(start);
    let client = addr(1);
    m.handle_datagram(start, client, &connect_request(PROTOCOL, 0, 42));
    let (nonce, ts, mac) = parse_challenge(&transmits(&mut m)[0].1);
    assert_eq!(nonce, 42);

    let cases = [
        ("other address", addr(2), challenge_response(42, ts, mac, &[]), start),
        ("other nonce", client, challenge_response(43, ts, mac, &[]), start),
        (
            "other timestamp",
            client,
            challenge_response(42, ts + 1, mac, &[]),
            start,
        ),
        (
            "flipped mac bit",
            client,
            challenge_response(42, ts, mac ^ 1, &[]),
            start,
        ),
        (
            "expired",
            client,
            challenge_response(42, ts, mac, &[]),
            start + Duration::from_secs(11),
        ),
    ];
    for (name, from, packet, at) in &cases {
        let before = m.stats().invalid_cookies;
        m.handle_datagram(*at, *from, packet);
        assert_eq!(m.stats().invalid_cookies, before + 1, "{name}");
        assert_eq!(m.peers().count(), 0, "{name}");
        assert!(m.poll_event().is_none(), "{name}");
    }

    // Positive control: the genuine response creates exactly one pending peer.
    m.handle_datagram(
        start + Duration::from_secs(9),
        client,
        &challenge_response(42, ts, mac, b"key"),
    );
    assert_eq!(m.peers().count(), 1);
    assert!(matches!(
        m.poll_event(),
        Some(Event::ConnectRequest { data: b"key", .. })
    ));
    // A replay does not create a second peer or a second event.
    m.handle_datagram(
        start + Duration::from_secs(9),
        client,
        &challenge_response(42, ts, mac, b"key"),
    );
    assert_eq!(m.peers().count(), 1);
    assert!(m.poll_event().is_none());
}

#[test]
fn s2_unanswered_pending_peers_expire() {
    let start = Time::from_millis(1000);
    let mut m = server(start);
    for i in 0..10 {
        m.handle_datagram(start, addr(i), &connect_request(PROTOCOL, 0, 1));
    }
    for (to, p) in transmits(&mut m) {
        let (nonce, ts, mac) = parse_challenge(&p);
        m.handle_datagram(start, to, &challenge_response(nonce, ts, mac, &[]));
    }
    assert_eq!(m.peers().count(), 10);
    m.update(start + Duration::from_secs(5));
    assert_eq!(m.peers().count(), 0, "pending peers live at most connect_timeout");
}
