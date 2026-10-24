//! The driver over real localhost UDP.

use std::time::{Duration, Instant};

use netlib::{ChannelConfig, Config, DeliveryMode, DriverConfig, Event, Manager, PeerId, PeerState};

const TICK: Duration = Duration::from_micros(15_625);

fn config(server: bool) -> Config {
    Config {
        protocol_id: 77,
        accept_incoming: server,
        channels: vec![
            ChannelConfig::new(DeliveryMode::Unreliable),
            ChannelConfig::new(DeliveryMode::ReliableOrdered),
        ],
        ..Config::default()
    }
}

/// Runs a server and a client at 64 Hz for `seconds`; returns the RTT each side measured.
fn session(receive_thread: bool, seconds: u64) -> (Duration, Duration, u64) {
    let driver = DriverConfig { receive_thread };
    let mut server = Manager::bind_with("127.0.0.1:0", config(true), driver.clone()).unwrap();
    let mut client = Manager::bind_with("127.0.0.1:0", config(false), driver).unwrap();
    let peer = client.connect(server.local_addr().unwrap(), b"hi").unwrap().unwrap();
    let mut speer: Option<PeerId> = None;
    let mut echoed = 0u64;
    let start = Instant::now();
    let mut next = start;
    while start.elapsed() < Duration::from_secs(seconds) {
        server.poll().unwrap();
        let mut echo = Vec::new();
        while let Some(e) = server.poll_event() {
            match e {
                Event::ConnectRequest { peer, .. } => echo.push((peer, None)),
                Event::Connected { peer, .. } => speer = Some(peer),
                Event::Message { peer, data, .. } => echo.push((peer, Some(data.to_vec()))),
                _ => {}
            }
        }
        for (p, d) in echo {
            match d {
                None => server.accept(p).unwrap(),
                Some(d) => server.send(p, 0, &d).unwrap(),
            }
        }
        server.flush();
        // The client runs half a tick out of phase with the server, like a real remote host.
        std::thread::sleep(TICK / 2);
        client.poll().unwrap();
        while let Some(e) = client.poll_event() {
            if let Event::Message { .. } = e {
                echoed += 1;
            }
        }
        if client.peer_state(peer) == Some(PeerState::Connected) {
            client.send(peer, 0, &[0; 32]).unwrap();
        }
        client.flush();
        next += TICK;
        std::thread::sleep(next.saturating_duration_since(Instant::now()));
    }
    let client_rtt = client.peer_rtt(peer).expect("client measured rtt");
    let server_rtt = server
        .peer_rtt(speer.expect("server connected"))
        .expect("server measured rtt");
    (client_rtt, server_rtt, echoed)
}

#[test]
fn echo_over_localhost_with_receive_thread() {
    let (client_rtt, server_rtt, echoed) = session(true, 3);
    assert!(echoed > 100, "echoed {echoed}");
    // Localhost RTT is well below a millisecond; the waiting time until the next poll
    // (half a tick here, ~7.8 ms) must not show up in the measurement.
    for (who, rtt) in [("client", client_rtt), ("server", server_rtt)] {
        assert!(rtt < Duration::from_millis(4), "{who}: {rtt:?}");
    }
}

#[test]
fn echo_over_localhost_without_receive_thread() {
    let (client_rtt, _, echoed) = session(false, 2);
    assert!(echoed > 50, "echoed {echoed}");
    // Without arrival timestamps the wait for the next poll is part of the RTT, and with it
    // the scheduling noise of `sleep` (tens of ms on CI macOS runners), so only a loose bound.
    assert!(client_rtt < Duration::from_millis(100), "{client_rtt:?}");
}

#[test]
fn m2_rebind_keeps_the_connection() {
    let mut server = Manager::bind("127.0.0.1:0", config(true)).unwrap();
    let mut client = Manager::bind("127.0.0.1:0", config(false)).unwrap();
    let peer = client.connect(server.local_addr().unwrap(), &[]).unwrap().unwrap();
    let mut speer = None;
    let mut received = Vec::new();
    let mut sent = 0u32;
    let mut disconnected = false;
    let old_addr = client.local_addr().unwrap();
    let mut new_addr = None;
    let start = Instant::now();
    let mut next = start;
    while start.elapsed() < Duration::from_secs(3) {
        if new_addr.is_none() && start.elapsed() > Duration::from_secs(1) {
            client.rebind().unwrap();
            new_addr = Some(client.local_addr().unwrap());
        }
        server.poll().unwrap();
        let mut accept = Vec::new();
        while let Some(e) = server.poll_event() {
            match e {
                Event::ConnectRequest { peer, .. } => accept.push(peer),
                Event::Connected { peer, .. } => speer = Some(peer),
                Event::Message { channel: 1, data, .. } => received.push(u32::from_le_bytes(data.try_into().unwrap())),
                Event::Disconnected { .. } => disconnected = true,
                _ => {}
            }
        }
        for p in accept {
            server.accept(p).unwrap();
        }
        server.flush();
        client.poll().unwrap();
        while let Some(e) = client.poll_event() {
            if let Event::Disconnected { .. } = e {
                disconnected = true;
            }
        }
        if client.peer_state(peer) == Some(PeerState::Connected) && start.elapsed() < Duration::from_millis(2500) {
            client.send(peer, 1, &sent.to_le_bytes()).unwrap();
            sent += 1;
        }
        client.flush();
        next += TICK;
        std::thread::sleep(next.saturating_duration_since(Instant::now()));
    }
    let new_addr = new_addr.unwrap();
    assert_ne!(old_addr, new_addr, "the new socket has a new port");
    assert!(!disconnected);
    assert_eq!(server.peer_addr(speer.unwrap()), Some(new_addr));
    assert_eq!(
        received,
        (0..sent).collect::<Vec<_>>(),
        "reliable stream across the rebind"
    );
    assert_eq!(client.socket_stats().rebinds, 1);
}
