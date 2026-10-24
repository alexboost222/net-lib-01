//! Driver API surface: sinks, queries, rejects, disconnects and options over real UDP.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use netlib::log::Level;
use netlib::logging::{ClefSink, TextSink, WallClock};
use netlib::{
    ChannelConfig, Config, DeliveryMode, DisconnectReason, DriverConfig, Event, Manager, PeerState, SendError,
    SendOptions,
};

#[derive(Clone, Default)]
struct Shared(Arc<Mutex<Vec<u8>>>);

impl Write for Shared {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Shared {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

fn config(server: bool) -> Config {
    Config {
        protocol_id: 31,
        accept_incoming: server,
        channels: vec![
            ChannelConfig::new(DeliveryMode::Unreliable),
            ChannelConfig::new(DeliveryMode::ReliableOrdered),
        ],
        ..Config::default()
    }
}

/// Polls both managers for up to 2 s until `done` holds; returns all events as text.
fn pump(a: &mut Manager, b: &mut Manager, mut done: impl FnMut(&[String]) -> bool) -> Vec<String> {
    let mut seen = Vec::new();
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(2) && !done(&seen) {
        for m in [&mut *a, &mut *b] {
            m.poll().unwrap();
            while let Some(e) = m.poll_event() {
                seen.push(format!("{e:?}"));
            }
            m.flush();
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    seen
}

#[test]
fn sinks_write_text_and_clef() {
    let text = Shared::default();
    let clef = Shared::default();
    let mut server = Manager::bind("127.0.0.1:0", config(true)).unwrap();
    let mut client = Manager::bind("127.0.0.1:0", config(false)).unwrap();
    server.set_log_sink(Some(Box::new(TextSink::new(
        text.clone(),
        Level::Debug,
        WallClock::new(server.epoch()),
    ))));
    client.set_log_sink(Some(Box::new(ClefSink::new(
        clef.clone(),
        Level::Info,
        WallClock::fixed(0),
    ))));
    let peer = client.connect(server.local_addr().unwrap(), b"x").unwrap().unwrap();
    let mut speer = None;
    let start = Instant::now();
    while speer.is_none() && start.elapsed() < Duration::from_secs(2) {
        server.poll().unwrap();
        let mut req = None;
        while let Some(e) = server.poll_event() {
            if let Event::ConnectRequest { peer, .. } = e {
                req = Some(peer);
            }
        }
        if let Some(p) = req {
            server.accept(p).unwrap();
            speer = Some(p);
        }
        server.flush();
        client.poll().unwrap();
        while client.poll_event().is_some() {}
        client.flush();
        std::thread::sleep(Duration::from_millis(2));
    }
    let _ = peer;
    pump(&mut server, &mut client, |_| clef.text().contains("Connected to"));
    let t = text.text();
    assert!(t.contains(" INF [") && t.contains("connected from 127.0.0.1:"), "{t}");
    let year = &t[..4];
    assert!(year.parse::<u32>().unwrap() >= 2024, "wall clock time: {t}");
    let c = clef.text();
    let line = c
        .lines()
        .find(|l| l.contains("Connected to"))
        .expect("client logged the connection");
    assert!(line.starts_with("{\"@t\":\"1970-01-01T") && line.contains("\"@mt\":\"Connected to {RemoteEndpoint}"));
    assert!(!c.contains("\"@l\":\"Debug\""), "below the minimum level");
    // A sink can be removed again.
    server.set_log_sink(None);
}

#[test]
fn queries_reject_disconnect_and_options() {
    let mut server = Manager::bind("127.0.0.1:0", config(true)).unwrap();
    let mut client = Manager::bind_with("127.0.0.1:0", config(false), DriverConfig { receive_thread: false }).unwrap();
    // A rejected attempt delivers the reject data.
    let first = client.connect(server.local_addr().unwrap(), b"no").unwrap().unwrap();
    assert_eq!(client.peer_state(first), Some(PeerState::Connecting));
    let mut rejected = false;
    let start = Instant::now();
    while !rejected && start.elapsed() < Duration::from_secs(2) {
        server.poll().unwrap();
        let mut req = None;
        while let Some(e) = server.poll_event() {
            if let Event::ConnectRequest { peer, .. } = e {
                req = Some(peer);
            }
        }
        if let Some(p) = req {
            server.reject(p, b"denied").unwrap();
        }
        server.flush();
        client.poll().unwrap();
        while let Some(e) = client.poll_event() {
            if let Event::Disconnected {
                reason: DisconnectReason::Rejected { data },
                ..
            } = e
            {
                assert_eq!(data, b"denied");
                rejected = true;
            }
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(rejected);
    // Now a real connection.
    let peer = client.connect(server.local_addr().unwrap(), b"yes").unwrap().unwrap();
    let mut speer = None;
    let mut got = Vec::new();
    let start = Instant::now();
    while got.len() < 2 && start.elapsed() < Duration::from_secs(3) {
        server.poll().unwrap();
        let mut req = None;
        while let Some(e) = server.poll_event() {
            match e {
                Event::ConnectRequest { peer, .. } => req = Some(peer),
                Event::Connected { peer, .. } => speer = Some(peer),
                Event::Message { channel, data, .. } => got.push((channel, data.to_vec())),
                _ => {}
            }
        }
        if let Some(p) = req {
            server.accept(p).unwrap();
        }
        server.flush();
        client.poll().unwrap();
        while client.poll_event().is_some() {}
        if client.peer_state(peer) == Some(PeerState::Connected) && got.is_empty() {
            let key = SendOptions {
                replace_key: Some(1),
                ..SendOptions::default()
            };
            client.send_with(peer, 0, b"latest", key).unwrap();
            client.send(peer, 1, b"reliable").unwrap();
            assert_eq!(client.send(peer, 7, b"x"), Err(SendError::InvalidChannel));
        }
        client.flush();
        std::thread::sleep(Duration::from_millis(2));
    }
    got.sort();
    assert_eq!(got, vec![(0, b"latest".to_vec()), (1, b"reliable".to_vec())]);
    let speer = speer.unwrap();
    assert_eq!(server.peer_addr(speer), Some(client.local_addr().unwrap()));
    assert!(client.peer_rtt(peer).is_some() || client.peer_stats(peer).unwrap().packets_sent > 0);
    assert!(client.time_to_next_timeout(Duration::from_secs(60)) <= Duration::from_secs(10));
    assert_eq!(client.socket_stats().rebinds, 0);
    assert!(server.with_core(|c| c.stats().challenges_sent) >= 2);
    assert!(client.now().as_micros() > 0);
    // Disconnect from the client reaches the server with its data.
    client.disconnect(peer, b"bye").unwrap();
    let events = pump(&mut server, &mut client, |seen| {
        seen.iter().any(|e| e.contains("Remote"))
    });
    assert!(
        events
            .iter()
            .any(|e| e.contains("Remote") && e.contains("[98, 121, 101]")),
        "{events:?}"
    );
}

#[test]
fn bind_errors() {
    let bad = Config {
        mtu: 10,
        ..config(false)
    };
    let e = Manager::bind("127.0.0.1:0", bad).err().unwrap();
    assert!(e.to_string().contains("invalid config"));
    let e = Manager::bind("256.0.0.1:0", config(false)).err().unwrap();
    assert!(e.to_string().contains("socket error"));
}
