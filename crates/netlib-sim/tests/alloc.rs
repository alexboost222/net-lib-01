//! P1 / PK5: no heap allocations per tick in steady state.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use netlib_core::{ChannelConfig, Config, DeliveryMode, Event, Manager, PeerId, PeerState, SendOptions, Time};

struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
}

// SAFETY: forwards to the system allocator; only counts calls.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.with(Cell::get) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: same contract as the caller's.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: same contract as the caller's.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.with(Cell::get) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: same contract as the caller's.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Counts allocations made by `f` on this thread.
fn counted<R>(f: impl FnOnce() -> R) -> (R, u64) {
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    COUNTING.with(|c| c.set(true));
    let r = f();
    COUNTING.with(|c| c.set(false));
    (r, ALLOCATIONS.load(Ordering::Relaxed) - before)
}

const TICK: Duration = Duration::from_micros(15_625);

fn config(server: bool) -> Config {
    Config {
        protocol_id: 3,
        accept_incoming: server,
        channels: vec![
            ChannelConfig::new(DeliveryMode::Unreliable),
            ChannelConfig::new(DeliveryMode::UnreliableSequenced).with_priority(200),
            ChannelConfig::new(DeliveryMode::ReliableUnordered),
            ChannelConfig::new(DeliveryMode::ReliableOrdered),
            ChannelConfig::new(DeliveryMode::ReliableSequenced),
        ],
        bandwidth: server.then_some(1_000_000),
        ..Config::default()
    }
}

struct Wire {
    /// Preallocated datagram slots.
    bufs: Vec<([u8; 1500], usize, SocketAddr)>,
}

impl Wire {
    fn carry(&mut self, from: &mut Manager, from_addr: SocketAddr, to: &mut Manager, now: Time) {
        self.bufs.clear();
        let mut buf = [0u8; 1500];
        while let Some((dst, n)) = from.poll_transmit(&mut buf) {
            self.bufs.push((buf, n, dst));
        }
        for (b, n, _) in &self.bufs {
            to.handle_datagram(now, from_addr, &b[..*n]);
        }
    }
}

fn traffic(m: &mut Manager, peer: PeerId, tick: u32) -> u64 {
    let mut received = 0;
    while let Some(e) = m.poll_event() {
        if let Event::Message { data, .. } = e {
            received += data.len() as u64;
        }
    }
    let state = tick.to_le_bytes();
    for entity in 0..16u32 {
        let key = SendOptions {
            replace_key: Some(entity),
            ..SendOptions::default()
        };
        m.send_with(peer, 1, &[state[0]; 24], key).unwrap();
    }
    for _ in 0..8 {
        m.send(peer, 0, &[1; 40]).unwrap();
    }
    m.send(peer, 2, &[2; 30]).unwrap();
    m.send(peer, 3, &[3; 30]).unwrap();
    m.send(peer, 3, &[4; 30]).unwrap();
    m.send(peer, 4, &[5; 12]).unwrap();
    received
}

#[test]
fn p1_pk5_steady_state_does_not_allocate() {
    // The counter works.
    let (v, n) = counted(|| Vec::<u8>::with_capacity(16));
    assert_eq!(n, 1);
    drop(v);
    let saddr: SocketAddr = "10.0.0.1:1".parse().unwrap();
    let caddr: SocketAddr = "10.0.0.2:2".parse().unwrap();
    let mut now = Time::from_millis(1000);
    let mut server = Manager::new(config(true), [1; 32], now).unwrap();
    let mut client = Manager::new(config(false), [2; 32], now).unwrap();
    let mut wire = Wire {
        bufs: Vec::with_capacity(256),
    };
    let cpeer = client.connect(now, saddr, &[]).unwrap();
    let mut speer = None;
    let mut step = |server: &mut Manager, client: &mut Manager, now: Time, tick: u32, speer: &mut Option<PeerId>| {
        let mut received = 0;
        server.update(now);
        client.update(now);
        if let Some(sp) = *speer {
            received += traffic(server, sp, tick);
        } else {
            while let Some(e) = server.poll_event() {
                match e {
                    Event::ConnectRequest { peer, .. } => server.accept(now, peer).unwrap(),
                    Event::Connected { peer, .. } => *speer = Some(peer),
                    _ => {}
                }
            }
        }
        if client.peer_state(cpeer) == Some(PeerState::Connected) {
            received += traffic(client, cpeer, tick);
        }
        server.flush(now);
        client.flush(now);
        wire.carry(client, caddr, server, now);
        wire.carry(server, saddr, client, now);
        received
    };
    // Warm up: buffers and queues reach their working size.
    for tick in 0..3000 {
        step(&mut server, &mut client, now, tick, &mut speer);
        now = now + TICK;
    }
    assert!(speer.is_some());
    let (received, allocations) = counted(|| {
        let mut received = 0;
        for tick in 3000..6000 {
            received += step(&mut server, &mut client, now, tick, &mut speer);
            now = now + TICK;
        }
        received
    });
    assert!(received > 3000 * 2 * 500, "traffic flowed: {received} bytes");
    assert_eq!(allocations, 0, "allocations in 3000 steady-state ticks");
}
