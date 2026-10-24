//! L4: Debug logging costs little.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use netlib_core::log::{Level, LogSink, Record};
use netlib_core::{ChannelConfig, Config, DeliveryMode, Event};
use netlib_sim::{Link, Sim};

struct Counter(Arc<AtomicU64>, Level);

impl LogSink for Counter {
    fn enabled(&self, level: Level) -> bool {
        level >= self.1
    }
    fn log(&mut self, r: &Record<'_>) {
        self.0.fetch_add(r.properties.len() as u64, Ordering::Relaxed);
    }
}

/// 16 clients on a lossy link for 5 s; returns the wall time of the run.
fn run(log: Option<Level>, counter: &Arc<AtomicU64>) -> Duration {
    let config = |server| Config {
        protocol_id: 2,
        accept_incoming: server,
        channels: vec![
            ChannelConfig::new(DeliveryMode::UnreliableSequenced),
            ChannelConfig::new(DeliveryMode::ReliableOrdered),
        ],
        ..Config::default()
    };
    let mut sim = Sim::new(
        5,
        Link {
            loss: 0.1,
            latency: Duration::from_millis(30),
            ..Link::default()
        },
    );
    sim.add_node(config(true)).unwrap();
    for _ in 0..16 {
        sim.add_node(config(false)).unwrap();
    }
    if let Some(level) = log {
        for i in 0..sim.nodes.len() {
            sim.manager(i)
                .set_log_sink(Some(Box::new(Counter(counter.clone(), level))));
        }
    }
    let addr = sim.addr(0);
    for i in 1..sim.nodes.len() {
        let now = sim.now;
        sim.manager(i).connect(now, addr, &[]).unwrap();
    }
    sim.transmit();
    let start = Instant::now();
    for _ in 0..64 * 5 {
        sim.tick(Duration::from_micros(15_625), |_, m, now| {
            while let Some(e) = m.poll_event() {
                if let Event::ConnectRequest { peer, .. } = e {
                    m.accept(now, peer).unwrap();
                }
            }
            let peers: Vec<_> = m.peers().collect();
            for p in peers {
                let _ = m.send(p, 0, &[0; 40]);
                let _ = m.send(p, 1, &[0; 16]);
            }
        });
    }
    start.elapsed()
}

#[test]
fn l4_debug_logging_overhead_is_small() {
    let counter = Arc::new(AtomicU64::new(0));
    let best = |log| (0..3).map(|_| run(log, &counter)).min().unwrap();
    let off = best(None);
    let before = counter.load(Ordering::Relaxed);
    let debug = best(Some(Level::Debug));
    let events = counter.load(Ordering::Relaxed) - before;
    assert!(events > 1000, "debug events were produced: {events}");
    let ratio = debug.as_secs_f64() / off.as_secs_f64();
    eprintln!("debug logging: {ratio:.3}x ({off:?} -> {debug:?})");
    assert!(ratio < 1.10, "debug logging costs {:.1}%", (ratio - 1.0) * 100.0);
}
