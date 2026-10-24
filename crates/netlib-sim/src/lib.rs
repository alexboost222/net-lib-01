//! Deterministic network emulator for netlib.
//!
//! Every node is a [`netlib_core::Manager`] attached to a virtual address. Datagrams travel
//! through a [`Link`] that can lose, delay, jitter, duplicate and rate-limit them. All
//! randomness comes from one seed, so a run is fully reproducible.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use netlib_core::{Config, ConfigError, Manager, Time};

/// Conditions applied to every datagram.
#[derive(Debug, Clone, PartialEq)]
pub struct Link {
    /// Probability of dropping a datagram, `0.0..=1.0`.
    pub loss: f64,
    /// One-way base delay.
    pub latency: Duration,
    /// Uniform random extra delay in `0..=jitter`; reorders datagrams when large enough.
    pub jitter: Duration,
    /// Probability of delivering a datagram twice.
    pub duplicate: f64,
    /// Bandwidth limit per direction in bytes per second; `None` is unlimited.
    pub bandwidth: Option<u64>,
}

impl Default for Link {
    fn default() -> Self {
        Link {
            loss: 0.0,
            latency: Duration::from_millis(10),
            jitter: Duration::ZERO,
            duplicate: 0.0,
            bandwidth: None,
        }
    }
}

/// Small deterministic PRNG (SplitMix64).
#[derive(Debug, Clone)]
pub struct SimRng(u64);

impl SimRng {
    /// Creates a generator from a seed.
    pub fn new(seed: u64) -> Self {
        SimRng(seed)
    }

    /// Next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform float in `0.0..1.0`.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// `true` with probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        p > 0.0 && self.next_f64() < p
    }

    /// 32 random bytes.
    pub fn secret(&mut self) -> [u8; 32] {
        let mut s = [0u8; 32];
        for chunk in s.chunks_mut(8) {
            chunk.copy_from_slice(&self.next_u64().to_le_bytes());
        }
        s
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct InFlight {
    deliver_at: Time,
    order: u64,
    from: SocketAddr,
    to: SocketAddr,
    data: Vec<u8>,
}

/// Counters of the virtual network.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NetStats {
    /// Datagrams handed to the network.
    pub sent: u64,
    /// Bytes handed to the network.
    pub sent_bytes: u64,
    /// Datagrams delivered (duplicates included).
    pub delivered: u64,
    /// Datagrams dropped by loss or bandwidth limits.
    pub lost: u64,
    /// Extra copies created.
    pub duplicated: u64,
    /// Datagrams addressed to nobody.
    pub unroutable: u64,
}

/// A virtual host.
pub struct Node {
    /// The address of the node.
    pub addr: SocketAddr,
    /// Its manager.
    pub manager: Manager,
    /// Datagrams sent by this node to a given destination are dropped while this is `true`.
    pub partitioned: bool,
    busy_until: Time,
}

/// The virtual network.
pub struct Sim {
    /// Current virtual time.
    pub now: Time,
    /// Conditions applied to every datagram.
    pub link: Link,
    /// Network counters.
    pub stats: NetStats,
    /// Hosts, indexed by the value returned from [`add_node`](Self::add_node).
    pub nodes: Vec<Node>,
    rng: SimRng,
    in_flight: BinaryHeap<Reverse<InFlight>>,
    order: u64,
    buf: Vec<u8>,
}

impl Sim {
    /// Creates an empty network.
    pub fn new(seed: u64, link: Link) -> Self {
        Sim {
            now: Time::from_millis(1_000),
            link,
            stats: NetStats::default(),
            nodes: Vec::new(),
            rng: SimRng::new(seed),
            in_flight: BinaryHeap::new(),
            order: 0,
            buf: vec![0; netlib_core::MAX_MTU],
        }
    }

    /// Adds a host with address `10.0.0.<n>:9000` and returns its index.
    pub fn add_node(&mut self, config: Config) -> Result<usize, ConfigError> {
        let n = self.nodes.len() + 1;
        let addr = SocketAddr::from((Ipv4Addr::new(10, 0, (n / 250) as u8, (n % 250) as u8 + 1), 9000));
        let manager = Manager::new(config, self.rng.secret(), self.now)?;
        self.nodes.push(Node {
            addr,
            manager,
            partitioned: false,
            busy_until: self.now,
        });
        Ok(self.nodes.len() - 1)
    }

    /// Address of a node.
    pub fn addr(&self, node: usize) -> SocketAddr {
        self.nodes[node].addr
    }

    /// Mutable access to the manager of a node.
    pub fn manager(&mut self, node: usize) -> &mut Manager {
        &mut self.nodes[node].manager
    }

    /// Moves every queued outgoing datagram of every node onto the network.
    pub fn transmit(&mut self) {
        for i in 0..self.nodes.len() {
            while let Some((to, len)) = self.nodes[i].manager.poll_transmit(&mut self.buf) {
                let data = self.buf[..len].to_vec();
                let from = self.nodes[i].addr;
                self.inject(i, from, to, data);
            }
        }
    }

    /// Puts a raw datagram on the network as if `node` sent it from `from`.
    pub fn inject(&mut self, node: usize, from: SocketAddr, to: SocketAddr, data: Vec<u8>) {
        self.stats.sent += 1;
        self.stats.sent_bytes += data.len() as u64;
        if self.nodes[node].partitioned || self.rng.chance(self.link.loss) {
            self.stats.lost += 1;
            return;
        }
        let mut depart = self.now;
        if let Some(bps) = self.link.bandwidth {
            let n = &mut self.nodes[node];
            let start = n.busy_until.max(self.now);
            // Drop when more than one second of data is queued, like a router buffer.
            if start - self.now > Duration::from_secs(1) {
                self.stats.lost += 1;
                return;
            }
            n.busy_until = start + Duration::from_micros(data.len() as u64 * 1_000_000 / bps.max(1));
            depart = n.busy_until;
        }
        let copies = if self.rng.chance(self.link.duplicate) {
            self.stats.duplicated += 1;
            2
        } else {
            1
        };
        for _ in 0..copies {
            let jitter_us = self.link.jitter.as_micros() as u64;
            let extra = if jitter_us > 0 {
                self.rng.next_u64() % (jitter_us + 1)
            } else {
                0
            };
            let deliver_at = depart + self.link.latency + Duration::from_micros(extra);
            self.order += 1;
            self.in_flight.push(Reverse(InFlight {
                deliver_at,
                order: self.order,
                from,
                to,
                data: data.clone(),
            }));
        }
    }

    /// Advances time by `dt`, delivering due datagrams and running `update` on every node.
    pub fn advance(&mut self, dt: Duration) {
        let end = self.now + dt;
        while let Some(Reverse(next)) = self.in_flight.peek() {
            if next.deliver_at > end {
                break;
            }
            let Reverse(d) = self.in_flight.pop().expect("peeked");
            self.now = self.now.max(d.deliver_at);
            match self.nodes.iter_mut().find(|n| n.addr == d.to) {
                Some(node) => {
                    self.stats.delivered += 1;
                    node.manager.handle_datagram(self.now, d.from, &d.data);
                }
                None => self.stats.unroutable += 1,
            }
        }
        self.now = end;
        for node in &mut self.nodes {
            node.manager.update(self.now);
        }
    }

    /// One game tick: deliver, update, let `app` react on each node, flush and transmit.
    pub fn tick(&mut self, dt: Duration, mut app: impl FnMut(usize, &mut Manager, Time)) {
        self.advance(dt);
        for i in 0..self.nodes.len() {
            let now = self.now;
            app(i, &mut self.nodes[i].manager, now);
            self.nodes[i].manager.flush(now);
        }
        self.transmit();
    }

    /// Number of datagrams still travelling.
    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rng_is_deterministic() {
        let mut a = SimRng::new(7);
        let mut b = SimRng::new(7);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
        let f = a.next_f64();
        assert!((0.0..1.0).contains(&f));
    }
}
