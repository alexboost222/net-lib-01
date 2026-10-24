//! Round-trip time estimation: RFC 6298 smoothing with ack delay handling from RFC 9002 (QUIC).
//!
//! Two estimates are kept:
//! - the *network* RTT excludes the time the remote side held the ack before sending it;
//!   this is what the application sees;
//! - the *ack* RTT is the raw time until an ack arrives; retransmission timers use it,
//!   because that is how long the sender actually waits.

use core::time::Duration;

#[derive(Debug, Clone, Default)]
struct Smoothed {
    srtt: Option<Duration>,
    rttvar: Duration,
}

impl Smoothed {
    fn sample(&mut self, r: Duration) {
        match self.srtt {
            None => {
                self.srtt = Some(r);
                self.rttvar = r / 2;
            }
            Some(srtt) => {
                self.rttvar = (self.rttvar * 3 + srtt.abs_diff(r)) / 4;
                self.srtt = Some((srtt * 7 + r) / 8);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RttEstimator {
    network: Smoothed,
    ack: Smoothed,
    min_rtt: Option<Duration>,
    latest: Duration,
    min_rto: Duration,
    max_rto: Duration,
}

/// RTO before the first sample.
const INITIAL_RTO: Duration = Duration::from_secs(1);
/// Clock granularity term from RFC 6298.
const GRANULARITY: Duration = Duration::from_millis(1);

impl RttEstimator {
    pub(crate) fn new(min_rto: Duration, max_rto: Duration) -> Self {
        RttEstimator {
            network: Smoothed::default(),
            ack: Smoothed::default(),
            min_rtt: None,
            latest: Duration::ZERO,
            min_rto,
            max_rto,
        }
    }

    /// Takes a sample: `latest_rtt` is the time from sending a packet until its ack arrived,
    /// `ack_delay` is how long the remote side reports holding that ack.
    ///
    /// Protection against bogus reports, as in RFC 9002 section 5.3:
    /// - `ack_delay` is capped at `max_ack_delay`;
    /// - the adjusted sample never goes below `min_rtt`, the smallest raw sample seen.
    ///
    /// RFC 9002 keeps the raw sample when subtracting would cross `min_rtt`; we clamp to
    /// `min_rtt` instead. The guarantee is the same (a peer cannot push the estimate below
    /// `min_rtt`), but sub-millisecond rounding of the reported delay does not throw away
    /// the whole adjustment.
    pub(crate) fn on_ack(&mut self, latest_rtt: Duration, ack_delay: Duration, max_ack_delay: Duration) {
        self.ack.sample(latest_rtt);
        let min_rtt = self.min_rtt.map_or(latest_rtt, |m| m.min(latest_rtt));
        self.min_rtt = Some(min_rtt);
        let adjusted = latest_rtt.saturating_sub(ack_delay.min(max_ack_delay)).max(min_rtt);
        self.latest = adjusted;
        self.network.sample(adjusted);
    }

    /// Smoothed network RTT, if at least one sample was taken.
    pub(crate) fn srtt(&self) -> Option<Duration> {
        self.network.srtt
    }

    /// Smallest raw sample.
    pub(crate) fn min_rtt(&self) -> Option<Duration> {
        self.min_rtt
    }

    /// Latest adjusted sample.
    pub(crate) fn latest(&self) -> Duration {
        self.latest
    }

    /// Mean deviation of the network RTT.
    pub(crate) fn rttvar(&self) -> Duration {
        self.network.rttvar
    }

    /// Retransmission timeout, computed from the raw time until acks arrive.
    pub(crate) fn rto(&self) -> Duration {
        match self.ack.srtt {
            None => INITIAL_RTO.clamp(self.min_rto, self.max_rto),
            Some(srtt) => (srtt + (self.ack.rttvar * 4).max(GRANULARITY)).clamp(self.min_rto, self.max_rto),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    fn estimator() -> RttEstimator {
        RttEstimator::new(MS * 10, Duration::from_secs(2))
    }

    #[test]
    fn rfc6298_smoothing() {
        let mut e = estimator();
        assert_eq!(e.rto(), Duration::from_secs(1));
        e.on_ack(MS * 100, Duration::ZERO, MS * 100);
        assert_eq!(e.srtt(), Some(MS * 100));
        assert_eq!(e.rttvar(), MS * 50);
        assert_eq!(e.rto(), MS * 300);
        for _ in 0..100 {
            e.on_ack(MS * 40, Duration::ZERO, MS * 100);
        }
        assert!(e.srtt().unwrap().abs_diff(MS * 40) < MS);
        assert!(e.rto() >= MS * 10 && e.rto() < MS * 50);
    }

    #[test]
    fn ack_delay_is_subtracted_from_network_rtt_only() {
        let mut e = estimator();
        e.on_ack(MS * 100, Duration::ZERO, MS * 100);
        for _ in 0..200 {
            e.on_ack(MS * 130, MS * 30, MS * 100);
        }
        assert!(e.srtt().unwrap().abs_diff(MS * 100) < MS, "{:?}", e.srtt());
        assert_eq!(e.latest(), MS * 100);
        // The retransmission timer waits for the real ack arrival time.
        assert!(e.rto() >= MS * 130, "{:?}", e.rto());
    }

    #[test]
    fn bogus_ack_delay_cannot_push_rtt_below_min_rtt() {
        let mut e = estimator();
        e.on_ack(MS * 100, Duration::ZERO, MS * 100);
        // The peer claims it held every ack for 90 ms: subtraction would give 10..30 ms.
        for i in 0..200u32 {
            e.on_ack(MS * (100 + i % 20), MS * 90, MS * 100);
        }
        assert_eq!(e.min_rtt(), Some(MS * 100));
        assert!(e.srtt().unwrap() >= MS * 100);
        assert_eq!(e.latest(), MS * 100);
    }

    #[test]
    fn ack_delay_is_capped_at_max_ack_delay() {
        let mut e = estimator();
        e.on_ack(MS * 50, Duration::ZERO, MS * 25);
        e.on_ack(MS * 200, MS * 150, MS * 25);
        // Only 25 ms of the claimed 150 ms are believed.
        assert_eq!(e.latest(), MS * 175);
    }
}
