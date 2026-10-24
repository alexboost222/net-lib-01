//! Round-trip time estimation (RFC 6298).

use core::time::Duration;

#[derive(Debug, Clone)]
pub(crate) struct RttEstimator {
    srtt: Option<Duration>,
    rttvar: Duration,
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
            srtt: None,
            rttvar: Duration::ZERO,
            latest: Duration::ZERO,
            min_rto,
            max_rto,
        }
    }

    pub(crate) fn sample(&mut self, r: Duration) {
        self.latest = r;
        match self.srtt {
            None => {
                self.srtt = Some(r);
                self.rttvar = r / 2;
            }
            Some(srtt) => {
                let diff = srtt.abs_diff(r);
                self.rttvar = (self.rttvar * 3 + diff) / 4;
                self.srtt = Some((srtt * 7 + r) / 8);
            }
        }
    }

    /// Smoothed RTT, if at least one sample was taken.
    pub(crate) fn srtt(&self) -> Option<Duration> {
        self.srtt
    }

    #[cfg(test)]
    pub(crate) fn rttvar(&self) -> Duration {
        self.rttvar
    }

    pub(crate) fn latest(&self) -> Duration {
        self.latest
    }

    pub(crate) fn rto(&self) -> Duration {
        match self.srtt {
            None => INITIAL_RTO.clamp(self.min_rto, self.max_rto),
            Some(srtt) => (srtt + (self.rttvar * 4).max(GRANULARITY)).clamp(self.min_rto, self.max_rto),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc6298_smoothing() {
        let mut e = RttEstimator::new(Duration::from_millis(10), Duration::from_secs(2));
        assert_eq!(e.rto(), Duration::from_secs(1));
        e.sample(Duration::from_millis(100));
        assert_eq!(e.srtt(), Some(Duration::from_millis(100)));
        assert_eq!(e.rttvar(), Duration::from_millis(50));
        assert_eq!(e.rto(), Duration::from_millis(300));
        for _ in 0..100 {
            e.sample(Duration::from_millis(40));
        }
        let srtt = e.srtt().unwrap();
        assert!(srtt.abs_diff(Duration::from_millis(40)) < Duration::from_millis(1));
        assert_eq!(e.rto(), Duration::from_millis(10).max(e.rto()));
        assert!(e.rto() < Duration::from_millis(50));
    }
}
