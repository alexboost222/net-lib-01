use core::ops::{Add, Sub};
use core::time::Duration;

/// Monotonic timestamp supplied by the caller, in microseconds since an arbitrary epoch.
///
/// The core never reads a clock itself: every call that depends on time takes a `Time`.
/// This keeps the state machine deterministic and lets tests run on virtual time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Time(u64);

impl Time {
    /// The epoch.
    pub const ZERO: Time = Time(0);

    /// Creates a timestamp from microseconds since the epoch.
    pub const fn from_micros(us: u64) -> Self {
        Time(us)
    }

    /// Creates a timestamp from milliseconds since the epoch.
    pub const fn from_millis(ms: u64) -> Self {
        Time(ms.saturating_mul(1000))
    }

    /// Microseconds since the epoch.
    pub const fn as_micros(self) -> u64 {
        self.0
    }

    /// Time elapsed since `earlier`, or zero if `earlier` is in the future.
    pub fn since(self, earlier: Time) -> Duration {
        Duration::from_micros(self.0.saturating_sub(earlier.0))
    }
}

impl Add<Duration> for Time {
    type Output = Time;

    fn add(self, rhs: Duration) -> Time {
        let us = u64::try_from(rhs.as_micros()).unwrap_or(u64::MAX);
        Time(self.0.saturating_add(us))
    }
}

impl Sub<Time> for Time {
    type Output = Duration;

    fn sub(self, rhs: Time) -> Duration {
        self.since(rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arithmetic_saturates() {
        let t = Time::from_millis(5);
        assert_eq!(t + Duration::from_millis(3), Time::from_millis(8));
        assert_eq!(Time::from_millis(8) - t, Duration::from_millis(3));
        assert_eq!(t - Time::from_millis(8), Duration::ZERO);
        assert_eq!(
            Time::from_micros(u64::MAX) + Duration::from_secs(1),
            Time::from_micros(u64::MAX)
        );
    }
}
