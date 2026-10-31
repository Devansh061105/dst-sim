use std::fmt;
use std::ops::{Add, Sub};
use std::time::Duration;

/// A point in virtual time: nanoseconds since the simulation started.
///
/// Deliberately not `std::time::Instant`: virtual time only moves when the simulator advances it,
/// which is what makes runs reproducible and lets an hour of cluster time pass in milliseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Time(u64);

impl Time {
    pub const ZERO: Time = Time(0);

    pub const fn from_nanos(nanos: u64) -> Self {
        Time(nanos)
    }

    pub const fn as_nanos(self) -> u64 {
        self.0
    }
}

impl Add<Duration> for Time {
    type Output = Time;

    fn add(self, d: Duration) -> Time {
        let nanos = u64::try_from(d.as_nanos()).expect("duration too large for virtual time");
        Time(self.0.checked_add(nanos).expect("virtual time overflow"))
    }
}

impl Sub for Time {
    type Output = Duration;

    fn sub(self, earlier: Time) -> Duration {
        let nanos = self.0.checked_sub(earlier.0).expect("subtracted a later Time");
        Duration::from_nanos(nanos)
    }
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{:09}s", self.0 / 1_000_000_000, self.0 % 1_000_000_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arithmetic_and_display() {
        let t = Time::ZERO + Duration::from_millis(1500);
        assert_eq!(t.as_nanos(), 1_500_000_000);
        assert_eq!(t - Time::from_nanos(500_000_000), Duration::from_secs(1));
        assert_eq!(t.to_string(), "1.500000000s");
    }
}
