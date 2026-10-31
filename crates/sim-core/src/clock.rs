use std::time::Duration;

use sim_api::{Rand, Time};

/// How wrong node clocks may be.
#[derive(Clone, Debug, Default)]
pub struct ClockConfig {
    /// Each node's clock starts ahead of real time by a random amount up to this.
    pub max_offset: Duration,
    /// Each node's clock runs fast or slow by up to this many parts per million (10_000 = 1%).
    pub max_drift_ppm: u32,
}

const PPM: i128 = 1_000_000;

/// A node's local clock: `local = offset + real × (1 + drift)`.
///
/// Real hardware drifts by tens of ppm; chaos runs use percent-level drift to stress anything
/// that relies on timeouts or leases. Timers are set in local time, so a fast clock fires its
/// timers early in real time. Integer math keeps it exact: a perfect clock is the identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Clock {
    offset_nanos: u64,
    drift_ppm: i64,
}

impl Clock {
    pub(crate) fn random(config: &ClockConfig, rand: &mut impl Rand) -> Self {
        assert!(config.max_drift_ppm < 500_000, "clock drift must stay below 50%");
        let max = i64::from(config.max_drift_ppm);
        let drift_ppm = if max == 0 { 0 } else { rand.below(2 * max as u64 + 1) as i64 - max };
        let offset = rand.duration_in(Duration::ZERO..config.max_offset);
        Clock { offset_nanos: offset.as_nanos() as u64, drift_ppm }
    }

    /// What this node's clock reads at real time `real`.
    pub(crate) fn local(&self, real: Time) -> Time {
        let scaled = i128::from(real.as_nanos()) * (PPM + i128::from(self.drift_ppm)) / PPM;
        Time::from_nanos(self.offset_nanos + scaled as u64)
    }

    /// How much real time passes while this clock advances by `local`.
    pub(crate) fn real_delay(&self, local: Duration) -> Duration {
        let nanos = local.as_nanos() as i128 * PPM / (PPM + i128::from(self.drift_ppm));
        Duration::from_nanos(nanos as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: Duration = Duration::from_secs(1);

    #[test]
    fn perfect_clock_is_identity() {
        let clock = Clock { offset_nanos: 0, drift_ppm: 0 };
        let t = Time::from_nanos(123_456_789);
        assert_eq!(clock.local(t), t);
        assert_eq!(
            clock.real_delay(Duration::from_nanos(987_654_321)),
            Duration::from_nanos(987_654_321)
        );
    }

    #[test]
    fn fast_clock_reads_ahead_and_fires_timers_early() {
        let fast = Clock { offset_nanos: 0, drift_ppm: 100_000 }; // 10% fast
        assert_eq!(fast.local(Time::ZERO + SEC), Time::ZERO + Duration::from_millis(1100));
        assert_eq!(fast.real_delay(Duration::from_millis(1100)), SEC);
    }

    #[test]
    fn slow_clock_and_offset() {
        let slow = Clock { offset_nanos: 500_000_000, drift_ppm: -100_000 }; // 10% slow, +0.5 s
        assert_eq!(slow.local(Time::ZERO + SEC), Time::ZERO + Duration::from_millis(1400));
        assert_eq!(slow.real_delay(Duration::from_millis(900)), SEC);
    }

    #[test]
    fn random_clocks_stay_within_config() {
        let config = ClockConfig { max_offset: SEC, max_drift_ppm: 50_000 };
        let mut rng = crate::rng::SimRng::new(9, crate::rng::Stream::Clocks);
        for _ in 0..1000 {
            let c = Clock::random(&config, &mut rng);
            assert!(c.offset_nanos < 1_000_000_000);
            assert!((-50_000..=50_000).contains(&c.drift_ppm));
        }
    }
}
