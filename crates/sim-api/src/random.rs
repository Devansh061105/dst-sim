use std::ops::Range;
use std::time::Duration;

/// Deterministic randomness for the simulator's fault models (network, nemesis, clocks, disk).
///
/// It lives here rather than in `sim-core` so the model crates, which `sim-core` depends on, can
/// use it without a dependency cycle. Nodes don't use it: they get randomness through
/// [`Context`](crate::Context).
pub trait Rand {
    fn next_u64(&mut self) -> u64;

    /// A number in `0..n`. Modulo bias is negligible for the small `n` used here.
    fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below(0) has no valid result");
        self.next_u64() % n
    }

    /// `true` with probability `p`. A probability of exactly 0 or 1 draws nothing, so switching a
    /// fault off doesn't shift any other random choice in the run.
    fn chance(&mut self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        // 53 random bits scaled into [0, 1). Exact in IEEE 754 arithmetic, so the same on every CPU.
        let unit = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        unit < p
    }

    /// A duration drawn uniformly from `range`, or `range.start` (drawing nothing) if it is empty.
    fn duration_in(&mut self, range: Range<Duration>) -> Duration {
        let lo = nanos(range.start);
        let hi = nanos(range.end);
        if hi <= lo {
            return range.start;
        }
        Duration::from_nanos(lo + self.next_u64() % (hi - lo))
    }
}

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).expect("duration does not fit in u64 nanoseconds")
}
