use std::ops::Range;
use std::time::Duration;

use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{Rng, SeedableRng};
use sim_api::NodeId;

/// Which part of the simulation a random stream belongs to.
///
/// Every subsystem draws from its own stream, so adding a random call in one place (say, the
/// network) doesn't shift the numbers any other subsystem sees. That keeps seeds meaningful as the
/// code grows. The numeric ids are part of the replay format: never renumber existing variants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stream {
    Network,
    Node(NodeId),
}

impl Stream {
    fn id(self) -> u64 {
        match self {
            Stream::Network => 0,
            Stream::Node(node) => (1 << 32) | u64::from(node.0),
        }
    }
}

/// Deterministic random number generator: same seed and stream, same numbers, on every platform.
///
/// ChaCha8 is used because its output is fixed by a published algorithm (unlike std's hashers or
/// OS randomness), and ChaCha supports 2^64 independent streams per seed, which is what
/// [`Stream`] maps onto.
#[derive(Clone, Debug)]
pub struct SimRng(ChaCha8Rng);

impl SimRng {
    pub fn new(seed: u64, stream: Stream) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        rng.set_stream(stream.id());
        SimRng(rng)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0.next_u64()
    }

    /// A duration drawn uniformly from `range` (or `range.start` if the range is empty).
    pub fn duration_in(&mut self, range: Range<Duration>) -> Duration {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn draw(rng: &mut SimRng) -> Vec<u64> {
        (0..8).map(|_| rng.next_u64()).collect()
    }

    #[test]
    fn same_seed_and_stream_repeat() {
        let a = draw(&mut SimRng::new(7, Stream::Network));
        let b = draw(&mut SimRng::new(7, Stream::Network));
        assert_eq!(a, b);
    }

    #[test]
    fn streams_and_seeds_are_independent() {
        let base = draw(&mut SimRng::new(7, Stream::Network));
        assert_ne!(base, draw(&mut SimRng::new(8, Stream::Network)));
        assert_ne!(base, draw(&mut SimRng::new(7, Stream::Node(NodeId(0)))));
        assert_ne!(
            draw(&mut SimRng::new(7, Stream::Node(NodeId(0)))),
            draw(&mut SimRng::new(7, Stream::Node(NodeId(1))))
        );
    }

    #[test]
    fn duration_in_stays_in_range() {
        let mut rng = SimRng::new(1, Stream::Network);
        let range = Duration::from_millis(1)..Duration::from_millis(10);
        for _ in 0..1000 {
            assert!(range.contains(&rng.duration_in(range.clone())));
        }
        let empty = Duration::from_millis(5)..Duration::from_millis(5);
        assert_eq!(rng.duration_in(empty), Duration::from_millis(5));
    }
}
