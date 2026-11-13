//! Simulated network with fault injection: latency, drop, duplicate, reorder and partitions,
//! optionally with nodes in regions whose links have their own latency.
//!
//! [`Network`] is a pure model: given a message's endpoints and a source of randomness, it decides
//! the message's fate as a [`Route`]. It holds no clock and no queue; the simulator turns a route
//! into delivery events. Being pure, it can be unit-tested without running a simulation.
//!
//! The model is deliberately harsher than TCP: messages can be lost, duplicated and reordered,
//! which is exactly the network the Raft paper assumes.

use std::collections::BTreeSet;
use std::ops::Range;
use std::time::Duration;

use sim_api::{NodeId, Rand};

/// How unreliable the network is. Probabilities are per message.
#[derive(Clone, Debug)]
pub struct NetConfig {
    /// Normal one-way delay, drawn uniformly. Messages sent close together with different delays
    /// already arrive out of order.
    pub latency: Range<Duration>,
    /// Probability that a message is lost.
    pub drop: f64,
    /// Probability that a message is delivered twice, each copy with its own delay.
    pub duplicate: f64,
    /// Probability that a message hits a delay spike (congestion, a stalled machine), which
    /// reorders it far behind messages sent after it.
    pub spike: f64,
    /// Delay of a message that hits a spike.
    pub spike_latency: Range<Duration>,
    /// Nodes placed in regions, each pair of regions with its own normal delay instead of
    /// `latency` (off by default: one latency for every link).
    pub regions: Option<Regions>,
}

/// Where nodes are and how far apart: a latency matrix by region.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Regions {
    /// The region of each node, indexed by node id. Nodes past the end are in region 0.
    pub of: Vec<usize>,
    /// The normal one-way delay from region `a` to region `b` is drawn from `latency[a][b]`
    /// (`latency[a][a]`: within region `a`).
    pub latency: Vec<Vec<Range<Duration>>>,
}

impl Regions {
    /// The region of `node`.
    pub fn region(&self, node: NodeId) -> usize {
        self.of.get(node.0 as usize).copied().unwrap_or(0)
    }

    /// The normal delay range of the link from `from` to `to`.
    pub fn latency(&self, from: NodeId, to: NodeId) -> Range<Duration> {
        self.latency[self.region(from)][self.region(to)].clone()
    }
}

impl Default for NetConfig {
    /// A reliable network: every message arrives once, 1–10 ms later.
    fn default() -> Self {
        NetConfig {
            latency: Duration::from_millis(1)..Duration::from_millis(10),
            drop: 0.0,
            duplicate: 0.0,
            spike: 0.0,
            spike_latency: Duration::from_millis(100)..Duration::from_secs(1),
            regions: None,
        }
    }
}

/// What happens to one message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    Deliver(Duration),
    Duplicate(Duration, Duration),
    Lost,
    Partitioned,
}

/// The network's fault model: decides what happens to each message when it is sent.
#[derive(Clone, Debug)]
pub struct Network {
    config: NetConfig,
    /// Directed links that currently lose every message.
    blocked: BTreeSet<(NodeId, NodeId)>,
}

impl Network {
    pub fn new(config: NetConfig) -> Self {
        for p in [config.drop, config.duplicate, config.spike] {
            assert!((0.0..=1.0).contains(&p), "probability {p} is outside [0, 1]");
        }
        if let Some(regions) = &config.regions {
            let n = regions.latency.len();
            assert!(
                regions.latency.iter().all(|row| row.len() == n),
                "latency matrix is not square"
            );
            assert!(regions.of.iter().all(|&r| r < n), "a node is in a region the matrix lacks");
        }
        Network { config, blocked: BTreeSet::new() }
    }

    pub fn config(&self) -> &NetConfig {
        &self.config
    }

    /// Decides the fate of a message from `from` to `to`.
    ///
    /// The order of random draws is part of the replay format. Zero probabilities draw nothing,
    /// so a fault-free network consumes exactly one number (the delay) per message.
    pub fn route(&self, from: NodeId, to: NodeId, rand: &mut impl Rand) -> Route {
        if self.is_blocked(from, to) {
            return Route::Partitioned;
        }
        if rand.chance(self.config.drop) {
            return Route::Lost;
        }
        let first = self.delay(from, to, rand);
        if rand.chance(self.config.duplicate) {
            Route::Duplicate(first, self.delay(from, to, rand))
        } else {
            Route::Deliver(first)
        }
    }

    fn delay(&self, from: NodeId, to: NodeId, rand: &mut impl Rand) -> Duration {
        if rand.chance(self.config.spike) {
            rand.duration_in(self.config.spike_latency.clone())
        } else if let Some(regions) = &self.config.regions {
            rand.duration_in(regions.latency(from, to))
        } else {
            rand.duration_in(self.config.latency.clone())
        }
    }

    /// Cuts the link from `from` to `to` only (an asymmetric partition).
    pub fn block(&mut self, from: NodeId, to: NodeId) {
        if from != to {
            self.blocked.insert((from, to));
        }
    }

    /// Cuts every link, in both directions, between the nodes in `side` and the rest of `all`.
    pub fn split(&mut self, side: &[NodeId], all: &[NodeId]) {
        for &inside in side {
            for &outside in all.iter().filter(|n| !side.contains(n)) {
                self.block(inside, outside);
                self.block(outside, inside);
            }
        }
    }

    /// Cuts `node` off from everyone else.
    pub fn isolate(&mut self, node: NodeId, all: &[NodeId]) {
        self.split(&[node], all);
    }

    /// Removes every partition.
    pub fn heal(&mut self) {
        self.blocked.clear();
    }

    pub fn is_blocked(&self, from: NodeId, to: NodeId) -> bool {
        self.blocked.contains(&(from, to))
    }

    pub fn is_healed(&self) -> bool {
        self.blocked.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small deterministic generator, so these tests don't depend on `sim-core`.
    struct Lcg(u64);

    impl Rand for Lcg {
        fn next_u64(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0
        }
    }

    const A: NodeId = NodeId(0);
    const B: NodeId = NodeId(1);
    const C: NodeId = NodeId(2);
    const ALL: [NodeId; 3] = [A, B, C];

    #[test]
    fn reliable_network_delivers_within_latency() {
        let net = Network::new(NetConfig::default());
        let mut rand = Lcg(1);
        for _ in 0..1000 {
            match net.route(A, B, &mut rand) {
                Route::Deliver(d) => assert!(net.config().latency.contains(&d)),
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn certain_loss_and_duplication() {
        let lossy = Network::new(NetConfig { drop: 1.0, ..NetConfig::default() });
        assert_eq!(lossy.route(A, B, &mut Lcg(1)), Route::Lost);

        let dup = Network::new(NetConfig { duplicate: 1.0, ..NetConfig::default() });
        assert!(matches!(dup.route(A, B, &mut Lcg(1)), Route::Duplicate(..)));
    }

    #[test]
    fn regions_set_each_links_latency() {
        let ms = Duration::from_millis;
        let regions = Regions {
            of: vec![0, 0, 1],
            latency: vec![vec![ms(1)..ms(2), ms(40)..ms(50)], vec![ms(40)..ms(50), ms(1)..ms(2)]],
        };
        let net = Network::new(NetConfig { regions: Some(regions), ..NetConfig::default() });
        let mut rand = Lcg(9);
        for _ in 0..100 {
            let near = net.route(NodeId(0), NodeId(1), &mut rand);
            let far = net.route(NodeId(1), NodeId(2), &mut rand);
            // A node past the end of `of` is in region 0.
            let unplaced = net.route(NodeId(7), NodeId(0), &mut rand);
            assert!(matches!(near, Route::Deliver(d) if d < ms(2)), "{near:?}");
            assert!(matches!(far, Route::Deliver(d) if d >= ms(40)), "{far:?}");
            assert!(matches!(unplaced, Route::Deliver(d) if d < ms(2)), "{unplaced:?}");
        }
    }

    #[test]
    fn spikes_use_spike_latency() {
        let net = Network::new(NetConfig { spike: 1.0, ..NetConfig::default() });
        let Route::Deliver(d) = net.route(A, B, &mut Lcg(1)) else { panic!("expected delivery") };
        assert!(net.config().spike_latency.contains(&d));
    }

    #[test]
    fn split_is_symmetric_and_heals() {
        let mut net = Network::new(NetConfig::default());
        net.split(&[A], &ALL);
        assert_eq!(net.route(A, B, &mut Lcg(1)), Route::Partitioned);
        assert_eq!(net.route(C, A, &mut Lcg(1)), Route::Partitioned);
        assert!(matches!(net.route(B, C, &mut Lcg(1)), Route::Deliver(_)));

        net.heal();
        assert!(net.is_healed());
        assert!(matches!(net.route(A, B, &mut Lcg(1)), Route::Deliver(_)));
    }

    #[test]
    fn one_way_block_is_asymmetric() {
        let mut net = Network::new(NetConfig::default());
        net.block(A, B);
        assert_eq!(net.route(A, B, &mut Lcg(1)), Route::Partitioned);
        assert!(matches!(net.route(B, A, &mut Lcg(1)), Route::Deliver(_)));
    }

    #[test]
    fn isolate_cuts_every_link_of_one_node() {
        let mut net = Network::new(NetConfig::default());
        net.isolate(C, &ALL);
        for other in [A, B] {
            assert!(net.is_blocked(C, other) && net.is_blocked(other, C));
        }
        assert!(!net.is_blocked(A, B));
    }
}
