//! Milestone M1: the simulator is deterministic, and the seed is what drives each run.

use std::collections::BTreeSet;

use sim_api::NodeId;
use sim_core::toys::{Gossip, PingPong};
use sim_core::{RunSummary, SimConfig, Simulation, StopReason};

fn gossip(seed: u64, nodes: u32, keep_trace_lines: bool) -> (RunSummary, Simulation<Gossip>) {
    let config = SimConfig { keep_trace_lines, ..SimConfig::new(seed, nodes) };
    let mut sim = Simulation::new(config, |_| Gossip::default());
    let summary = sim.run();
    (summary, sim)
}

#[test]
fn ping_pong_completes_all_rounds() {
    let mut sim = Simulation::new(SimConfig::new(1, 2), |_| PingPong::new(100));
    let summary = sim.run();
    assert_eq!(summary.stop, StopReason::Quiescent);
    assert_eq!(sim.node(NodeId(0)).completed(), 100);
    // 2 starts + 100 pings + 100 pongs.
    assert_eq!(summary.steps, 202);
}

#[test]
fn gossip_converges_and_goes_quiet() {
    for seed in 0..200 {
        let (summary, sim) = gossip(seed, 7, false);
        assert_eq!(summary.stop, StopReason::Quiescent, "seed {seed}");
        for (id, node) in sim.nodes() {
            assert_eq!(node.known().len(), 7, "seed {seed}: {id} knows {:?}", node.known());
        }
    }
}

#[test]
fn same_seed_same_run() {
    for seed in 0..100 {
        let (a, sim_a) = gossip(seed, 5, true);
        let (b, sim_b) = gossip(seed, 5, true);
        assert_eq!(a, b, "seed {seed}");
        assert_eq!(sim_a.trace().lines(), sim_b.trace().lines(), "seed {seed}");
    }
}

#[test]
fn different_seeds_different_runs() {
    let hashes: BTreeSet<u64> = (0..100).map(|seed| gossip(seed, 5, false).0.trace_hash).collect();
    assert_eq!(hashes.len(), 100);
}

/// Pins the exact run for one seed. CI runs this on Linux and Windows, so passing there means a
/// seed replays bit-for-bit across operating systems. If you change simulator or gossip behaviour
/// on purpose, update the constant (the failure message prints the new value).
#[test]
fn golden_hash_is_stable_across_platforms() {
    const GOLDEN: u64 = 0x1551_fea9_48af_1f1a;
    let (summary, _) = gossip(42, 5, false);
    assert_eq!(
        summary.trace_hash, GOLDEN,
        "seed 42 trace hash changed: {:#018x}",
        summary.trace_hash
    );
}
