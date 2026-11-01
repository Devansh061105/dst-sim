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
        assert_eq!(sim_a.trace().events(), sim_b.trace().events(), "seed {seed}");
    }
}

#[test]
fn different_seeds_different_runs() {
    let hashes: BTreeSet<u64> = (0..100).map(|seed| gossip(seed, 5, false).0.trace_hash).collect();
    assert_eq!(hashes.len(), 100);
}

#[test]
fn same_seed_same_run_under_chaos() {
    for seed in 0..100 {
        let run = || {
            let config = SimConfig { keep_trace_lines: true, ..SimConfig::chaos(seed, 5) };
            let mut sim = Simulation::new(config, |_| Gossip::default());
            let summary = sim.run();
            (summary, sim.trace().events().map(<[_]>::to_vec))
        };
        assert_eq!(run(), run(), "seed {seed}");
    }
}

/// The recorded fault schedule, replayed in place of the random nemesis, gives the very same run:
/// this is what lets a failing run be shrunk by editing its schedule.
#[test]
fn replaying_the_recorded_schedule_reproduces_the_run() {
    for seed in 0..100 {
        let config = SimConfig::chaos(seed, 5);
        let mut original = Simulation::new(config.clone(), |_| Gossip::default());
        let summary = original.run();
        let script = original.script();
        assert!(!script.faults.is_empty(), "seed {seed}: no faults recorded");

        let mut replay_config = config.clone();
        replay_config.faults.script = Some(script.clone());
        let mut replay = Simulation::new(replay_config, |_| Gossip::default());
        assert_eq!(replay.run().trace_hash, summary.trace_hash, "seed {seed}");
        assert_eq!(replay.script(), script, "seed {seed}");

        // Any subset of the schedule is still a valid schedule.
        let mut thinned = config;
        thinned.faults.script = Some(sim_core::Script {
            faults: script.faults.iter().step_by(2).cloned().collect(),
            ..script
        });
        Simulation::new(thinned, |_| Gossip::default()).run();
    }
}

/// Like the golden test below, but through every fault path: float probabilities, shuffles,
/// crashes, partitions and drifting clocks must replay bit-for-bit on every platform too.
#[test]
fn golden_chaos_hash_is_stable_across_platforms() {
    const GOLDEN: u64 = 0x026e_78a5_66d3_5866;
    let mut sim = Simulation::new(SimConfig::chaos(42, 5), |_| Gossip::default());
    let summary = sim.run();
    assert_eq!(
        summary.trace_hash, GOLDEN,
        "seed 42 chaos trace hash changed: {:#018x}",
        summary.trace_hash
    );
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
