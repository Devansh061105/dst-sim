//! Two-phase commit under the simulator's fault modes: correct code passes, each planted bug is
//! caught, and runs replay exactly.

use sim_core::SimConfig;
use twopc::{Bugs, NODES, run};

fn chaos(seed: u64) -> SimConfig {
    SimConfig::chaos(seed, NODES)
}

#[test]
fn correct_two_phase_commit_passes_chaos_and_swarm() {
    for seed in 0..300 {
        for config in [chaos(seed), SimConfig::swarm(seed, NODES)] {
            let outcome = run(config, Bugs::default());
            assert_eq!(outcome.failure(), None, "seed {seed}");
        }
    }
}

#[test]
fn fault_free_runs_commit_most_transactions() {
    let outcome = run(SimConfig::new(1, NODES), Bugs::default());
    assert_eq!(outcome.failure(), None);
    assert!(outcome.committed >= 10, "{outcome:?}");
}

#[test]
fn same_seed_same_run() {
    for seed in 0..50 {
        let a = run(chaos(seed), Bugs::default());
        let b = run(chaos(seed), Bugs::default());
        assert_eq!((a.trace_hash, a.events), (b.trace_hash, b.events), "seed {seed}");
    }
}

fn first_failing_seed(bugs: Bugs) -> Option<(u64, &'static str)> {
    (0..2000).find_map(|seed| {
        let outcome = run(chaos(seed), bugs);
        outcome.failure_kind().map(|kind| (seed, kind))
    })
}

#[test]
fn every_planted_bug_is_caught() {
    let bugs = [
        Bugs { commit_on_majority: true, ..Bugs::default() },
        Bugs { vote_without_fsync: true, ..Bugs::default() },
        Bugs { decision_not_synced: true, ..Bugs::default() },
        Bugs { abort_after_yes_timeout: true, ..Bugs::default() },
    ];
    for bug in bugs {
        let caught = first_failing_seed(bug);
        assert!(caught.is_some(), "{bug:?} not caught in 2,000 chaos seeds");
        println!("{bug:?}: first caught at {caught:?}");
    }
}

/// Seeds that found real bugs in this implementation while it was being built (see
/// docs/BUG_MUSEUM.md): a restart during redelivery forgot an older decision (chaos 104), and
/// an unsynced truncate during recovery let a stale commit record come back (swarm 2571, chaos
/// 94858).
#[test]
fn seeds_that_found_real_bugs_pass() {
    for (config, seed) in
        [(chaos(104), 104), (chaos(94858), 94858), (SimConfig::swarm(2571, NODES), 2571)]
    {
        assert_eq!(run(config, Bugs::default()).failure(), None, "seed {seed}");
    }
}
