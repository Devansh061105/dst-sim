//! The Raft KV store stays safe under every fault, recovers to serve all clients, and what clients
//! observe is linearizable.

use harness::{Bug, Mode, NODES, OPS, SERVERS, run_cluster, run_seed};
use kv::Bugs;
use sim_core::{SimConfig, Stats};

fn sweep(mode: Mode, seeds: std::ops::Range<u64>) -> Stats {
    let mut total = Stats::default();
    for seed in seeds {
        let outcome = run_seed(mode, seed, None, false);
        outcome.assert_healthy();
        total += outcome.stats;
    }
    total
}

#[test]
fn fault_free_cluster_serves_every_client() {
    for seed in 0..20 {
        let outcome = run_seed(Mode::Calm, seed, None, false);
        outcome.assert_healthy();
        assert!(outcome.committed >= (2 * OPS) as usize, "seed {seed}: {}", outcome.committed);
    }
}

#[test]
fn single_server_cluster_works() {
    run_cluster(SimConfig::new(1, 2), 1, 20, Bugs::default()).assert_healthy();
}

#[test]
fn survives_chaos() {
    let total = sweep(Mode::Chaos, 0..200);
    assert!(total.crashes > 0 && total.partitions > 0 && total.lost > 0, "{total:?}");
    assert!(total.sync_failures > 0 && total.halts > 0, "{total:?}");
    // Crashes in the middle of an fsync are what exercise torn tails in Raft's recovery.
    assert!(
        total.sync_crashes > 0 && total.torn_writes > 0 && total.unsynced_survived > 0,
        "{total:?}"
    );
}

#[test]
fn survives_swarm() {
    sweep(Mode::Swarm, 0..200);
}

#[test]
fn swarm_mixes_differ_per_seed() {
    let mixes: std::collections::BTreeSet<String> = (0..50)
        .map(|seed| {
            let c = SimConfig::swarm(seed, NODES);
            format!("{:?} {:?} {:?} {:?} {}", c.net, c.faults, c.clocks, c.disk, c.buggify)
        })
        .collect();
    assert!(mixes.len() > 40, "only {} distinct fault mixes in 50 seeds", mixes.len());
}

/// Slow in debug builds:
/// `cargo test --release -p harness --test cluster -- --ignored`
#[test]
#[ignore = "10,000 seeds; run with --release -- --ignored"]
fn survives_chaos_10k_seeds() {
    println!("{:?}", sweep(Mode::Chaos, 0..10_000));
}

#[test]
#[ignore = "10,000 seeds; run with --release -- --ignored"]
fn survives_swarm_10k_seeds() {
    println!("{:?}", sweep(Mode::Swarm, 0..10_000));
}

#[test]
fn same_seed_same_run() {
    for mode in [Mode::Chaos, Mode::Swarm] {
        for seed in 0..20 {
            let a = run_seed(mode, seed, None, false);
            let b = run_seed(mode, seed, None, false);
            assert_eq!((a.trace_hash, a.stats), (b.trace_hash, b.stats), "{mode} seed {seed}");
        }
    }
}

/// A store that answers reads from a replica's own copy is caught by the linearizability checker,
/// while Raft's own invariants stay green (the log is fine; the reads are not).
#[test]
fn stale_local_reads_are_caught() {
    let caught = (0..1000).find(|&seed| {
        let outcome = run_seed(Mode::Chaos, seed, Some(Bug::LocalReads), false);
        assert!(outcome.violation.is_none(), "seed {seed}: Raft itself should stay safe");
        matches!(outcome.verdict, checker::Verdict::NotLinearizable { .. })
    });
    assert!(caught.is_some(), "stale local reads went undetected for 1,000 seeds");
}

#[test]
fn histories_are_recorded_and_complete_without_faults() {
    let outcome = run_seed(Mode::Calm, 3, None, false);
    outcome.assert_healthy();
    assert_eq!(outcome.operations, (2 * OPS) as usize, "every invocation is in the history");
    assert_eq!(SERVERS + 2, NODES);
}

/// Regression: this swarm seed once failed on correct code because the checker demanded that a
/// stale leader of an older term hold entries committed in a newer term, which Raft doesn't
/// promise. The checker was fixed, not Raft.
#[test]
fn stale_leader_of_older_term_is_not_a_violation() {
    run_seed(Mode::Swarm, 11644, None, false).assert_healthy();
}

fn caught_within(bug: Bug, mode: Mode, seeds: u64) -> Option<u64> {
    (0..seeds).find(|&seed| !run_seed(mode, seed, Some(bug), false).passed())
}

/// Planted bugs the random search finds within a few seeds; each must keep being caught.
#[test]
fn quickly_found_planted_bugs_are_caught() {
    for bug in [
        Bug::TruncateWithoutConflict,
        Bug::SkipAppendFsync,
        Bug::VoteWithoutLogCheck,
        Bug::LocalReads,
    ] {
        assert!(caught_within(bug, Mode::Chaos, 100).is_some(), "{bug} not caught in 100 seeds");
    }
}

/// Reachable only when an AppendEntries carries fewer entries than the follower has, which in
/// practice needs BUGGIFY's one-entry batches: a few seeds in a thousand.
#[test]
#[ignore = "thousands of seeds; run with --release -- --ignored"]
fn commit_beyond_match_is_caught_by_swarm() {
    assert!(caught_within(Bug::CommitBeyondMatch, Mode::Swarm, 5_000).is_some());
}
