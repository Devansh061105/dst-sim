//! Milestone M4: the Raft KV store stays safe under every fault, and recovers to serve all clients.

mod common;

use common::run;
use sim_core::{SimConfig, Stats};

const SERVERS: u32 = 5;
const NODES: u32 = 7; // 5 servers + 2 clients
const OPS: u32 = 30;

#[test]
fn fault_free_cluster_serves_every_client() {
    for seed in 0..20 {
        let outcome = run(SimConfig::new(seed, NODES), SERVERS, OPS);
        outcome.assert_healthy();
        assert!(outcome.committed >= (2 * OPS) as usize, "seed {seed}: {}", outcome.committed);
    }
}

#[test]
fn single_server_cluster_works() {
    run(SimConfig::new(1, 2), 1, 20).assert_healthy();
}

fn chaos_sweep(seeds: std::ops::Range<u64>) -> Stats {
    let mut total = Stats::default();
    for seed in seeds {
        let outcome = run(SimConfig::chaos(seed, NODES), SERVERS, OPS);
        outcome.assert_healthy();
        let s = outcome.stats;
        total.crashes += s.crashes;
        total.halts += s.halts;
        total.partitions += s.partitions;
        total.lost += s.lost;
        total.sync_failures += s.sync_failures;
        total.torn_writes += s.torn_writes;
        total.unsynced_survived += s.unsynced_survived;
        total.sync_crashes += s.sync_crashes;
    }
    total
}

#[test]
fn survives_chaos() {
    let total = chaos_sweep(0..200);
    assert!(total.crashes > 0 && total.partitions > 0 && total.lost > 0, "{total:?}");
    assert!(total.sync_failures > 0 && total.halts > 0, "{total:?}");
    // Crashes in the middle of an fsync are what exercise torn tails in Raft's recovery.
    assert!(
        total.sync_crashes > 0 && total.torn_writes > 0 && total.unsynced_survived > 0,
        "{total:?}"
    );
}

/// The M4 "done when". Slow in debug builds:
/// `cargo test --release -p kv --test cluster -- --ignored`
#[test]
#[ignore = "10,000 seeds; run with --release -- --ignored"]
fn survives_chaos_10k_seeds() {
    let total = chaos_sweep(0..10_000);
    println!("{total:?}");
}

#[test]
fn same_seed_same_run() {
    for seed in 0..20 {
        let a = run(SimConfig::chaos(seed, NODES), SERVERS, OPS);
        let b = run(SimConfig::chaos(seed, NODES), SERVERS, OPS);
        assert_eq!((a.trace_hash, a.stats), (b.trace_hash, b.stats), "seed {seed}");
    }
}

/// The M5 "done when": a store that answers reads from a replica's own copy is caught by the
/// linearizability checker, while Raft's own invariants stay green (the log is fine; the reads
/// are not).
#[test]
fn stale_local_reads_are_caught() {
    let bugs = kv::Bugs { local_reads: true };
    let caught = (0..1000).find(|&seed| {
        let outcome = common::run_with_bugs(SimConfig::chaos(seed, NODES), SERVERS, OPS, bugs);
        assert!(outcome.violation.is_none(), "seed {seed}: Raft itself should stay safe");
        matches!(outcome.verdict, checker::Verdict::NotLinearizable { .. })
    });
    let seed = caught.expect("stale local reads went undetected for 1,000 seeds");
    println!("stale local reads caught at seed {seed}");
}

#[test]
fn histories_are_recorded_and_complete_without_faults() {
    let outcome = run(SimConfig::new(3, NODES), SERVERS, OPS);
    outcome.assert_healthy();
    assert_eq!(outcome.operations, (2 * OPS) as usize, "every invocation is in the history");
}
