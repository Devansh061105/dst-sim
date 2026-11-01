//! Milestone M3: synced data survives crashes, unsynced data may not, torn tails are detected,
//! and the simulator catches code that acknowledges writes before they are durable.

use std::time::Duration;

use sim_api::NodeId;
use sim_core::toys::{Ledger, LedgerMode};
use sim_core::{DiskConfig, Fault, SimConfig, Simulation, Stats};

const N0: NodeId = NodeId(0);
/// Every ledger record is an 8-byte header plus an 8-byte entry number.
const RECORD_LEN: u64 = 16;

fn harsh_disk() -> DiskConfig {
    DiskConfig { unsynced_survive: 0.5, torn_write: 0.3, sync_fail: 0.02 }
}

struct Outcome {
    /// The first crash whose recovery lost acknowledged entries, as (acked, recovered).
    lost_ack: Option<(u64, u64)>,
    stats: Stats,
    trace_hash: u64,
}

/// Runs one ledger through `crashes` crash/restart cycles at seed-dependent moments.
fn crash_cycles(mode: LedgerMode, disk: DiskConfig, seed: u64, crashes: u64) -> Outcome {
    let config = SimConfig { disk, ..SimConfig::new(seed, 1) };
    let mut sim = Simulation::new(config, move |_| Ledger::new(mode));
    let mut lost_ack = None;
    for cycle in 0..crashes {
        let run_for = Duration::from_millis(100 + (seed * 37 + cycle * 101) % 900);
        sim.run_until(sim.now() + run_for);
        let acked = sim.node(N0).acked();

        sim.inject(Fault::Crash(N0));
        sim.inject(Fault::Restart(N0));
        sim.run_until(sim.now() + Duration::from_millis(1)); // just the restart's recovery

        let recovered = sim.node(N0).recovered();
        assert_eq!(
            sim.disk(N0).read("ledger").len() as u64,
            recovered * RECORD_LEN,
            "seed {seed}: recovery must cut the log back to its valid records"
        );
        if recovered < acked && lost_ack.is_none() {
            lost_ack = Some((acked, recovered));
        }
    }
    Outcome { lost_ack, stats: sim.stats(), trace_hash: sim.trace().hash() }
}

#[test]
fn strict_disk_keeps_exactly_what_was_synced() {
    let config = SimConfig::new(1, 1);
    let mut correct = Simulation::new(config.clone(), |_| Ledger::new(LedgerMode::Correct));
    let mut no_sync = Simulation::new(config, |_| Ledger::new(LedgerMode::NoSync));
    for sim in [&mut correct, &mut no_sync] {
        sim.run_until(sim.now() + Duration::from_millis(105));
        assert_eq!(sim.node(N0).acked(), 10);
        sim.inject(Fault::Crash(N0));
        sim.inject(Fault::Restart(N0));
        sim.run_until(sim.now() + Duration::from_millis(1));
    }
    assert_eq!(correct.node(N0).recovered(), 10, "every synced entry survives");
    assert_eq!(no_sync.node(N0).recovered(), 0, "nothing unsynced survives a strict disk");
}

#[test]
fn recovery_cuts_torn_tails() {
    // `crash_cycles` asserts the log length after every recovery; NoSync leaves plenty of torn
    // and partly surviving tails to cut.
    let mut torn = 0;
    for seed in 0..50 {
        torn += crash_cycles(LedgerMode::NoSync, harsh_disk(), seed, 5).stats.torn_writes;
    }
    assert!(torn > 0, "expected some torn writes to recover from");
}

#[test]
fn correct_ledger_never_loses_an_acknowledged_entry() {
    let mut sync_failures = 0;
    for seed in 0..300 {
        let outcome = crash_cycles(LedgerMode::Correct, harsh_disk(), seed, 5);
        assert_eq!(outcome.lost_ack, None, "seed {seed} lost acknowledged entries");
        sync_failures += outcome.stats.sync_failures;
    }
    assert!(sync_failures > 0, "the sweep should exercise failed syncs");
}

fn first_seed_losing_acks(mode: LedgerMode, seeds: u64) -> Option<(u64, (u64, u64))> {
    (0..seeds)
        .find_map(|seed| crash_cycles(mode, harsh_disk(), seed, 5).lost_ack.map(|l| (seed, l)))
}

#[test]
fn simulator_catches_acknowledging_before_fsync() {
    let found = first_seed_losing_acks(LedgerMode::NoSync, 20);
    assert!(found.is_some(), "NoSync bug went undetected");
}

#[test]
fn simulator_catches_fsyncgate() {
    let found = first_seed_losing_acks(LedgerMode::RetrySync, 300);
    assert!(found.is_some(), "retry-after-failed-fsync bug went undetected");
}

#[test]
fn disk_faults_replay_identically() {
    for seed in 0..50 {
        let a = crash_cycles(LedgerMode::NoSync, harsh_disk(), seed, 5);
        let b = crash_cycles(LedgerMode::NoSync, harsh_disk(), seed, 5);
        assert_eq!((a.trace_hash, a.stats), (b.trace_hash, b.stats), "seed {seed}");
    }
}
