//! Written-out fault schedules replay exactly, and shrinking turns a failing run into a much
//! smaller one that still fails the same way.

use harness::{Bug, Mode, Scenario, shrink};

#[test]
fn written_out_schedule_replays_the_same_run() {
    for mode in [Mode::Chaos, Mode::Swarm] {
        for seed in 0..40 {
            let scenario = Scenario::new(mode, seed, None);
            let original = scenario.run(false);
            let replay = Scenario { script: Some(original.script.clone()), ..scenario }.run(false);
            assert_eq!(replay.trace_hash, original.trace_hash, "{mode} seed {seed}");
        }
    }
}

/// Acknowledging appends before fsync only loses data if a process dies, so the shrunk run must
/// keep a crash or a halt (after a failed fsync), and little else.
#[test]
fn shrinking_keeps_the_failure_and_cuts_the_faults() {
    let start = (0..200)
        .map(|seed| Scenario::new(Mode::Chaos, seed, Some(Bug::SkipAppendFsync)))
        .find(|s| {
            let outcome = s.run(false);
            !outcome.passed() && outcome.script.faults.len() >= 5
        })
        .expect("skip-append-fsync is caught within 200 seeds");
    let shrunk = shrink(&start).expect("a failing run shrinks");
    assert_eq!(shrunk.kind, "Raft invariants");
    let script = shrunk.scenario.script.as_ref().expect("schedule written out");
    let original = shrunk.original.script.as_ref().expect("schedule written out");
    assert!(script.faults.len() * 2 <= original.faults.len(), "{shrunk:?}");

    // The result is a plain text file that replays to the same failing run.
    let saved = shrunk.scenario.to_string();
    let loaded: Scenario = saved.parse().expect("parses");
    let replay = loaded.run(false);
    assert_eq!(replay.failure_kind(), Some("Raft invariants"), "{saved}");
    assert_eq!(replay.events, shrunk.events);
    assert!(replay.stats.crashes + replay.stats.halts > 0, "{saved}");
}

#[test]
fn shrinking_a_passing_run_is_an_error() {
    assert!(shrink(&Scenario::new(Mode::Chaos, 0, None)).is_err());
}
