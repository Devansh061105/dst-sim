//! Failure shrinking: turn a failing run into a much smaller one that fails the same way.
//!
//! A failing seed typically runs ten seconds of virtual time with dozens of crashes and
//! partitions, almost all of them irrelevant. The shrinker first writes the run's fault schedule
//! out (so faults can be deleted one by one), then repeatedly tries smaller variants and keeps any
//! that still fail with the same kind of failure:
//!
//! - delete faults from the schedule, in halves, then quarters, and so on down to single faults
//!   (Zeller's delta debugging, ddmin);
//! - switch whole fault types off (message loss, torn writes, BUGGIFY, ...);
//! - drop a client, and cut the number of operations;
//! - end the fault period earlier.
//!
//! Every candidate is itself a deterministic simulation, so "still fails" is a definite answer
//! and the result replays exactly.

use std::time::Duration;

use sim_api::Time;

use crate::scenario::{Knob, Scenario};

/// Upper bound on simulations per shrink, in case a failure is fragile and progress is slow.
const MAX_RUNS: u32 = 3_000;

/// What shrinking achieved.
#[derive(Clone, Debug)]
pub struct Shrunk {
    /// The failure, as found first (e.g. "safety: log matching").
    pub kind: &'static str,
    /// The original failing run, with its schedule written out.
    pub original: Scenario,
    pub original_events: u64,
    /// The smallest failing variant found.
    pub scenario: Scenario,
    pub events: u64,
    /// One line per accepted simplification.
    pub steps: Vec<String>,
    /// Simulations run.
    pub runs: u32,
}

impl Shrunk {
    /// Events in the original failing run per event in the shrunk one.
    pub fn ratio(&self) -> f64 {
        self.original_events as f64 / self.events.max(1) as f64
    }
}

/// Shrinks a failing scenario. Errors if it doesn't fail, or if its written-out schedule doesn't
/// reproduce the failure.
pub fn shrink(start: &Scenario) -> Result<Shrunk, String> {
    let first = start.run(false);
    let kind = first.failure_kind().ok_or("the scenario passes: nothing to shrink")?;
    let original = Scenario { script: Some(first.script.clone()), ..start.clone() };
    let replay = original.run(false);
    if replay.failure_kind() != Some(kind) {
        return Err(format!(
            "the written-out fault schedule doesn't reproduce the failure ({kind}): {}",
            replay.failure().unwrap_or_else(|| "it passes".to_owned())
        ));
    }

    let mut s = Shrinker {
        kind,
        best: original.clone(),
        events: replay.events,
        steps: Vec::new(),
        runs: 2,
    };
    loop {
        let before = s.best.clone();
        s.delete_faults();
        s.switch_off_fault_types();
        s.fewer_clients();
        s.fewer_ops();
        s.earlier_calm();
        if s.best == before || s.runs >= MAX_RUNS {
            break;
        }
    }
    Ok(Shrunk {
        kind: s.kind,
        original,
        original_events: replay.events,
        scenario: s.best,
        events: s.events,
        steps: s.steps,
        runs: s.runs,
    })
}

struct Shrinker {
    kind: &'static str,
    best: Scenario,
    /// Events in the run of `best`.
    events: u64,
    steps: Vec<String>,
    runs: u32,
}

impl Shrinker {
    /// Keeps `candidate` if it still fails the same way. Unless `longer_ok`, it must also not take
    /// more events to get there: switching a fault type off changes the whole run, which can
    /// make it longer. Deleting faults may make a failure happen later, and is kept anyway.
    fn try_accept(&mut self, candidate: Scenario, longer_ok: bool) -> bool {
        if self.runs >= MAX_RUNS {
            return false;
        }
        self.runs += 1;
        let outcome = candidate.run(false);
        if outcome.failure_kind() != Some(self.kind) || (outcome.events > self.events && !longer_ok)
        {
            return false;
        }
        self.best = candidate;
        self.events = outcome.events;
        true
    }

    fn faults(&self) -> usize {
        self.best.script.as_ref().map_or(0, |s| s.faults.len())
    }

    /// ddmin over the fault schedule: try deleting each of `n` chunks; on success keep the
    /// smaller schedule (and coarsen a little), otherwise split into twice as many chunks.
    fn delete_faults(&mut self) {
        let before = self.faults();
        let mut chunks = 2;
        while self.faults() > 0 {
            let len = self.faults();
            let size = len.div_ceil(chunks);
            let mut deleted = false;
            for start in (0..len).step_by(size) {
                let mut candidate = self.best.clone();
                let script = candidate.script.as_mut().expect("schedule written out");
                script.faults.drain(start..(start + size).min(len));
                if self.try_accept(candidate, true) {
                    chunks = (chunks - 1).max(2);
                    deleted = true;
                    break;
                }
            }
            if !deleted {
                if size == 1 {
                    break;
                }
                chunks = (chunks * 2).min(len);
            }
        }
        if self.faults() < before {
            self.steps.push(format!("faults: {before} -> {}", self.faults()));
        }
    }

    fn switch_off_fault_types(&mut self) {
        let config = self.best.config();
        for knob in Knob::ALL {
            if !knob.is_on(&config) || self.best.off.contains(&knob) {
                continue;
            }
            let mut candidate = self.best.clone();
            candidate.off.insert(knob);
            if self.try_accept(candidate, false) {
                self.steps.push(format!("switched off {}", knob.name()));
            }
        }
    }

    fn fewer_clients(&mut self) {
        while self.best.clients > 1 {
            let mut candidate = self.best.clone();
            candidate.clients -= 1;
            if !self.try_accept(candidate, false) {
                break;
            }
            self.steps.push(format!("clients: {}", self.best.clients));
        }
    }

    /// Halves the operation count while that still fails, then steps down one at a time.
    fn fewer_ops(&mut self) {
        let before = self.best.ops;
        for halving in [true, false] {
            while self.best.ops > 1 {
                let mut candidate = self.best.clone();
                candidate.ops = if halving { candidate.ops / 2 } else { candidate.ops - 1 };
                if !self.try_accept(candidate, false) {
                    break;
                }
            }
        }
        if self.best.ops < before {
            self.steps.push(format!("operations per client: {before} -> {}", self.best.ops));
        }
    }

    /// Binary search for the earliest end of the fault period (to 1 ms) that still fails.
    fn earlier_calm(&mut self) {
        let Some(script) = &self.best.script else { return };
        if script.faults.is_empty() {
            return; // nothing to end
        }
        let before = script.calm_at;
        let mut lo = script.faults.last().map_or(Time::ZERO, |(at, _)| *at);
        let mut hi = before;
        while hi - lo > Duration::from_millis(1) {
            let mid = lo + (hi - lo) / 2;
            let mut candidate = self.best.clone();
            candidate.script.as_mut().expect("schedule written out").calm_at = mid;
            if self.try_accept(candidate, false) {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        if hi < before {
            self.steps.push(format!("faults end at {hi} instead of {before}"));
        }
    }
}
