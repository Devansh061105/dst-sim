//! One checked run of the two-phase commit cluster.

use sim_api::Time;
use sim_core::{SimConfig, Simulation, Stats};

use crate::check::{Checker, Violation};
use crate::{Bugs, Decision, NODES, TRANSACTIONS, TwoPcNode};

/// How a run went.
#[derive(Debug)]
pub struct Outcome {
    /// The first broken property, if any (the run stops there).
    pub violation: Option<Violation>,
    /// Whether every transaction was decided and every participant knows the decision.
    pub finished: bool,
    /// Transactions committed and aborted, by the coordinator's log.
    pub committed: u32,
    /// See `committed`.
    pub aborted: u32,
    /// When the run ended.
    pub end_time: Time,
    /// Events processed.
    pub events: u64,
    /// Faults injected.
    pub stats: Stats,
    /// Fingerprint of the run.
    pub trace_hash: u64,
}

impl Outcome {
    /// Why the run failed, or `None`.
    pub fn failure(&self) -> Option<String> {
        if let Some(v) = &self.violation {
            return Some(format!("safety violation at {}: {v}", self.end_time));
        }
        if !self.finished {
            return Some(format!(
                "liveness: transactions unfinished at {} ({} committed, {} aborted)",
                self.end_time, self.committed, self.aborted
            ));
        }
        None
    }

    /// The broken property, coarsely: `"atomicity"`, `"validity"`, `"durability"` or
    /// `"liveness"`.
    pub fn failure_kind(&self) -> Option<&'static str> {
        match &self.violation {
            Some(v) => Some(v.property()),
            None if !self.finished => Some("liveness"),
            None => None,
        }
    }
}

/// Runs the cluster under `config` (which must have [`NODES`] nodes), checking after every step.
pub fn run(mut config: SimConfig, bugs: Bugs) -> Outcome {
    assert_eq!(config.nodes, NODES, "two-phase commit runs on {NODES} nodes");
    // Nothing can repair a corrupted log without replicas: outside this system's fault model.
    config.disk.corrupt = 0.0;
    config.faults.max_corrupted = 0;
    let mut sim = Simulation::new(config, move |id| TwoPcNode::new(id, bugs));
    let mut checker = Checker::default();
    let mut violation = None;
    while sim.step().is_none() {
        let running = sim.nodes().filter(|&(id, _)| sim.is_up(id));
        if let Err(v) = checker.check(running) {
            violation = Some(v);
            break;
        }
    }

    let (mut committed, mut aborted, mut finished) = (0, 0, true);
    for (id, node) in sim.nodes() {
        match node {
            TwoPcNode::Coordinator(c) => {
                finished &= sim.is_up(id) && c.is_done();
                committed = c.decisions().iter().filter(|&&d| d == Decision::Commit).count() as u32;
                aborted = c.decisions().len() as u32 - committed;
            }
            TwoPcNode::Participant(p) => {
                finished &= (0..TRANSACTIONS)
                    .all(|tx| p.txs().get(&tx).is_some_and(|s| s.decision.is_some()));
            }
        }
    }
    Outcome {
        violation,
        finished,
        committed,
        aborted,
        end_time: sim.now(),
        events: sim.trace().event_count(),
        stats: sim.stats(),
        trace_hash: sim.trace().hash(),
    }
}
