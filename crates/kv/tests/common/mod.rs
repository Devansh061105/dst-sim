//! Runs a KV cluster step by step: Raft's safety invariants are checked after every step, client
//! events are recorded into a history, the run stops once every client has finished (liveness),
//! and the history is then checked for linearizability.

use checker::Verdict;
use kv::{Bugs, KvModel, Recorder, Server};
use raft::{Checker, Violation};
use sim_api::Time;
use sim_core::{SimConfig, Simulation, Stats};

/// Search steps allowed per key when checking linearizability.
const CHECK_BUDGET: u64 = 1_000_000;

pub struct Outcome {
    pub seed: u64,
    pub violation: Option<Violation>,
    pub clients_done: bool,
    pub verdict: Verdict,
    pub operations: usize,
    pub committed: usize,
    pub max_term: u64,
    pub end_time: Time,
    pub stats: Stats,
    pub trace_hash: u64,
}

impl Outcome {
    /// Panics with a replayable description unless the run was safe, every client finished, and
    /// what clients observed is linearizable.
    pub fn assert_healthy(&self) {
        if let Some(v) = &self.violation {
            panic!("seed {}: safety violation at {}: {v}", self.seed, self.end_time);
        }
        assert!(
            self.clients_done,
            "seed {}: clients still unfinished at {} (max term {}, committed {})",
            self.seed, self.end_time, self.max_term, self.committed
        );
        assert_eq!(self.verdict, Verdict::Linearizable, "seed {}: history check failed", self.seed);
    }
}

pub fn run(config: SimConfig, servers: u32, ops_per_client: u32) -> Outcome {
    run_with_bugs(config, servers, ops_per_client, Bugs::default())
}

pub fn run_with_bugs(config: SimConfig, servers: u32, ops_per_client: u32, bugs: Bugs) -> Outcome {
    let seed = config.seed;
    let mut sim = Simulation::new(config, kv::cluster_with_bugs(servers, ops_per_client, bugs));
    let clients: Vec<_> =
        sim.nodes().filter(|(_, n)| n.as_client().is_some()).map(|(id, _)| id).collect();
    let mut checker = Checker::new();
    let mut recorder = Recorder::default();
    let mut violation = None;
    let mut clients_done = false;
    while sim.step().is_none() {
        // Drain after every step so the history's order matches real time.
        for &id in &clients {
            let client = sim.node_mut(id).as_client_mut().expect("client stays a client");
            for event in client.drain_events() {
                recorder.record(event);
            }
        }
        let running = sim.nodes().filter(|&(id, _)| sim.is_up(id));
        if let Err(v) = checker.check(running.filter_map(|(_, n)| n.as_server()).map(Server::raft))
        {
            violation = Some(v);
            break;
        }
        clients_done = sim
            .nodes()
            .filter_map(|(id, n)| n.as_client().map(|c| sim.is_up(id) && c.is_done()))
            .all(|done| done);
        if clients_done {
            break;
        }
    }
    let max_term = sim.nodes().filter_map(|(_, n)| n.as_server()).map(|s| s.raft().term()).max();
    Outcome {
        seed,
        violation,
        clients_done,
        verdict: checker::check(&KvModel, recorder.history(), CHECK_BUDGET),
        operations: recorder.history().ops().len(),
        committed: checker.committed().len(),
        max_term: max_term.unwrap_or(0),
        end_time: sim.now(),
        stats: sim.stats(),
        trace_hash: sim.trace().hash(),
    }
}
