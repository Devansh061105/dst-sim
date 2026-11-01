//! Runs a KV cluster step by step, checking Raft's safety invariants after every step and
//! stopping once every client has finished (the liveness check).

use kv::Server;
use raft::{Checker, Violation};
use sim_api::Time;
use sim_core::{SimConfig, Simulation, Stats};

pub struct Outcome {
    pub seed: u64,
    pub violation: Option<Violation>,
    pub clients_done: bool,
    pub committed: usize,
    pub max_term: u64,
    pub end_time: Time,
    pub stats: Stats,
    pub trace_hash: u64,
}

impl Outcome {
    /// Panics with a replayable description unless the run was safe and every client finished.
    pub fn assert_healthy(&self) {
        if let Some(v) = &self.violation {
            panic!("seed {}: safety violation at {}: {v}", self.seed, self.end_time);
        }
        assert!(
            self.clients_done,
            "seed {}: clients still unfinished at {} (max term {}, committed {})",
            self.seed, self.end_time, self.max_term, self.committed
        );
    }
}

pub fn run(config: SimConfig, servers: u32, ops_per_client: u32) -> Outcome {
    let seed = config.seed;
    let mut sim = Simulation::new(config, kv::cluster(servers, ops_per_client));
    let mut checker = Checker::new();
    let mut violation = None;
    let mut clients_done = false;
    while sim.step().is_none() {
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
        committed: checker.committed().len(),
        max_term: max_term.unwrap_or(0),
        end_time: sim.now(),
        stats: sim.stats(),
        trace_hash: sim.trace().hash(),
    }
}
