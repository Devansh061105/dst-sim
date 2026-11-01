//! Runs a Raft KV cluster under simulation with every check: Raft invariants each step, liveness, linearizability.
//!
//! The cluster runs one step at a time. After each step Raft's safety invariants are checked on
//! the running servers and client events are recorded into a history; the run stops once every
//! client has finished (liveness), and the history is then checked for linearizability. Tests and
//! the `dst` command-line tool both run clusters through here.

use std::fmt;
use std::str::FromStr;

use checker::Verdict;
use kv::{Bugs, KvModel, Recorder, Server};
use raft::{Checker, Violation};
use sim_api::Time;
use sim_core::{SimConfig, Simulation, Stats};

/// Raft servers in the standard cluster.
pub const SERVERS: u32 = 5;
/// Servers plus two clients.
pub const NODES: u32 = 7;
/// Operations each client performs.
pub const OPS: u32 = 30;

/// Search steps allowed per key when checking linearizability.
const CHECK_BUDGET: u64 = 1_000_000;

/// Which fault preset a run uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// No faults: messages are only delayed.
    Calm,
    /// Every fault at a fixed intensity ([`SimConfig::chaos`]).
    Chaos,
    /// A random fault mix per seed ([`SimConfig::swarm`]).
    Swarm,
}

impl Mode {
    pub fn config(self, seed: u64) -> SimConfig {
        match self {
            Mode::Calm => SimConfig::new(seed, NODES),
            Mode::Chaos => SimConfig::chaos(seed, NODES),
            Mode::Swarm => SimConfig::swarm(seed, NODES),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Mode::Calm => "calm",
            Mode::Chaos => "chaos",
            Mode::Swarm => "swarm",
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for Mode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        [Mode::Calm, Mode::Chaos, Mode::Swarm]
            .into_iter()
            .find(|m| m.name() == s)
            .ok_or_else(|| format!("unknown mode {s:?} (expected calm, chaos or swarm)"))
    }
}

/// A planted bug, by name. Each is a real mistake; the test suite must catch every one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bug {
    LocalReads,
    VoteWithoutLogCheck,
    ForgetVote,
    TruncateWithoutConflict,
    CommitBeyondMatch,
    SkipAppendFsync,
    CommitOldTerms,
    SingleSyncTruncate,
}

impl Bug {
    pub const ALL: [Bug; 8] = [
        Bug::LocalReads,
        Bug::VoteWithoutLogCheck,
        Bug::ForgetVote,
        Bug::TruncateWithoutConflict,
        Bug::CommitBeyondMatch,
        Bug::SkipAppendFsync,
        Bug::CommitOldTerms,
        Bug::SingleSyncTruncate,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Bug::LocalReads => "local-reads",
            Bug::VoteWithoutLogCheck => "vote-without-log-check",
            Bug::ForgetVote => "forget-vote",
            Bug::TruncateWithoutConflict => "truncate-without-conflict",
            Bug::CommitBeyondMatch => "commit-beyond-match",
            Bug::SkipAppendFsync => "skip-append-fsync",
            Bug::CommitOldTerms => "commit-old-terms",
            Bug::SingleSyncTruncate => "single-sync-truncate",
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            Bug::LocalReads => "replicas answer reads from their own, possibly stale, copy",
            Bug::VoteWithoutLogCheck => "votes granted without the up-to-date log check",
            Bug::ForgetVote => "votes not persisted, so a restart can vote twice in a term",
            Bug::TruncateWithoutConflict => "followers truncate even when entries match",
            Bug::CommitBeyondMatch => "followers commit past the entries a message verified",
            Bug::SkipAppendFsync => "appended entries acknowledged without fsync",
            Bug::CommitOldTerms => "leaders commit earlier-term entries by counting (Figure 8)",
            Bug::SingleSyncTruncate => "truncate and append share one fsync",
        }
    }

    /// The bug switches that plant this bug.
    pub fn bugs(self) -> Bugs {
        let mut bugs = Bugs::default();
        let raft = &mut bugs.raft;
        match self {
            Bug::LocalReads => bugs.local_reads = true,
            Bug::VoteWithoutLogCheck => raft.vote_without_log_check = true,
            Bug::ForgetVote => raft.forget_vote = true,
            Bug::TruncateWithoutConflict => raft.truncate_without_conflict = true,
            Bug::CommitBeyondMatch => raft.commit_beyond_match = true,
            Bug::SkipAppendFsync => raft.skip_append_fsync = true,
            Bug::CommitOldTerms => raft.commit_old_terms = true,
            Bug::SingleSyncTruncate => raft.single_sync_truncate = true,
        }
        bugs
    }
}

impl fmt::Display for Bug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for Bug {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        Bug::ALL.into_iter().find(|b| b.name() == s).ok_or_else(|| {
            let names: Vec<_> = Bug::ALL.iter().map(|b| b.name()).collect();
            format!("unknown bug {s:?} (expected one of: {})", names.join(", "))
        })
    }
}

/// Everything a run found out.
#[derive(Clone, Debug)]
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
    /// One line per event, if the run kept them.
    pub trace: Option<Vec<String>>,
}

impl Outcome {
    /// Why the run failed, or `None` if it was safe, every client finished, and what clients
    /// observed is linearizable.
    pub fn failure(&self) -> Option<String> {
        if let Some(v) = &self.violation {
            return Some(format!("safety violation at {}: {v}", self.end_time));
        }
        if !self.clients_done {
            return Some(format!(
                "liveness: clients unfinished at {} (max term {}, {} entries committed)",
                self.end_time, self.max_term, self.committed
            ));
        }
        match &self.verdict {
            Verdict::Linearizable => None,
            Verdict::NotLinearizable { partition, ops } => Some(format!(
                "not linearizable: no valid order for the {} operations on key {partition}",
                ops.len()
            )),
            Verdict::Unknown { partition } => {
                Some(format!("linearizability check ran out of budget on key {partition}"))
            }
        }
    }

    pub fn passed(&self) -> bool {
        self.failure().is_none()
    }

    /// Panics with a replayable description unless the run passed.
    pub fn assert_healthy(&self) {
        if let Some(failure) = self.failure() {
            panic!("seed {}: {failure}", self.seed);
        }
    }
}

/// Runs the standard cluster (5 servers, 2 clients doing 30 operations each) for one seed.
pub fn run_seed(mode: Mode, seed: u64, bug: Option<Bug>, keep_trace: bool) -> Outcome {
    let config = SimConfig { keep_trace_lines: keep_trace, ..mode.config(seed) };
    run_cluster(config, SERVERS, OPS, bug.map_or_else(Bugs::default, Bug::bugs))
}

/// Runs a cluster of `servers` servers; every other node in `config` is a client doing
/// `ops_per_client` operations.
pub fn run_cluster(config: SimConfig, servers: u32, ops_per_client: u32, bugs: Bugs) -> Outcome {
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
        trace: sim.trace().lines().map(<[String]>::to_vec),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_round_trip() {
        for bug in Bug::ALL {
            assert_eq!(bug.name().parse::<Bug>(), Ok(bug));
        }
        for mode in [Mode::Calm, Mode::Chaos, Mode::Swarm] {
            assert_eq!(mode.name().parse::<Mode>(), Ok(mode));
        }
        assert!("no-such-bug".parse::<Bug>().is_err());
    }

    #[test]
    fn each_bug_flips_exactly_one_switch() {
        for bug in Bug::ALL {
            assert_ne!(bug.bugs(), Bugs::default(), "{bug}");
        }
        let distinct: std::collections::BTreeSet<_> =
            Bug::ALL.iter().map(|b| format!("{:?}", b.bugs())).collect();
        assert_eq!(distinct.len(), Bug::ALL.len());
    }
}
