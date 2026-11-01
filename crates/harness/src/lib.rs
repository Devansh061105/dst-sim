//! Runs a Raft KV cluster under simulation with every check: Raft invariants each step, liveness, linearizability.
//!
//! The cluster runs one step at a time. After each step Raft's safety invariants are checked on
//! the running servers and client events are recorded into a history; the run stops once every
//! client has finished (liveness), and the history is then checked for linearizability. Tests and
//! the `dst` command-line tool both run clusters through here.

mod coverage;
mod scenario;
mod shrink;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use checker::Verdict;
pub use coverage::abstract_state;
use kv::{Bugs, ClientEvent, KvModel, Op, OpResult, Recorder, Server};
use raft::{Checker, Violation};
pub use scenario::{Knob, Scenario};
pub use shrink::{Shrunk, shrink};
use sim_api::{NodeId, Time};
use sim_core::{Script, SimConfig, Simulation, Stats, TraceEvent};

/// Raft servers in the standard cluster.
pub const SERVERS: u32 = 5;
/// Servers plus two clients.
pub const NODES: u32 = 7;
/// Operations each client performs.
pub const OPS: u32 = 30;
/// In a membership run, the configuration starts with this many of the [`SERVERS`].
pub const INITIAL_MEMBERS: u32 = 3;
/// Configuration changes the admin makes in a membership run.
pub const CHANGES: u32 = 6;
/// Nodes in a membership run: the server pool, the admin and two clients.
pub const MEMBERSHIP_NODES: u32 = SERVERS + 3;

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
    /// The simulator configuration for one seed of the standard cluster.
    pub fn config(self, seed: u64) -> SimConfig {
        self.config_for(seed, NODES)
    }

    /// The same for a cluster with `nodes` nodes (servers plus clients).
    pub fn config_for(self, seed: u64, nodes: u32) -> SimConfig {
        let mut config = match self {
            Mode::Calm => SimConfig::new(seed, nodes),
            Mode::Chaos => SimConfig::chaos(seed, nodes),
            Mode::Swarm => SimConfig::swarm(seed, nodes),
        };
        // Only servers keep data, and Raft can rebuild a damaged log only from a majority of
        // intact servers: corrupt at most a minority of them.
        config.faults.max_corrupted = config.faults.max_corrupted.min((SERVERS - 1) / 2);
        config
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
    TrustDamagedLog,
    ReadWithoutQuorum,
    SnapshotWithoutSessions,
}

impl Bug {
    pub const ALL: [Bug; 11] = [
        Bug::LocalReads,
        Bug::VoteWithoutLogCheck,
        Bug::ForgetVote,
        Bug::TruncateWithoutConflict,
        Bug::CommitBeyondMatch,
        Bug::SkipAppendFsync,
        Bug::CommitOldTerms,
        Bug::SingleSyncTruncate,
        Bug::TrustDamagedLog,
        Bug::ReadWithoutQuorum,
        Bug::SnapshotWithoutSessions,
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
            Bug::TrustDamagedLog => "trust-damaged-log",
            Bug::ReadWithoutQuorum => "read-without-quorum",
            Bug::SnapshotWithoutSessions => "snapshot-without-sessions",
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
            Bug::TrustDamagedLog => {
                "a corrupted log is cut short and trusted, so the node keeps voting"
            }
            Bug::ReadWithoutQuorum => "the leader serves reads without confirming it still leads",
            Bug::SnapshotWithoutSessions => "snapshots leave out the client session table",
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
            Bug::TrustDamagedLog => raft.trust_damaged_log = true,
            Bug::ReadWithoutQuorum => raft.read_without_quorum = true,
            Bug::SnapshotWithoutSessions => bugs.snapshot_without_sessions = true,
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

/// A planted bug in Raft's membership changes. Only membership runs ([`run_membership`]) can
/// catch these: without configuration changes the code they break never runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MembershipBug {
    /// Start a change before the previous one has committed.
    ConcurrentChanges,
    /// Change the configuration before committing an entry of the leader's own term.
    ChangeBeforeCommitInTerm,
    /// Decide with the latest committed configuration instead of the latest one in the log.
    UseCommittedConfig,
}

impl MembershipBug {
    /// Every membership bug.
    pub const ALL: [MembershipBug; 3] = [
        MembershipBug::ConcurrentChanges,
        MembershipBug::ChangeBeforeCommitInTerm,
        MembershipBug::UseCommittedConfig,
    ];

    /// Its command-line name.
    pub fn name(self) -> &'static str {
        match self {
            MembershipBug::ConcurrentChanges => "concurrent-changes",
            MembershipBug::ChangeBeforeCommitInTerm => "change-before-commit-in-term",
            MembershipBug::UseCommittedConfig => "use-committed-config",
        }
    }

    /// One line on what goes wrong.
    pub fn description(self) -> &'static str {
        match self {
            MembershipBug::ConcurrentChanges => "a change starts before the previous one commits",
            MembershipBug::ChangeBeforeCommitInTerm => {
                "a new leader changes the configuration before committing in its own term"
            }
            MembershipBug::UseCommittedConfig => {
                "servers use the committed configuration instead of the latest one"
            }
        }
    }

    /// The switch that plants it.
    pub fn bugs(self) -> Bugs {
        let mut raft = raft::Bugs::default();
        match self {
            MembershipBug::ConcurrentChanges => raft.concurrent_changes = true,
            MembershipBug::ChangeBeforeCommitInTerm => raft.change_before_commit_in_term = true,
            MembershipBug::UseCommittedConfig => raft.use_committed_config = true,
        }
        Bugs { raft, ..Bugs::default() }
    }
}

impl fmt::Display for MembershipBug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for MembershipBug {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        MembershipBug::ALL.into_iter().find(|b| b.name() == s).ok_or_else(|| {
            let names: Vec<_> = MembershipBug::ALL.iter().map(|b| b.name()).collect();
            format!("unknown membership bug {s:?} (expected one of: {})", names.join(", "))
        })
    }
}

/// One client operation, with the virtual times it was invoked and completed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpRecord {
    pub client: NodeId,
    pub op: Op,
    pub invoked: Time,
    /// `None` if the client never heard back (it crashed, or the run stopped first).
    pub completed: Option<(Time, OpResult)>,
}

/// Everything a run found out.
#[derive(Clone, Debug)]
pub struct Outcome {
    pub seed: u64,
    pub violation: Option<Violation>,
    /// A server that decided its disk was damaged (and stopped voting) although the simulator
    /// never damaged it: recovery mistook something else for corruption.
    pub false_alarm: Option<NodeId>,
    pub clients_done: bool,
    pub verdict: Verdict,
    pub operations: usize,
    pub committed: usize,
    pub max_term: u64,
    pub end_time: Time,
    pub stats: Stats,
    pub trace_hash: u64,
    /// Events processed.
    pub events: u64,
    /// The faults that were injected, written out (see [`Scenario::script`]).
    pub script: Script,
    /// Client operations, in the same order as the checked history (so the indexes in
    /// [`Verdict::NotLinearizable`] point into this).
    pub ops: Vec<OpRecord>,
    /// Every event, if the run kept them.
    pub trace: Option<Vec<TraceEvent>>,
    /// The abstract states the run went through (see [`abstract_state`]); empty unless asked
    /// for with [`run_cluster_with_coverage`].
    pub states: BTreeSet<u64>,
}

impl Outcome {
    /// Why the run failed, or `None` if it was safe, every client finished, and what clients
    /// observed is linearizable.
    pub fn failure(&self) -> Option<String> {
        if let Some(v) = &self.violation {
            return Some(format!("safety violation at {}: {v}", self.end_time));
        }
        if let Some(node) = self.false_alarm {
            return Some(format!(
                "recovery: {node} decided at {} that its disk was damaged, but it never was",
                self.end_time
            ));
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

    /// Which check failed: "Raft invariants", "recovery", "liveness" or "linearizability".
    /// Shrinking keeps a smaller run only if the same check fails. Any Raft property counts as
    /// the same failure: one bug can break a different property of Figure 3 depending on timing.
    pub fn failure_kind(&self) -> Option<&'static str> {
        if self.violation.is_some() {
            Some("Raft invariants")
        } else if self.false_alarm.is_some() {
            Some("recovery")
        } else if !self.clients_done {
            Some("liveness")
        } else {
            match self.verdict {
                Verdict::Linearizable => None,
                Verdict::NotLinearizable { .. } => Some("linearizability"),
                Verdict::Unknown { .. } => Some("linearizability check budget"),
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
    Scenario::new(mode, seed, bug).run(keep_trace)
}

/// Runs a cluster of `servers` servers; every other node in `config` is a client doing
/// `ops_per_client` operations.
pub fn run_cluster(config: SimConfig, servers: u32, ops_per_client: u32, bugs: Bugs) -> Outcome {
    run(config, servers, ops_per_client, bugs, false)
}

/// [`run_cluster`], also recording the abstract states the cluster passes through.
pub fn run_cluster_with_coverage(
    config: SimConfig,
    servers: u32,
    ops_per_client: u32,
    bugs: Bugs,
) -> Outcome {
    run(config, servers, ops_per_client, bugs, true)
}

/// A run in which the cluster's membership changes: [`SERVERS`] servers of which
/// [`INITIAL_MEMBERS`] start as the configuration, an admin that makes [`CHANGES`] changes
/// (adding or removing one server each), and two clients. The run must also let the admin
/// finish.
pub fn run_membership(mode: Mode, seed: u64, bug: Option<MembershipBug>) -> Outcome {
    let mut config = mode.config_for(seed, MEMBERSHIP_NODES);
    // The configuration can shrink to INITIAL_MEMBERS servers, which survive only a minority of
    // them losing data.
    config.faults.max_corrupted = config.faults.max_corrupted.min((INITIAL_MEMBERS - 1) / 2);
    let bugs = bug.map(MembershipBug::bugs).unwrap_or_default();
    let nodes = kv::cluster_with_membership(SERVERS, INITIAL_MEMBERS, CHANGES, OPS, bugs);
    run_nodes(config, nodes, false)
}

fn run(
    config: SimConfig,
    servers: u32,
    ops_per_client: u32,
    bugs: Bugs,
    coverage: bool,
) -> Outcome {
    run_nodes(config, kv::cluster_with_bugs(servers, ops_per_client, bugs), coverage)
}

fn run_nodes(
    config: SimConfig,
    nodes: impl FnMut(NodeId) -> kv::KvNode + 'static,
    coverage: bool,
) -> Outcome {
    let seed = config.seed;
    let mut states = BTreeSet::new();
    let mut sim = Simulation::new(config, nodes);
    let clients: Vec<_> =
        sim.nodes().filter(|(_, n)| n.as_client().is_some()).map(|(id, _)| id).collect();
    let mut checker = Checker::new();
    let mut recorder = Recorder::default();
    let mut ops: Vec<OpRecord> = Vec::new();
    let mut in_flight = BTreeMap::new();
    let mut violation = None;
    let mut false_alarm = None;
    let mut clients_done = false;
    while sim.step().is_none() {
        // Drain after every step so the history's order matches real time.
        let now = sim.now();
        for &client in &clients {
            let node = sim.node_mut(client).as_client_mut().expect("client stays a client");
            for event in node.drain_events() {
                match &event {
                    ClientEvent::Invoke { id, op } => {
                        in_flight.insert(*id, ops.len());
                        ops.push(OpRecord {
                            client,
                            op: op.clone(),
                            invoked: now,
                            completed: None,
                        });
                    }
                    ClientEvent::Complete { id, result } => {
                        if let Some(i) = in_flight.remove(id) {
                            ops[i].completed = Some((now, result.clone()));
                        }
                    }
                }
                recorder.record(event);
            }
        }
        let running = sim.nodes().filter(|&(id, _)| sim.is_up(id));
        if let Err(v) = checker.check(running.filter_map(|(_, n)| n.as_server()).map(Server::raft))
        {
            violation = Some(v);
            break;
        }
        if coverage {
            let rafts = sim
                .nodes()
                .filter_map(|(id, n)| n.as_server().map(|s| sim.is_up(id).then(|| s.raft())));
            states.insert(abstract_state(rafts));
        }
        // Rejoining (no votes until caught up) is only right after real damage; a false alarm
        // would quietly cost the cluster a voter, and show up much later as lost liveness.
        false_alarm = sim
            .nodes()
            .filter(|&(id, _)| sim.is_up(id) && !sim.was_corrupted(id))
            .find(|(_, n)| n.as_server().is_some_and(|s| s.raft().is_rejoining()))
            .map(|(id, _)| id);
        if false_alarm.is_some() {
            break;
        }
        // Clients, and the admin if there is one, must all have finished.
        clients_done = sim
            .nodes()
            .filter_map(|(id, n)| {
                let done = n.as_client().map(|c| c.is_done()).or(n.as_admin().map(|a| a.is_done()));
                done.map(|done| done && sim.is_up(id))
            })
            .all(|done| done);
        if clients_done {
            break;
        }
    }
    let max_term = sim.nodes().filter_map(|(_, n)| n.as_server()).map(|s| s.raft().term()).max();
    Outcome {
        seed,
        violation,
        false_alarm,
        clients_done,
        verdict: checker::check(&KvModel, recorder.history(), CHECK_BUDGET),
        operations: recorder.history().ops().len(),
        committed: checker.committed().len(),
        max_term: max_term.unwrap_or(0),
        end_time: sim.now(),
        stats: sim.stats(),
        trace_hash: sim.trace().hash(),
        events: sim.trace().event_count(),
        script: sim.script(),
        ops,
        trace: sim.trace().events().map(<[TraceEvent]>::to_vec),
        states,
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
