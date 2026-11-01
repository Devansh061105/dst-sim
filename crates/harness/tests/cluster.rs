//! The Raft KV store stays safe under every fault, recovers to serve all clients, and what clients
//! observe is linearizable.

use std::cell::RefCell;
use std::time::Duration;

use harness::{Bug, Mode, NODES, OPS, SERVERS, run_cluster, run_seed};
use kv::Bugs;
use raft::{Checker, Raft, RaftConfig, RaftContext, RaftMsg, RaftTimer, Role, Violation};
use sim_api::{Context, Node, NodeId, Storage, SyncError, Time, TimerId};
use sim_core::{Fault, SimConfig, Simulation, Stats, StopReason};

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
        assert_eq!(outcome.ops.len(), (2 * OPS) as usize, "seed {seed}");
        // Every write is in the log; reads are not (ReadIndex).
        let writes = outcome.ops.iter().filter(|o| !matches!(o.op, kv::Op::Get { .. })).count();
        assert!(outcome.committed > writes, "seed {seed}: {} for {writes}", outcome.committed);
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
    // Damaged disks: every one must have been noticed and repaired from the other servers.
    assert!(total.corruptions > 0, "{total:?}");
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

/// Regression: in this swarm seed a stale leader of an older term lacks an entry committed in a
/// newer term. Raft allows that, but the first version of the checker flagged it (found at swarm
/// seed 11644; 9588 is a seed with the same situation after the simulator's random streams
/// changed). The checker was fixed, not Raft.
#[test]
fn stale_leader_of_older_term_is_not_a_violation() {
    run_seed(Mode::Swarm, 9588, None, false).assert_healthy();
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
        Bug::TrustDamagedLog,
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

/// Needs a leader cut off by a partition that still serves a read after a new leader has taken a
/// write: about one seed in a thousand.
#[test]
#[ignore = "thousands of seeds; run with --release -- --ignored"]
fn reads_without_a_leadership_check_are_caught() {
    assert!(caught_within(Bug::ReadWithoutQuorum, Mode::Chaos, 5_000).is_some());
}

/// Needs a server restored from a snapshot (after a restart, or installed from the leader) to
/// receive a retry of a write it already applied: a few seeds in a thousand.
#[test]
#[ignore = "thousands of seeds; run with --release -- --ignored"]
fn snapshots_without_the_session_table_are_caught() {
    assert!(caught_within(Bug::SnapshotWithoutSessions, Mode::Swarm, 5_000).is_some());
}

// Targeted scenarios. Two planted bugs need an exact sequence of events that random faults
// practically never line up, so these tests drive a bare Raft cluster through that sequence step
// by step: crashes and restarts are injected at chosen moments, and Raft's safety checker runs
// after every step, as in a normal run. Each scenario must pass on correct Raft and fail with its
// bug planted, for every seed tried (the seed only changes timings, never the sequence).

/// Servers in a scripted cluster.
const REPLICAS: u32 = 5;
/// The log file while there is no snapshot (these clusters never compact).
const LOG_FILE: &str = "raft-log";

/// A Raft server with nothing on top, so a test decides what gets proposed and when.
struct Replica {
    raft: Raft,
    /// Entries to propose as soon as this server leads. They are empty, like a leader's no-op,
    /// so every log record has the same size.
    to_propose: u64,
    disk_fault: DiskFault,
}

/// A fault the test makes this server's disk suffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DiskFault {
    None,
    /// The process dies in the middle of the first fsync of the log after the next log truncate.
    /// Of the operations that fsync was flushing, the writes reach the disk and the truncate does
    /// not. A crash mid-fsync on the simulated disk can end exactly like this; here the test
    /// picks that outcome instead of leaving it to chance.
    LoseNextTruncate,
    /// The truncate was dropped; the process dies at the next fsync of the log.
    TruncateLost,
}

impl Replica {
    fn new(id: NodeId, bugs: raft::Bugs) -> Self {
        let members = (0..REPLICAS).map(NodeId).collect();
        let raft = Raft::new(id, RaftConfig { bugs, ..RaftConfig::new(members) });
        Replica { raft, to_propose: 0, disk_fault: DiskFault::None }
    }

    fn handle(
        &mut self,
        ctx: &mut dyn Context<RaftMsg, RaftTimer>,
        event: impl FnOnce(&mut Raft, &mut RaftContext<'_>),
    ) {
        let nodes = ctx.nodes().to_vec();
        let mut ctx = FaultyDisk { ctx: RefCell::new(ctx), nodes, fault: &mut self.disk_fault };
        event(&mut self.raft, &mut ctx);
        while self.to_propose > 0 && self.raft.role() == Role::Leader {
            self.to_propose -= 1;
            if self.raft.propose(&mut ctx, Vec::new()).is_err() {
                break;
            }
        }
    }
}

impl Node for Replica {
    type Message = RaftMsg;
    type Timer = RaftTimer;

    fn on_start(&mut self, ctx: &mut dyn Context<RaftMsg, RaftTimer>) {
        self.handle(ctx, |raft, ctx| raft.start(ctx));
    }

    fn on_message(
        &mut self,
        ctx: &mut dyn Context<RaftMsg, RaftTimer>,
        from: NodeId,
        msg: RaftMsg,
    ) {
        self.handle(ctx, |raft, ctx| raft.handle_message(ctx, from, msg));
    }

    fn on_timer(
        &mut self,
        ctx: &mut dyn Context<RaftMsg, RaftTimer>,
        id: TimerId,
        timer: RaftTimer,
    ) {
        self.handle(ctx, |raft, ctx| raft.handle_timer(ctx, id, timer));
    }
}

/// The simulator's context with a [`DiskFault`] applied to its storage. (`Storage::read` takes
/// `&self` but reaching the storage needs `&mut`, hence the `RefCell`.)
struct FaultyDisk<'a, 'b> {
    ctx: RefCell<&'a mut (dyn Context<RaftMsg, RaftTimer> + 'b)>,
    nodes: Vec<NodeId>,
    fault: &'a mut DiskFault,
}

impl Context<RaftMsg, RaftTimer> for FaultyDisk<'_, '_> {
    fn id(&self) -> NodeId {
        self.ctx.borrow().id()
    }

    fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    fn now(&self) -> Time {
        self.ctx.borrow().now()
    }

    fn send(&mut self, to: NodeId, msg: RaftMsg) {
        self.ctx.get_mut().send(to, msg);
    }

    fn set_timer(&mut self, after: Duration, timer: RaftTimer) -> TimerId {
        self.ctx.get_mut().set_timer(after, timer)
    }

    fn cancel_timer(&mut self, id: TimerId) {
        self.ctx.get_mut().cancel_timer(id);
    }

    fn storage(&mut self) -> &mut dyn Storage {
        self
    }

    fn halt(&mut self) {
        self.ctx.get_mut().halt();
    }

    fn random_u64(&mut self) -> u64 {
        self.ctx.get_mut().random_u64()
    }

    fn buggify(&mut self) -> bool {
        self.ctx.get_mut().buggify()
    }
}

impl Storage for FaultyDisk<'_, '_> {
    fn read(&self, file: &str) -> Vec<u8> {
        self.ctx.borrow_mut().storage().read(file)
    }

    fn file_len(&self, file: &str) -> u64 {
        self.ctx.borrow_mut().storage().file_len(file)
    }

    fn write(&mut self, file: &str, offset: u64, data: &[u8]) {
        self.ctx.get_mut().storage().write(file, offset, data);
    }

    fn truncate(&mut self, file: &str, len: u64) {
        if file == LOG_FILE && *self.fault == DiskFault::LoseNextTruncate {
            *self.fault = DiskFault::TruncateLost;
        } else {
            self.ctx.get_mut().storage().truncate(file, len);
        }
    }

    fn sync(&mut self, file: &str) -> Result<(), SyncError> {
        let result = self.ctx.get_mut().storage().sync(file);
        if file == LOG_FILE && *self.fault == DiskFault::TruncateLost {
            // The writes are on disk now; the process dies before the fsync returns.
            *self.fault = DiskFault::None;
            self.ctx.get_mut().halt();
            return Err(SyncError);
        }
        result
    }
}

/// A cluster of [`Replica`]s on a fault-free network and a strict disk, where only the test
/// injects faults, checked after every step.
struct ScriptedCluster {
    sim: Simulation<Replica>,
    checker: Checker,
    seed: u64,
}

impl ScriptedCluster {
    fn new(seed: u64, bug: Option<Bug>) -> Self {
        let bugs = bug.map(|b| b.bugs().raft).unwrap_or_default();
        let sim = Simulation::new(SimConfig::new(seed, REPLICAS), move |id| Replica::new(id, bugs));
        ScriptedCluster { sim, checker: Checker::new(), seed }
    }

    fn servers() -> Vec<NodeId> {
        (0..REPLICAS).map(NodeId).collect()
    }

    fn raft(&self, id: NodeId) -> &Raft {
        &self.sim.node(id).raft
    }

    fn check(&mut self) -> Result<(), Violation> {
        let sim = &self.sim;
        let running = sim.nodes().filter(|&(id, _)| sim.is_up(id));
        self.checker.check(running.map(|(_, replica)| &replica.raft))
    }

    fn inject(&mut self, fault: Fault) -> Result<(), Violation> {
        self.sim.inject(fault);
        self.check()
    }

    fn crash(&mut self, nodes: &[NodeId]) -> Result<(), Violation> {
        nodes.iter().try_for_each(|&n| self.inject(Fault::Crash(n)))
    }

    fn restart(&mut self, nodes: &[NodeId]) -> Result<(), Violation> {
        nodes.iter().try_for_each(|&n| self.inject(Fault::Restart(n)))
    }

    /// Processes one event and checks; `false` if there was none left.
    fn step(&mut self) -> Result<bool, Violation> {
        match self.sim.step() {
            None => self.check().map(|()| true),
            Some(StopReason::Quiescent) => Ok(false),
            Some(stop) => panic!("seed {}: the run stopped ({stop:?})", self.seed),
        }
    }

    /// Steps until `found` returns something (checking first, so it may take no steps). Panics
    /// if that takes over 10 s of virtual time: the scenario itself went wrong.
    fn wait_for<T>(
        &mut self,
        what: &str,
        found: impl Fn(&Self) -> Option<T>,
    ) -> Result<T, Violation> {
        let deadline = self.sim.now() + Duration::from_secs(10);
        loop {
            if let Some(value) = found(self) {
                return Ok(value);
            }
            let stepped = self.sim.now() < deadline && self.step()?;
            assert!(stepped, "seed {}: never saw {what}", self.seed);
        }
    }

    /// Runs for `duration`, or until nothing is left to happen (every server down and nothing
    /// in flight).
    fn run_for(&mut self, duration: Duration) -> Result<(), Violation> {
        let end = self.sim.now() + duration;
        while self.sim.now() < end && self.step()? {}
        Ok(())
    }

    /// Lets every message in flight arrive, or be dropped at a crashed server, so that servers
    /// restarted afterwards never see them.
    fn drain_network(&mut self) -> Result<(), Violation> {
        self.run_for(Duration::from_millis(20)) // message delays are 1–10 ms
    }

    /// The running leader among `nodes`, if there is one.
    fn leader_among(&self, nodes: &[NodeId]) -> Option<NodeId> {
        let mut up = nodes.iter().copied().filter(|&n| self.sim.is_up(n));
        up.find(|&n| self.raft(n).role() == Role::Leader)
    }

    /// Waits for a leader among `nodes` that has committed its whole log, with every running
    /// server in `nodes` holding all of it.
    fn settled_leader(&mut self, nodes: &[NodeId]) -> Result<NodeId, Violation> {
        self.wait_for("a settled leader", |c| {
            let leader = c.leader_among(nodes)?;
            let last = c.raft(leader).last_log_index();
            let mut up = nodes.iter().filter(|&&n| c.sim.is_up(n));
            let all_hold = up.all(|&n| c.raft(n).last_log_index() == last);
            (c.raft(leader).commit_index() == last && all_hold).then_some(leader)
        })
    }
}

/// Figure 8 of the Raft paper with this implementation's no-op. A leader that counts replicas
/// of an entry from an earlier term may commit it although a server with a newer, conflicting
/// entry can still win an election and overwrite it. A new leader's no-op normally travels in
/// the same AppendEntries as the old entries and commits them safely; the window opens when the
/// leader has more old entries than fit in one message. Servers: `a` (the old leader), `e` (a
/// leader that wrote one entry nobody else got), `p` and `q` (voters), `b` (sits it out).
fn figure_8(seed: u64, bug: Option<Bug>) -> Result<(), Violation> {
    let mut c = ScriptedCluster::new(seed, bug);
    let servers = ScriptedCluster::servers();
    let batch = RaftConfig::new(servers.clone()).max_batch as u64;

    // 1. A leader `a` is elected and everyone holds its log (its no-op, maybe more).
    let a = c.settled_leader(&servers)?;
    let base = c.raft(a).last_log_index();
    let others: Vec<NodeId> = servers.iter().copied().filter(|&n| n != a).collect();
    let (b, three) = (others[0], &others[1..]);

    // 2. Alone, `a` appends more than one AppendEntries' worth of entries in its term, then
    //    crashes. Nobody else has them, so they are not committed.
    c.crash(&others)?;
    c.sim.node_mut(a).to_propose = batch + 6;
    let full = base + batch + 6;
    c.wait_for("`a` append its entries", |c| (c.raft(a).last_log_index() == full).then_some(()))?;
    c.crash(&[a])?;
    c.drain_network()?;

    // 3. The three others elect `e`. It writes its no-op (a newer term) and sends it, but all
    //    three crash before the message arrives: only `e` holds that entry.
    c.restart(three)?;
    let e = c.wait_for("one of the three win an election", |c| c.leader_among(three))?;
    let pq: Vec<NodeId> = three.iter().copied().filter(|&n| n != e).collect();
    c.crash(three)?;
    c.drain_network()?;

    // 4. `a` comes back and wins with `p` and `q` (its log is the most up to date of the three).
    //    It sends them its old entries, a batch at a time; its new no-op is last. As soon as
    //    `p` or `q` holds the first batch it crashes: its acknowledgement still reaches `a`,
    //    the next batch (with the no-op) doesn't reach it. So a majority (`a`, `p`, `q`) holds
    //    the first batch, all of it from `a`'s old term. Correct Raft doesn't commit it.
    c.restart(&[a])?;
    c.restart(&pq)?;
    for _ in 0..2 {
        let got = c.wait_for("`p` or `q` receive the first batch", |c| {
            pq.iter().copied().find(|&n| c.sim.is_up(n) && c.raft(n).last_log_index() > base)
        })?;
        assert_eq!(c.raft(got).last_log_index(), base + batch, "seed {seed}: one batch");
        assert_eq!(c.leader_among(&[a]), Some(a), "seed {seed}: the batch came from `a`");
        c.crash(&[got])?;
    }
    c.drain_network()?; // both acknowledgements arrive
    c.crash(&[a])?;
    c.drain_network()?;

    // 5. `e` wins with `p` and `q`: its last entry has a newer term than theirs. It overwrites
    //    the batch. With the bug, `a` committed that batch, so this loses committed entries.
    c.restart(&[e])?;
    c.restart(&pq)?;
    c.wait_for("`e` win an election", |c| c.leader_among(&[e]))?;

    // 6. Everyone back: the cluster settles on `e`'s log.
    c.restart(&[a, b])?;
    c.settled_leader(&servers)?;
    Ok(())
}

/// The leader commits entries of an earlier term by counting replicas (Figure 8 of the paper).
/// Random sweeps never caught it: the no-op almost always commits alongside the old entries.
#[test]
fn committing_old_terms_by_counting_is_caught_by_figure_8() {
    for seed in 0..10 {
        if let Err(v) = figure_8(seed, None) {
            panic!("seed {seed}: correct Raft failed the Figure 8 scenario: {v}");
        }
        // The new leader `e` lacks the batch the buggy leader committed.
        let caught = figure_8(seed, Some(Bug::CommitOldTerms));
        assert!(
            matches!(caught, Err(Violation::LeaderMissingCommitted { .. })),
            "seed {seed}: commit-old-terms not caught as expected: {caught:?}"
        );
    }
}

/// A follower replaces a stale tail with the leader's entries. If the truncate and the appends
/// share one fsync and the process dies inside it with the appends on disk but not the
/// truncate, the restarted follower's log is the new entries followed by the rest of the stale
/// tail (same-size records line up): new entries spliced in front of stale ones. Servers: `a`
/// (old leader), `f` (the follower with the stale tail), three others that elect `l`.
fn crash_inside_a_truncating_fsync(seed: u64, bug: Option<Bug>) -> Result<(), Violation> {
    let mut c = ScriptedCluster::new(seed, bug);
    let servers = ScriptedCluster::servers();

    // 1. A leader `a` is elected and everyone holds its log.
    let a = c.settled_leader(&servers)?;
    let base = c.raft(a).last_log_index();
    let others: Vec<NodeId> = servers.iter().copied().filter(|&n| n != a).collect();
    let (f, three) = (others[0], &others[1..]);

    // 2. With only `f` reachable, `a` appends three entries and `f` stores them: a tail that is
    //    never committed. Then both crash.
    c.crash(three)?;
    c.sim.node_mut(a).to_propose = 3;
    c.wait_for("`f` store the tail", |c| (c.raft(f).last_log_index() == base + 3).then_some(()))?;
    c.crash(&[a, f])?;
    c.drain_network()?;

    // 3. The other three elect `l`, whose no-op (a newer term) is entry `base + 1`.
    c.restart(three)?;
    c.settled_leader(three)?;

    // 4. `f` comes back. `l` sends it its no-op, which conflicts with `f`'s entry `base + 1`, so
    //    `f` truncates from there and appends. It dies inside the fsync after the truncate,
    //    keeping the writes but not the truncate. Correct Raft syncs the truncate on its own, so
    //    that fsync has no writes and nothing changes on disk; with the bug it also carries the
    //    append. Then `f` is restarted (by the simulated supervisor) and reads its log back.
    c.restart(&[f])?;
    c.sim.node_mut(f).disk_fault = DiskFault::LoseNextTruncate;
    c.wait_for("`f` die inside the fsync", |c| (!c.sim.is_up(f)).then_some(()))?;
    c.wait_for("`f` restart", |c| c.sim.is_up(f).then_some(()))?;

    // 5. Everyone back: the cluster settles on `l`'s log.
    c.restart(&[a])?;
    c.settled_leader(&servers)?;
    Ok(())
}

/// Truncate and append share one fsync. Random sweeps never caught it: it needs a real conflict,
/// a crash inside that one fsync, and the crash to keep the append but not the truncate.
#[test]
fn a_truncate_sharing_an_fsync_with_appends_is_caught() {
    for seed in 0..10 {
        if let Err(v) = crash_inside_a_truncating_fsync(seed, None) {
            panic!("seed {seed}: correct Raft failed the truncate scenario: {v}");
        }
        // The restarted `f` holds a stale entry right after a newer one.
        let caught = crash_inside_a_truncating_fsync(seed, Some(Bug::SingleSyncTruncate));
        assert!(
            matches!(caught, Err(Violation::LogMatching { .. })),
            "seed {seed}: single-sync-truncate not caught as expected: {caught:?}"
        );
    }
}
