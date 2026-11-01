//! Raft membership changes under a scripted sequence of crashes: the bug found in 2015 in the
//! original single-server change algorithm, which random faults (200,000 seeds) never produce.
//!
//! Ongaro's report (raft-dev, 2015): a leader adds a server but the change reaches only that
//! server; a new leader removes another server before committing anything in its own term; then
//! the first leader returns, and under its own (uncommitted) configuration it holds a majority of
//! votes, wins, and overwrites the committed removal. The fix: a leader changes the configuration
//! only after committing an entry of its own term. This test runs the sequence with and without
//! the fix, for several seeds (the seed only moves timings, never the sequence).

use std::time::Duration;

use harness::MembershipBug;
use raft::{
    ChangeError, Checker, Raft, RaftConfig, RaftContext, RaftMsg, RaftTimer, Role, Violation,
};
use sim_api::{Context, Node, NodeId, TimerId};
use sim_core::{Fault, SimConfig, Simulation, StopReason};

/// Servers 0–3 start as the configuration; server 4 is the one to be added.
const SERVERS: u32 = 5;

/// A Raft server that can be told to change the configuration as soon as it leads.
struct Replica {
    raft: Raft,
    change: Option<Vec<NodeId>>,
}

impl Replica {
    fn handle(
        &mut self,
        ctx: &mut RaftContext<'_>,
        event: impl FnOnce(&mut Raft, &mut RaftContext<'_>),
    ) {
        event(&mut self.raft, ctx);
        if let Some(members) = self.change.clone() {
            match self.raft.change_membership(ctx, members) {
                Ok(_) => self.change = None,
                Err(ChangeError::Busy | ChangeError::NotLeader(_)) => {} // ask again next event
                Err(ChangeError::NotOneServer) => panic!("the test asked for a bad change"),
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

/// The cluster on a fault-free network, where only the test injects faults, checked after
/// every step.
struct Cluster {
    sim: Simulation<Replica>,
    checker: Checker,
    seed: u64,
}

impl Cluster {
    fn new(seed: u64, bug: Option<MembershipBug>) -> Self {
        let bugs = bug.map(|b| b.bugs().raft).unwrap_or_default();
        let initial: Vec<NodeId> = (0..SERVERS - 1).map(NodeId).collect();
        let sim = Simulation::new(SimConfig::new(seed, SERVERS), move |id| Replica {
            raft: Raft::new(id, RaftConfig { bugs, ..RaftConfig::new(initial.clone()) }),
            change: None,
        });
        Cluster { sim, checker: Checker::new(), seed }
    }

    fn raft(&self, id: NodeId) -> &Raft {
        &self.sim.node(id).raft
    }

    fn check(&mut self) -> Result<(), Violation> {
        let sim = &self.sim;
        let running = sim.nodes().filter(|&(id, _)| sim.is_up(id));
        self.checker.check(running.map(|(_, replica)| &replica.raft))
    }

    fn crash(&mut self, nodes: &[NodeId]) -> Result<(), Violation> {
        for &n in nodes {
            self.sim.inject(Fault::Crash(n));
        }
        self.check()
    }

    fn restart(&mut self, nodes: &[NodeId]) -> Result<(), Violation> {
        for &n in nodes {
            self.sim.inject(Fault::Restart(n));
        }
        self.check()
    }

    fn step(&mut self) -> Result<bool, Violation> {
        match self.sim.step() {
            None => self.check().map(|()| true),
            Some(StopReason::Quiescent) => Ok(false),
            Some(stop) => panic!("seed {}: the run stopped ({stop:?})", self.seed),
        }
    }

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

    fn run_for(&mut self, duration: Duration) -> Result<(), Violation> {
        let end = self.sim.now() + duration;
        while self.sim.now() < end && self.step()? {}
        Ok(())
    }

    /// Lets messages in flight arrive or be dropped at crashed servers (delays are 1–10 ms).
    fn drain_network(&mut self) -> Result<(), Violation> {
        self.run_for(Duration::from_millis(20))
    }

    fn leader_among(&self, nodes: &[NodeId]) -> Option<NodeId> {
        let mut up = nodes.iter().copied().filter(|&n| self.sim.is_up(n));
        up.find(|&n| self.raft(n).role() == Role::Leader)
    }
}

fn ongaro_2015(seed: u64, bug: Option<MembershipBug>) -> Result<(), Violation> {
    let mut c = Cluster::new(seed, bug);
    let members: Vec<NodeId> = (0..SERVERS - 1).map(NodeId).collect();
    let new = NodeId(SERVERS - 1);

    // 1. A leader `a` of the four members commits its no-op on all of them.
    let a = c.wait_for("a settled leader", |c| {
        let a = c.leader_among(&members)?;
        let last = c.raft(a).last_log_index();
        let all = members.iter().all(|&n| c.raft(n).commit_index() == last);
        all.then_some(a)
    })?;
    let others: Vec<NodeId> = members.iter().copied().filter(|&n| n != a).collect();

    // 2. `a` adds the new server, but only the new server receives the change: everyone else
    //    is down. Two of five hold it, so it isn't committed.
    c.crash(&others)?;
    let with_new: Vec<NodeId> = (0..SERVERS).map(NodeId).collect();
    c.sim.node_mut(a).change = Some(with_new);
    let last = c.wait_for("`a` append the change", |c| {
        let r = c.raft(a);
        (r.members().len() == SERVERS as usize).then_some(r.last_log_index())
    })?;
    c.wait_for("the new server receive it", |c| {
        (c.raft(new).last_log_index() == last).then_some(())
    })?;
    c.crash(&[a, new])?;
    c.drain_network()?;

    // 3. The other three elect `l` (the configuration they know is still the four). It removes
    //    `a` right away. One of the three, `d`, crashes before hearing anything from `l`.
    let without_a: Vec<NodeId> = members.iter().copied().filter(|&n| n != a).collect();
    c.restart(&others)?;
    let l = c.wait_for("one of the three win", |c| c.leader_among(&others))?;
    c.sim.node_mut(l).change = Some(without_a);
    let d = others.iter().copied().find(|&n| n != l).expect("three servers");
    c.crash(&[d])?;
    // With the bug, `l` commits the removal with the third server alone (two of the three left
    // in the new configuration). Correctly, it must first commit its no-op, which needs three
    // of the four old members, so the removal waits.
    c.run_for(Duration::from_secs(1))?;
    let rest: Vec<NodeId> = others.iter().copied().filter(|&n| n != d).collect();
    c.crash(&rest)?;
    c.drain_network()?;

    // 4. `a`, the new server and `d` come back. In `a`'s configuration (all five) they are a
    //    majority, and `d`'s log is behind `a`'s, so `a` can win. With the bug it then lacks the
    //    committed removal: a leader missing a committed entry.
    c.restart(&[a, new, d])?;
    c.wait_for("a leader", |c| c.leader_among(&[a, new, d]))?;
    c.run_for(Duration::from_millis(500))?;

    // 5. Everyone back; the cluster must settle without breaking anything.
    c.restart(&rest)?;
    c.run_for(Duration::from_secs(2))?;
    Ok(())
}

#[test]
fn changing_membership_before_committing_in_the_new_term_is_caught() {
    for seed in 0..10 {
        if let Err(v) = ongaro_2015(seed, None) {
            panic!("seed {seed}: correct Raft failed the 2015 scenario: {v}");
        }
        // `a` wins without the removal that `l` committed.
        let caught = ongaro_2015(seed, Some(MembershipBug::ChangeBeforeCommitInTerm));
        assert!(
            matches!(caught, Err(Violation::LeaderMissingCommitted { .. })),
            "seed {seed}: change-before-commit-in-term not caught as expected: {caught:?}"
        );
    }
}

/// Membership runs (an admin adding and removing servers while clients run) under the usual
/// fault modes.
mod sweeps {
    use harness::{MembershipBug, Mode, run_membership};

    #[test]
    fn correct_raft_survives_membership_changes_under_faults() {
        for seed in 0..150 {
            for mode in [Mode::Chaos, Mode::Swarm] {
                let outcome = run_membership(mode, seed, None);
                assert_eq!(outcome.failure(), None, "seed {seed} ({mode})");
            }
        }
    }

    #[test]
    fn deciding_with_the_committed_configuration_is_caught() {
        let caught = (0..1000).find(|&seed| {
            let bug = Some(MembershipBug::UseCommittedConfig);
            run_membership(Mode::Chaos, seed, bug).failure_kind().is_some()
        });
        assert!(caught.is_some(), "use-committed-config not caught in 1,000 chaos seeds");
    }

    /// About 1 in 270 chaos seeds: slow in a debug build.
    #[test]
    #[ignore]
    fn concurrent_changes_are_caught() {
        let caught = (0..5000).find(|&seed| {
            let bug = Some(MembershipBug::ConcurrentChanges);
            run_membership(Mode::Chaos, seed, bug).failure_kind().is_some()
        });
        assert!(caught.is_some(), "concurrent-changes not caught in 5,000 chaos seeds");
    }
}
