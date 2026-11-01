//! A second system under test: two-phase commit.
//!
//! It runs on the same simulator, under the same faults, through the same `dst` sweeps as the
//! Raft store, with its own checker ([`check`]) and its own planted bugs ([`Bugs`]). That is the
//! point: the framework is not tied to Raft.
//!
//! One coordinator (node 0) runs [`TRANSACTIONS`] transactions one after another across
//! [`PARTICIPANTS`] participants:
//! 1. *Prepare.* The coordinator asks every participant to vote. A participant decides yes or no
//!    (no about one time in ten), writes its vote to disk and syncs it **before** answering:
//!    once it has said yes it must be able to commit, even after a crash.
//! 2. *Decide.* Any no means abort; all yes means commit. The coordinator syncs its decision
//!    **before** telling anyone, then resends it until every participant has acknowledged it
//!    (participants sync the decision before acknowledging).
//!
//! Recovery: a participant replays its log and answers again what it already answered. The
//! coordinator replays its decision log, resends every decision that lacks a *done* record
//! (written once all participants acknowledged it), and *presumes abort* for the transaction it
//! may have been preparing: it logs an abort for it, since no commit for it can have been sent
//! without being logged first. Classic two-phase commit blocks while the
//! coordinator is down; the simulation's calm period restarts it, so every transaction must still
//! finish.
//!
//! Disk corruption is switched off for this system: there is no replica to repair a damaged log
//! from, so corruption at rest is outside what it promises to survive.

pub mod check;
mod run;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use sim_api::{Context, Node, NodeId, TimerId, record};

pub use run::{Outcome, run};

/// The coordinator's id.
pub const COORDINATOR: NodeId = NodeId(0);
/// Participants are nodes `1..=PARTICIPANTS`.
pub const PARTICIPANTS: u32 = 3;
/// Coordinator plus participants.
pub const NODES: u32 = PARTICIPANTS + 1;
/// Transactions per run.
pub const TRANSACTIONS: u32 = 50;

/// How often unanswered prepares and decisions are resent.
const RETRY: Duration = Duration::from_millis(100);
/// Pause between transactions (milliseconds), so a run's work spans the fault period.
const PAUSE_MS: std::ops::Range<u64> = 20..200;
/// How long a participant with the planted `abort_after_yes_timeout` bug waits.
const GIVE_UP: Duration = Duration::from_millis(300);
/// Chance (in percent) that a participant votes no.
const NO_VOTE_PERCENT: u64 = 10;

const COORDINATOR_LOG: &str = "decisions";
/// Coordinator log record kinds: 0 abort, 1 commit, [`DONE`] = every participant acknowledged.
const DONE: u8 = 2;
const PARTICIPANT_LOG: &str = "votes";

/// Planted bugs, all off by default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Bugs {
    /// The coordinator commits once a majority voted yes.
    pub commit_on_majority: bool,
    /// Participants answer before their vote is synced.
    pub vote_without_fsync: bool,
    /// The coordinator sends its decision without syncing it.
    pub decision_not_synced: bool,
    /// A participant that voted yes aborts on its own if no decision arrives in time (it must
    /// wait: the coordinator may already have committed).
    pub abort_after_yes_timeout: bool,
}

/// A planted bug, by name (for the `dst` tool).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bug {
    /// See [`Bugs::commit_on_majority`].
    CommitOnMajority,
    /// See [`Bugs::vote_without_fsync`].
    VoteWithoutFsync,
    /// See [`Bugs::decision_not_synced`].
    DecisionNotSynced,
    /// See [`Bugs::abort_after_yes_timeout`].
    AbortAfterYesTimeout,
}

impl Bug {
    /// Every planted bug.
    pub const ALL: [Bug; 4] = [
        Bug::CommitOnMajority,
        Bug::VoteWithoutFsync,
        Bug::DecisionNotSynced,
        Bug::AbortAfterYesTimeout,
    ];

    /// Its command-line name.
    pub fn name(self) -> &'static str {
        match self {
            Bug::CommitOnMajority => "commit-on-majority",
            Bug::VoteWithoutFsync => "vote-without-fsync",
            Bug::DecisionNotSynced => "decision-not-synced",
            Bug::AbortAfterYesTimeout => "abort-after-yes-timeout",
        }
    }

    /// One line on what goes wrong.
    pub fn description(self) -> &'static str {
        match self {
            Bug::CommitOnMajority => "the coordinator commits once a majority voted yes",
            Bug::VoteWithoutFsync => "participants answer before their vote is synced",
            Bug::DecisionNotSynced => "the coordinator sends a decision it has not synced",
            Bug::AbortAfterYesTimeout => "a participant that voted yes gives up and aborts",
        }
    }

    /// The switch that plants it.
    pub fn bugs(self) -> Bugs {
        let mut bugs = Bugs::default();
        match self {
            Bug::CommitOnMajority => bugs.commit_on_majority = true,
            Bug::VoteWithoutFsync => bugs.vote_without_fsync = true,
            Bug::DecisionNotSynced => bugs.decision_not_synced = true,
            Bug::AbortAfterYesTimeout => bugs.abort_after_yes_timeout = true,
        }
        bugs
    }
}

impl std::fmt::Display for Bug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl std::str::FromStr for Bug {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        Bug::ALL.into_iter().find(|b| b.name() == s).ok_or_else(|| {
            let names: Vec<_> = Bug::ALL.iter().map(|b| b.name()).collect();
            format!("unknown two-phase commit bug {s:?} (expected one of: {})", names.join(", "))
        })
    }
}

/// A transaction's outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Decision {
    /// Every participant applies it.
    Commit,
    /// Nobody applies it.
    Abort,
}

/// Messages between the coordinator and participants.
#[derive(Clone, Debug, Hash)]
pub enum Msg {
    /// Coordinator → participant: vote on transaction `tx`.
    Prepare(u32),
    /// Participant → coordinator: its vote (yes = `true`).
    Vote(u32, bool),
    /// Coordinator → participant: the outcome.
    Decide(u32, Decision),
    /// Participant → coordinator: decision recorded.
    Ack(u32),
}

/// Timers.
#[derive(Clone, Debug, Hash)]
pub enum Tick {
    /// Coordinator: resend what is unanswered.
    Retry,
    /// Coordinator: start the next transaction.
    Next,
    /// Participant with the planted bug: stop waiting for the decision on `tx`.
    GiveUp(u32),
}

/// A node of the cluster.
#[derive(Debug)]
pub enum TwoPcNode {
    /// Node 0.
    Coordinator(Coordinator),
    /// Every other node.
    Participant(Participant),
}

impl TwoPcNode {
    /// Node `id` with `bugs` planted.
    pub fn new(id: NodeId, bugs: Bugs) -> Self {
        if id == COORDINATOR {
            TwoPcNode::Coordinator(Coordinator::new(bugs))
        } else {
            TwoPcNode::Participant(Participant { bugs, txs: BTreeMap::new() })
        }
    }
}

impl Node for TwoPcNode {
    type Message = Msg;
    type Timer = Tick;

    fn on_start(&mut self, ctx: &mut dyn Context<Msg, Tick>) {
        match self {
            TwoPcNode::Coordinator(c) => c.start(ctx),
            TwoPcNode::Participant(p) => p.start(ctx),
        }
    }

    fn on_message(&mut self, ctx: &mut dyn Context<Msg, Tick>, from: NodeId, msg: Msg) {
        match self {
            TwoPcNode::Coordinator(c) => c.on_message(ctx, from, msg),
            TwoPcNode::Participant(p) => p.on_message(ctx, msg),
        }
    }

    fn on_timer(&mut self, ctx: &mut dyn Context<Msg, Tick>, id: TimerId, tick: Tick) {
        match self {
            TwoPcNode::Coordinator(c) => c.on_timer(ctx, id),
            TwoPcNode::Participant(p) => p.on_timer(ctx, tick),
        }
    }
}

fn participants() -> impl Iterator<Item = NodeId> {
    (1..=PARTICIPANTS).map(NodeId)
}

fn encode(tx: u32, kind: u8) -> Vec<u8> {
    let mut payload = tx.to_le_bytes().to_vec();
    payload.push(kind);
    record::encode(&payload)
}

/// Reads a log of `(tx, kind)` records and cuts off a torn tail (only unsynced writes can be
/// torn, so nothing acknowledged is lost). `None` if the node halted.
///
/// The cut is synced on its own before anything new is appended. Otherwise a crash could keep a
/// later append but lose the cut, and a new record of the same size as a lost one would fill the
/// gap exactly and bring an old, never-acknowledged record after it back to life.
fn recover_log(ctx: &mut dyn Context<Msg, Tick>, file: &str) -> Option<Vec<(u32, u8)>> {
    let storage = ctx.storage();
    let decoded = record::decode(&storage.read(file));
    if decoded.valid_len < storage.file_len(file) {
        storage.truncate(file, decoded.valid_len);
        if storage.sync(file).is_err() {
            ctx.halt();
            return None;
        }
    }
    Some(
        decoded
            .records
            .iter()
            .filter_map(|r| {
                let tx = u32::from_le_bytes(r.get(..4)?.try_into().ok()?);
                Some((tx, *r.get(4)?))
            })
            .collect(),
    )
}

/// Appends a record and syncs it unless `skip_sync`. `false` means the sync failed and the node
/// halted.
fn persist(
    ctx: &mut dyn Context<Msg, Tick>,
    file: &str,
    tx: u32,
    kind: u8,
    skip_sync: bool,
) -> bool {
    let storage = ctx.storage();
    storage.append(file, &encode(tx, kind));
    if !skip_sync && storage.sync(file).is_err() {
        ctx.halt();
        return false;
    }
    true
}

#[derive(Debug)]
enum Phase {
    Preparing { votes: BTreeMap<NodeId, bool> },
    Delivering { decision: Decision, acks: BTreeSet<NodeId> },
}

/// The coordinator.
#[derive(Debug)]
pub struct Coordinator {
    bugs: Bugs,
    /// Every decision so far, by transaction number (the durable log).
    decisions: Vec<Decision>,
    /// The transaction being worked on.
    current: Option<(u32, Phase)>,
    /// Decisions to resend after a restart before starting new transactions.
    redeliver: VecDeque<u32>,
    retry: Option<TimerId>,
    /// The pause before the next transaction.
    pause: Option<TimerId>,
}

impl Coordinator {
    fn new(bugs: Bugs) -> Self {
        Coordinator {
            bugs,
            decisions: Vec::new(),
            current: None,
            redeliver: VecDeque::new(),
            retry: None,
            pause: None,
        }
    }

    /// Decisions made so far, by transaction.
    pub fn decisions(&self) -> &[Decision] {
        &self.decisions
    }

    /// Whether every transaction is decided and acknowledged by every participant.
    pub fn is_done(&self) -> bool {
        self.current.is_none()
            && self.pause.is_none()
            && self.decisions.len() == TRANSACTIONS as usize
    }

    fn start(&mut self, ctx: &mut dyn Context<Msg, Tick>) {
        let Some(log) = recover_log(ctx, COORDINATOR_LOG) else { return };
        self.decisions = log
            .iter()
            .filter(|&&(_, kind)| kind != DONE)
            .map(|&(_, kind)| if kind == 1 { Decision::Commit } else { Decision::Abort })
            .collect();
        let decided = self.decisions.len() as u32;
        // Resend every decision not known to have reached everyone.
        let done: BTreeSet<u32> =
            log.iter().filter(|&&(_, kind)| kind == DONE).map(|&(tx, _)| tx).collect();
        self.redeliver = (0..decided).filter(|tx| !done.contains(tx)).collect();
        // Presumed abort: the next transaction may have been prepared before the crash. No
        // commit for it was sent (that would have been logged first), so aborting it is safe.
        // (With an empty log, preparing transaction 0 again is just as safe: votes are durable.)
        if decided > 0 && decided < TRANSACTIONS {
            if !persist(ctx, COORDINATOR_LOG, decided, 0, false) {
                return;
            }
            self.decisions.push(Decision::Abort);
            self.redeliver.push_back(decided);
        }
        self.next(ctx);
    }

    /// Moves on: redeliver a recovered decision, or prepare the next transaction.
    fn next(&mut self, ctx: &mut dyn Context<Msg, Tick>) {
        if let Some(tx) = self.redeliver.pop_front() {
            let decision = self.decisions[tx as usize];
            self.current = Some((tx, Phase::Delivering { decision, acks: BTreeSet::new() }));
        } else {
            self.current = None;
            if let Some(id) = self.retry.take() {
                ctx.cancel_timer(id);
            }
            if self.decisions.len() < TRANSACTIONS as usize {
                let pause = Duration::from_millis(ctx.random_range(PAUSE_MS));
                self.pause = Some(ctx.set_timer(pause, Tick::Next));
            }
            return;
        }
        self.resend(ctx);
    }

    /// Sends whatever is still unanswered and restarts the retry timer.
    fn resend(&mut self, ctx: &mut dyn Context<Msg, Tick>) {
        let Some((tx, phase)) = &self.current else { return };
        for p in participants() {
            match phase {
                Phase::Preparing { votes } if !votes.contains_key(&p) => {
                    ctx.send(p, Msg::Prepare(*tx));
                }
                Phase::Delivering { decision, acks } if !acks.contains(&p) => {
                    ctx.send(p, Msg::Decide(*tx, *decision));
                }
                _ => {}
            }
        }
        if let Some(id) = self.retry.take() {
            ctx.cancel_timer(id);
        }
        self.retry = Some(ctx.set_timer(RETRY, Tick::Retry));
    }

    fn on_message(&mut self, ctx: &mut dyn Context<Msg, Tick>, from: NodeId, msg: Msg) {
        let Some((tx, phase)) = &mut self.current else { return };
        match (msg, phase) {
            (Msg::Vote(t, yes), Phase::Preparing { votes }) if t == *tx => {
                votes.entry(from).or_insert(yes);
                let yes_votes = votes.values().filter(|&&v| v).count() as u32;
                let decision = if self.bugs.commit_on_majority && yes_votes > PARTICIPANTS / 2 {
                    Decision::Commit
                } else if votes.values().any(|&v| !v) {
                    Decision::Abort
                } else if yes_votes == PARTICIPANTS {
                    Decision::Commit
                } else {
                    return;
                };
                let tx = *tx;
                let kind = u8::from(decision == Decision::Commit);
                if !persist(ctx, COORDINATOR_LOG, tx, kind, self.bugs.decision_not_synced) {
                    return;
                }
                self.decisions.push(decision);
                self.current = Some((tx, Phase::Delivering { decision, acks: BTreeSet::new() }));
                self.resend(ctx);
            }
            (Msg::Ack(t), Phase::Delivering { acks, .. }) if t == *tx => {
                acks.insert(from);
                if acks.len() as u32 == PARTICIPANTS {
                    // Everyone has it: no need to resend it after a restart. Not synced: if
                    // this record is lost, recovery just resends a decision that arrived.
                    ctx.storage().append(COORDINATOR_LOG, &encode(t, DONE));
                    self.next(ctx);
                }
            }
            _ => {}
        }
    }

    fn on_timer(&mut self, ctx: &mut dyn Context<Msg, Tick>, id: TimerId) {
        if self.retry == Some(id) {
            self.retry = None;
            self.resend(ctx);
        } else if self.pause == Some(id) {
            self.pause = None;
            let tx = self.decisions.len() as u32;
            self.current = Some((tx, Phase::Preparing { votes: BTreeMap::new() }));
            self.resend(ctx);
        }
    }
}

/// What a participant knows about one transaction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TxState {
    /// Its vote (yes = `true`), once cast.
    pub vote: Option<bool>,
    /// The outcome, once known.
    pub decision: Option<Decision>,
}

/// A participant.
#[derive(Debug)]
pub struct Participant {
    bugs: Bugs,
    txs: BTreeMap<u32, TxState>,
}

// Log record kinds.
const VOTE_NO: u8 = 0;
const VOTE_YES: u8 = 1;
const COMMITTED: u8 = 2;
const ABORTED: u8 = 3;

impl Participant {
    /// What this participant knows, by transaction.
    pub fn txs(&self) -> &BTreeMap<u32, TxState> {
        &self.txs
    }

    fn start(&mut self, ctx: &mut dyn Context<Msg, Tick>) {
        self.txs.clear();
        let Some(log) = recover_log(ctx, PARTICIPANT_LOG) else { return };
        for (tx, kind) in log {
            let state = self.txs.entry(tx).or_default();
            match kind {
                VOTE_NO => state.vote = Some(false),
                VOTE_YES => state.vote = Some(true),
                COMMITTED => state.decision = Some(Decision::Commit),
                _ => state.decision = Some(Decision::Abort),
            }
        }
        if self.bugs.abort_after_yes_timeout {
            for (&tx, state) in &self.txs {
                if state.vote == Some(true) && state.decision.is_none() {
                    ctx.set_timer(GIVE_UP, Tick::GiveUp(tx));
                }
            }
        }
    }

    fn on_message(&mut self, ctx: &mut dyn Context<Msg, Tick>, msg: Msg) {
        match msg {
            Msg::Prepare(tx) => {
                let state = self.txs.get(&tx).copied().unwrap_or_default();
                if let Some(vote) = state.vote {
                    // Answer again exactly what was answered before.
                    ctx.send(COORDINATOR, Msg::Vote(tx, vote));
                } else if state.decision.is_some() {
                    // Already decided (aborted without our vote): a late prepare.
                    ctx.send(COORDINATOR, Msg::Vote(tx, false));
                } else {
                    let yes = ctx.random_range(0..100) >= NO_VOTE_PERCENT;
                    let kind = if yes { VOTE_YES } else { VOTE_NO };
                    if !persist(ctx, PARTICIPANT_LOG, tx, kind, self.bugs.vote_without_fsync) {
                        return;
                    }
                    self.txs.entry(tx).or_default().vote = Some(yes);
                    ctx.send(COORDINATOR, Msg::Vote(tx, yes));
                    if yes && self.bugs.abort_after_yes_timeout {
                        ctx.set_timer(GIVE_UP, Tick::GiveUp(tx));
                    }
                }
            }
            Msg::Decide(tx, decision) => {
                if self.txs.get(&tx).is_none_or(|s| s.decision.is_none()) {
                    let kind = if decision == Decision::Commit { COMMITTED } else { ABORTED };
                    if !persist(ctx, PARTICIPANT_LOG, tx, kind, false) {
                        return;
                    }
                    self.txs.entry(tx).or_default().decision = Some(decision);
                }
                ctx.send(COORDINATOR, Msg::Ack(tx));
            }
            Msg::Vote(..) | Msg::Ack(_) => {}
        }
    }

    fn on_timer(&mut self, ctx: &mut dyn Context<Msg, Tick>, tick: Tick) {
        let Tick::GiveUp(tx) = tick else { return };
        let state = self.txs.get(&tx).copied().unwrap_or_default();
        if state.vote == Some(true)
            && state.decision.is_none()
            && persist(ctx, PARTICIPANT_LOG, tx, ABORTED, false)
        {
            self.txs.entry(tx).or_default().decision = Some(Decision::Abort);
        }
    }
}
