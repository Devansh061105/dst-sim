//! Raft consensus written only against sim-api, so it runs both in simulation and for real.
//!
//! This follows Figure 2 of the Raft paper (Ongaro & Ousterhout, 2014). A host node owns a
//! [`Raft`], forwards Raft messages and timers to it, proposes commands on the leader, and applies
//! whatever [`Raft::take_committed`] returns. Entries are opaque bytes; Raft never interprets them.
//! Raft talks to the world through a `Context<RaftMsg, RaftTimer>`; a host whose message and timer
//! types are larger enums wraps its own context in a [`sim_api::SubContext`].
//!
//! Durability follows the paper's "persist before responding": the term, the vote and the log are
//! synced before any message that depends on them is sent. If a sync fails, the node halts and is
//! restarted, because the unsynced state may already be gone.

mod check;
mod durable;
mod wire;

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::time::Duration;

use sim_api::{Context, NodeId, SyncError, TimerId};

pub use check::{Checker, Violation};
use durable::{Durable, Snapshot};

/// A log entry. A new leader appends one with empty `data` (a no-op) to commit earlier entries.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Entry {
    pub term: u64,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Hash)]
pub enum RaftMsg {
    RequestVote {
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
    Vote {
        term: u64,
        granted: bool,
    },
    AppendEntries {
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
        /// The leader's latest read round (see [`Raft::read_index`]); echoed back in the result.
        read_round: u64,
    },
    /// On success, `index` is the last index now known to match the leader's log; on failure, it
    /// is the index the leader should try sending from next.
    AppendResult {
        term: u64,
        success: bool,
        index: u64,
        read_round: u64,
    },
    /// Sent instead of AppendEntries when the entries a follower needs are already compacted
    /// into the leader's snapshot. Answered with an AppendResult.
    InstallSnapshot {
        term: u64,
        /// The last entry the snapshot covers, and its term.
        index: u64,
        last_term: u64,
        data: Vec<u8>,
        read_round: u64,
    },
}

/// Something the host must apply, from [`Raft::take_committed`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Applied {
    /// A committed entry, in log order.
    Entry { index: u64, entry: Entry },
    /// Replace the whole state machine with this snapshot (made by the host's
    /// [`Raft::compact`], here or on the leader), which covers every entry up to `index`.
    Snapshot { index: u64, data: Vec<u8> },
}

/// What became of a read registered with [`Raft::read_index`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadOutcome {
    /// Answer it from the state machine now: everything committed before the read arrived has
    /// been applied, and this node was still the leader after the read arrived.
    Ready,
    /// This node lost its leadership first; the client should try again elsewhere.
    NotLeader,
}

/// A read waiting for its leadership check.
#[derive(Clone, Copy, Debug)]
struct PendingRead {
    id: u64,
    term: u64,
    /// Ready once a majority has answered a message of this read round or a later one.
    round: u64,
}

impl RaftMsg {
    fn term(&self) -> u64 {
        match self {
            RaftMsg::RequestVote { term, .. }
            | RaftMsg::Vote { term, .. }
            | RaftMsg::AppendEntries { term, .. }
            | RaftMsg::AppendResult { term, .. }
            | RaftMsg::InstallSnapshot { term, .. } => *term,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RaftTimer {
    Election,
    Heartbeat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug)]
pub struct RaftConfig {
    /// Every server in the cluster, including this one.
    pub members: Vec<NodeId>,
    /// A follower that hears nothing from a leader for a random time in this range starts an
    /// election. The randomness makes repeated split votes unlikely.
    pub election_timeout: Range<Duration>,
    pub heartbeat: Duration,
    /// Most entries per AppendEntries message.
    pub max_batch: usize,
    pub bugs: Bugs,
}

impl RaftConfig {
    pub fn new(members: Vec<NodeId>) -> Self {
        RaftConfig {
            members,
            election_timeout: Duration::from_millis(150)..Duration::from_millis(300),
            heartbeat: Duration::from_millis(50),
            max_batch: 64,
            bugs: Bugs::default(),
        }
    }
}

/// Deliberately planted bugs, each a real mistake found in Raft implementations. They exist to
/// prove the tests catch them; all are off by default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Bugs {
    /// Grant votes without checking that the candidate's log is at least as up to date.
    pub vote_without_log_check: bool,
    /// Record a vote in memory without persisting it, so a restart can vote again in the term.
    pub forget_vote: bool,
    /// Truncate at the incoming entries even when they match, so a stale, reordered
    /// AppendEntries erases newer entries.
    pub truncate_without_conflict: bool,
    /// Follower commits up to the leader's commit index bounded by its own log end, instead of by
    /// the last entry this message verified.
    pub commit_beyond_match: bool,
    /// Acknowledge appended entries without syncing them.
    pub skip_append_fsync: bool,
    /// Leader commits entries from earlier terms by counting replicas (Figure 8 of the paper).
    pub commit_old_terms: bool,
    /// Truncate and append under a single sync, so a crash can keep the appends but not the
    /// truncate.
    pub single_sync_truncate: bool,
    /// Treat a damaged log like a torn tail: drop the bad part and keep voting, although entries
    /// this node acknowledged may be gone.
    pub trust_damaged_log: bool,
    /// Serve reads without first confirming leadership with a majority, so a deposed leader that
    /// hasn't noticed yet answers from stale state.
    pub read_without_quorum: bool,
}

/// Returned by [`Raft::propose`] on a node that is not the leader, with its best guess at who is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotLeader {
    pub leader: Option<NodeId>,
}

pub type RaftContext<'a> = dyn Context<RaftMsg, RaftTimer> + 'a;

#[derive(Debug)]
pub struct Raft {
    id: NodeId,
    config: RaftConfig,
    durable: Durable,
    commit_index: u64,
    last_applied: u64,
    role: Role,
    leader: Option<NodeId>,
    votes: BTreeSet<NodeId>,
    /// Leader only: the next log index to send each peer.
    next_index: BTreeMap<NodeId, u64>,
    /// Leader only: the highest log index known to be replicated on each peer.
    match_index: BTreeMap<NodeId, u64>,
    election_timer: Option<TimerId>,
    heartbeat_timer: Option<TimerId>,
    /// Leader only: bumped for every read, carried by every AppendEntries and echoed back.
    read_round: u64,
    /// Leader only: the latest read round each peer has answered in this term.
    acked_rounds: BTreeMap<NodeId, u64>,
    reads: Vec<PendingRead>,
}

impl Raft {
    pub fn new(id: NodeId, config: RaftConfig) -> Self {
        assert!(config.members.contains(&id), "{id} is not a member of its own cluster");
        Raft {
            id,
            config,
            durable: Durable::default(),
            commit_index: 0,
            last_applied: 0,
            role: Role::Follower,
            leader: None,
            votes: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            election_timer: None,
            heartbeat_timer: None,
            read_round: 0,
            acked_rounds: BTreeMap::new(),
            reads: Vec::new(),
        }
    }

    /// Recovers the term, vote and log from disk and starts the election timer. Call from the
    /// host's `on_start`, which also runs after every restart.
    pub fn start(&mut self, ctx: &mut RaftContext<'_>) {
        match Durable::recover(ctx.storage(), self.config.bugs.trust_damaged_log) {
            Ok(durable) => self.durable = durable,
            Err(SyncError) => return ctx.halt(),
        }
        // A snapshot only ever holds committed entries; the host restores it first.
        self.commit_index = self.durable.snapshot.index;
        self.reset_election_timer(ctx);
    }

    pub fn handle_message(&mut self, ctx: &mut RaftContext<'_>, from: NodeId, msg: RaftMsg) {
        if self.on_message(ctx, from, msg).is_err() {
            ctx.halt();
        }
    }

    pub fn handle_timer(&mut self, ctx: &mut RaftContext<'_>, id: TimerId, timer: RaftTimer) {
        if self.on_timer(ctx, id, timer).is_err() {
            ctx.halt();
        }
    }

    /// Appends `data` to the log if this node is the leader, returning its index. The entry is
    /// applied (on every node) once [`take_committed`](Raft::take_committed) returns it.
    pub fn propose(&mut self, ctx: &mut RaftContext<'_>, data: Vec<u8>) -> Result<u64, NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader { leader: self.leader });
        }
        let entry = Entry { term: self.durable.term, data };
        if self.durable.append(ctx.storage(), &[entry], self.sync_appends()).is_err() {
            ctx.halt();
            return Err(NotLeader { leader: None });
        }
        self.advance_commit();
        // BUGGIFY: leave it to the next heartbeat instead of replicating right away.
        if !ctx.buggify() {
            self.broadcast_append(ctx);
        }
        Ok(self.last_index())
    }

    /// What was committed since the last call, in log order: a snapshot first if the host's state
    /// is older than this node's snapshot (after a restart, or one installed from the leader).
    pub fn take_committed(&mut self) -> Vec<Applied> {
        let mut applied = Vec::new();
        let snapshot = &self.durable.snapshot;
        if self.last_applied < snapshot.index {
            applied.push(Applied::Snapshot { index: snapshot.index, data: snapshot.data.clone() });
            self.last_applied = snapshot.index;
        }
        while self.last_applied < self.commit_index {
            // (A planted bug can leave the commit index past the log; the checker reports it.)
            let Some(entry) = self.durable.entry(self.last_applied + 1) else { break };
            self.last_applied += 1;
            applied.push(Applied::Entry { index: self.last_applied, entry: entry.clone() });
        }
        applied
    }

    /// Replaces every applied entry with `data`, the host's state machine as of
    /// [`last_applied`](Raft::last_applied), so the log stops growing without bound. Call it before
    /// applying anything new in the current event, so `data` matches exactly.
    pub fn compact(&mut self, ctx: &mut RaftContext<'_>, data: Vec<u8>) {
        let index = self.last_applied;
        let Some(term) = self.term_at(index).filter(|_| index > self.durable.snapshot.index) else {
            return;
        };
        let snapshot = Snapshot { index, term, data };
        if self.durable.install_snapshot(ctx.storage(), snapshot, true).is_err() {
            ctx.halt();
        }
    }

    /// The last entry the host has been given to apply.
    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }

    /// The last entry covered by this node's snapshot (0 if none).
    pub fn snapshot_index(&self) -> u64 {
        self.durable.snapshot.index
    }

    /// The term of that entry.
    pub fn snapshot_term(&self) -> u64 {
        self.durable.snapshot.term
    }

    /// The entry at `index`, unless it is compacted into the snapshot (or past the log's end).
    pub fn entry(&self, index: u64) -> Option<&Entry> {
        self.durable.entry(index)
    }

    /// Registers a linearizable read on the leader, without writing to the log (the ReadIndex
    /// scheme of the Raft thesis, §6.4). [`take_reads`](Raft::take_reads) later reports it ready
    /// once both hold:
    ///
    /// - this node is still the leader: a majority answered a message sent after the read
    ///   arrived, still in this node's term, so no newer leader can have committed anything yet;
    /// - every entry committed before the read arrived has been applied. A leader only knows its
    ///   commit index is up to date once it has committed an entry of its own term (its no-op).
    ///
    /// `id` is the host's, for matching outcomes to requests.
    pub fn read_index(&mut self, ctx: &mut RaftContext<'_>, id: u64) -> Result<(), NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader { leader: self.leader });
        }
        self.read_round += 1;
        self.reads.push(PendingRead { id, term: self.durable.term, round: self.read_round });
        // BUGGIFY: confirm at the next heartbeat instead of right away.
        if !ctx.buggify() {
            self.broadcast_append(ctx);
        }
        Ok(())
    }

    /// Reads registered with [`read_index`](Raft::read_index) that are now decided. Call after
    /// applying [`take_committed`](Raft::take_committed)'s entries.
    pub fn take_reads(&mut self) -> Vec<(u64, ReadOutcome)> {
        let leader = self.role == Role::Leader;
        let term = self.durable.term;
        // (`term_at` is `None` if a planted bug pushed the commit index past the log; the
        // checker reports that, and reads must not crash first.)
        let caught_up =
            self.term_at(self.commit_index) == Some(term) && self.last_applied == self.commit_index;
        let confirmed = self.confirmed_round();
        let mut decided = Vec::new();
        self.reads.retain(|read| {
            let outcome = if !leader || read.term != term {
                ReadOutcome::NotLeader
            } else if caught_up && read.round <= confirmed {
                ReadOutcome::Ready
            } else {
                return true; // still waiting
            };
            decided.push((read.id, outcome));
            false
        });
        decided
    }

    /// The latest read round a majority (this leader included) has answered.
    fn confirmed_round(&self) -> u64 {
        if self.config.bugs.read_without_quorum {
            return self.read_round;
        }
        let mut rounds: Vec<u64> =
            self.peers().iter().map(|p| self.acked_rounds.get(p).copied().unwrap_or(0)).collect();
        rounds.push(self.read_round);
        rounds.sort_unstable_by(|a, b| b.cmp(a));
        rounds[self.majority() - 1]
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn term(&self) -> u64 {
        self.durable.term
    }

    /// Who this node believes is the leader of its current term.
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    /// The entries after the snapshot (entry `snapshot_index() + 1` first).
    pub fn log(&self) -> &[Entry] {
        &self.durable.log
    }

    /// The index of the last entry, compacted or not.
    pub fn last_log_index(&self) -> u64 {
        self.last_index()
    }

    /// Whether this node lost log entries to disk damage and is catching up before it may vote.
    pub fn is_rejoining(&self) -> bool {
        self.durable.rejoin_after.is_some()
    }

    fn on_message(
        &mut self,
        ctx: &mut RaftContext<'_>,
        from: NodeId,
        msg: RaftMsg,
    ) -> Result<(), SyncError> {
        // Any message from a later term means this node is behind: adopt the term as a follower.
        if msg.term() > self.durable.term {
            self.step_down(ctx, msg.term())?;
        }
        match msg {
            RaftMsg::RequestVote { term, last_log_index, last_log_term } => {
                // The election restriction: only vote for a candidate whose log is at least as
                // up to date, so whoever wins already holds every committed entry.
                let up_to_date = self.config.bugs.vote_without_log_check
                    || (last_log_term, last_log_index) >= (self.last_term(), self.last_index());
                let free = self.durable.voted_for.is_none_or(|v| v == from);
                // A node that lost acknowledged entries to disk damage doesn't vote at all until
                // it has caught up (see `is_rejoining`).
                let granted =
                    term == self.durable.term && free && up_to_date && !self.is_rejoining();
                if granted {
                    if self.durable.voted_for != Some(from) {
                        self.durable.voted_for = Some(from);
                        if !self.config.bugs.forget_vote {
                            self.durable.save_meta(ctx.storage())?;
                        }
                    }
                    self.reset_election_timer(ctx);
                }
                ctx.send(from, RaftMsg::Vote { term: self.durable.term, granted });
            }
            RaftMsg::Vote { term, granted } => {
                if self.role == Role::Candidate && term == self.durable.term && granted {
                    self.votes.insert(from);
                    if self.votes.len() >= self.majority() {
                        self.become_leader(ctx)?;
                    }
                }
            }
            RaftMsg::AppendEntries {
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_round,
            } => {
                self.on_append_entries(
                    ctx,
                    from,
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    read_round,
                )?;
            }
            RaftMsg::InstallSnapshot { term, index, last_term, data, read_round } => {
                let snapshot = Snapshot { index, term: last_term, data };
                self.on_install_snapshot(ctx, from, term, snapshot, read_round)?;
            }
            RaftMsg::AppendResult { term, success, index, read_round } => {
                if self.role != Role::Leader || term != self.durable.term {
                    return Ok(());
                }
                // Any answer in this term, success or not, shows the peer still followed this
                // leader when it replied: that is what confirms reads.
                let acked = self.acked_rounds.get_mut(&from).expect("peer has a read round");
                *acked = (*acked).max(read_round);
                let matched = self.match_index[&from];
                let next = self.next_index.get_mut(&from).expect("peer has a next index");
                if success {
                    // Replies can arrive late or twice, so only ever move forward.
                    let matched = matched.max(index);
                    self.match_index.insert(from, matched);
                    *next = (*next).max(matched + 1);
                    self.advance_commit();
                    if self.next_index[&from] <= self.last_index() {
                        self.send_append(ctx, from);
                    }
                } else {
                    // BUGGIFY: ignore the hint and back off one entry at a time.
                    let hint = if ctx.buggify() { next.saturating_sub(1) } else { index };
                    // A follower whose disk was damaged can lose entries it acknowledged; then
                    // its hint falls at or below what it once matched. Believe it, or the leader
                    // keeps resending from past the follower's log forever. Lowering a match
                    // index never commits anything, so this is always safe.
                    let matched = matched.min(hint.saturating_sub(1));
                    self.match_index.insert(from, matched);
                    let next = self.next_index.get_mut(&from).expect("peer has a next index");
                    *next = (*next).min(hint).max(matched + 1);
                    self.send_append(ctx, from);
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append_entries(
        &mut self,
        ctx: &mut RaftContext<'_>,
        from: NodeId,
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
        read_round: u64,
    ) -> Result<(), SyncError> {
        let reply = |ctx: &mut RaftContext<'_>, term, success, index| {
            ctx.send(from, RaftMsg::AppendResult { term, success, index, read_round });
        };
        if term < self.durable.term {
            self.reply_stale(ctx, from);
            return Ok(());
        }
        self.follow(ctx, from);

        // Entries up to the snapshot are committed, so they match the leader's: skip them.
        let (mut prev_log_index, mut prev_log_term, mut entries) =
            (prev_log_index, prev_log_term, entries);
        let base = self.durable.snapshot.index;
        if prev_log_index < base {
            let skip = ((base - prev_log_index) as usize).min(entries.len());
            let count = entries.len() as u64;
            entries.drain(..skip);
            if entries.is_empty() {
                reply(ctx, self.durable.term, true, prev_log_index + count);
                return Ok(());
            }
            (prev_log_index, prev_log_term) = (base, self.durable.snapshot.term);
        }

        // The consistency check: we must hold the entry just before the new ones.
        if prev_log_index > self.last_index() {
            reply(ctx, self.durable.term, false, self.last_index() + 1);
            return Ok(());
        }
        let conflict_term = self.term_at(prev_log_index);
        if conflict_term != Some(prev_log_term) {
            // Hint: skip back over the whole conflicting term in one round trip.
            let mut hint = prev_log_index;
            while hint > base + 1 && self.term_at(hint - 1) == conflict_term {
                hint -= 1;
            }
            reply(ctx, self.durable.term, false, hint);
            return Ok(());
        }

        let sync_truncate = !self.config.bugs.single_sync_truncate;
        if self.config.bugs.truncate_without_conflict
            && !entries.is_empty()
            && prev_log_index < self.last_index()
        {
            self.durable.truncate_from(ctx.storage(), prev_log_index + 1, sync_truncate)?;
        }

        // Skip entries we already have. Truncate only on a real conflict (same index, different
        // term): a stale, reordered AppendEntries must never erase newer entries.
        let count = entries.len() as u64;
        let mut first_new = entries.len();
        for (offset, entry) in entries.iter().enumerate() {
            let index = prev_log_index + 1 + offset as u64;
            if index > self.last_index() {
                first_new = offset;
                break;
            }
            if self.term_at(index) != Some(entry.term) {
                self.durable.truncate_from(ctx.storage(), index, sync_truncate)?;
                first_new = offset;
                break;
            }
        }
        if first_new < entries.len() {
            self.durable.append(ctx.storage(), &entries[first_new..], self.sync_appends())?;
        }
        self.maybe_end_rejoining(ctx)?;

        // Commit up to what the leader has committed, but never past the entries we just checked.
        let last_new = prev_log_index + count;
        let bound = if self.config.bugs.commit_beyond_match { self.last_index() } else { last_new };
        self.commit_index = self.commit_index.max(leader_commit.min(bound));
        reply(ctx, self.durable.term, true, last_new);
        Ok(())
    }

    fn on_install_snapshot(
        &mut self,
        ctx: &mut RaftContext<'_>,
        from: NodeId,
        term: u64,
        snapshot: Snapshot,
        read_round: u64,
    ) -> Result<(), SyncError> {
        if term < self.durable.term {
            self.reply_stale(ctx, from);
            return Ok(());
        }
        self.follow(ctx, from);
        let index = snapshot.index;
        // Anything committed here already matches the leader's log; only install what is newer.
        if index > self.commit_index {
            // Keep the entries after the snapshot if the log agrees on its last one (Figure 13).
            let keep_tail = self.term_at(index) == Some(snapshot.term);
            self.durable.install_snapshot(ctx.storage(), snapshot, keep_tail)?;
            self.commit_index = index;
            self.maybe_end_rejoining(ctx)?;
        }
        let result = RaftMsg::AppendResult { term, success: true, index, read_round };
        ctx.send(from, result);
        Ok(())
    }

    /// Answers a message from an older term with our newer term. No read round: a stale
    /// message's round (maybe from before its sender restarted) must never confirm a read.
    fn reply_stale(&self, ctx: &mut RaftContext<'_>, to: NodeId) {
        let term = self.durable.term;
        ctx.send(to, RaftMsg::AppendResult { term, success: false, index: 0, read_round: 0 });
    }

    /// `leader` sent us AppendEntries or a snapshot for our term: it is the leader.
    fn follow(&mut self, ctx: &mut RaftContext<'_>, leader: NodeId) {
        if self.role != Role::Follower {
            self.role = Role::Follower;
            self.votes.clear();
            if let Some(t) = self.heartbeat_timer.take() {
                ctx.cancel_timer(t);
            }
        }
        self.leader = Some(leader);
        self.reset_election_timer(ctx);
    }

    /// A rejoining node votes again once it holds an entry from a term after the one it lost data
    /// in. That entry's leader won without its vote, so (Leader Completeness, with this node left
    /// out of every majority) it held every committed entry, and by Log Matching this node now
    /// holds them too.
    fn maybe_end_rejoining(&mut self, ctx: &mut RaftContext<'_>) -> Result<(), SyncError> {
        if self.durable.rejoin_after.is_some_and(|t| self.last_term() > t) {
            self.durable.rejoin_after = None;
            self.durable.save_meta(ctx.storage())?;
        }
        Ok(())
    }

    fn on_timer(
        &mut self,
        ctx: &mut RaftContext<'_>,
        id: TimerId,
        timer: RaftTimer,
    ) -> Result<(), SyncError> {
        match timer {
            RaftTimer::Election if self.election_timer == Some(id) => {
                self.election_timer = None;
                if self.is_rejoining() {
                    // Its own vote would count, so it may not campaign either: just keep waiting
                    // for a leader.
                    self.reset_election_timer(ctx);
                } else {
                    self.start_election(ctx)?;
                }
            }
            RaftTimer::Heartbeat if self.heartbeat_timer == Some(id) => {
                self.heartbeat_timer = None;
                if self.role == Role::Leader {
                    // BUGGIFY: skip a round, so followers may time out and start an election.
                    if !ctx.buggify() {
                        self.broadcast_append(ctx);
                    }
                    self.heartbeat_timer =
                        Some(ctx.set_timer(self.config.heartbeat, RaftTimer::Heartbeat));
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn start_election(&mut self, ctx: &mut RaftContext<'_>) -> Result<(), SyncError> {
        self.durable.term += 1;
        self.durable.voted_for = Some(self.id);
        self.durable.save_meta(ctx.storage())?;
        self.role = Role::Candidate;
        self.leader = None;
        self.votes = BTreeSet::from([self.id]);
        self.reset_election_timer(ctx);
        if self.votes.len() >= self.majority() {
            return self.become_leader(ctx);
        }
        let request = RaftMsg::RequestVote {
            term: self.durable.term,
            last_log_index: self.last_index(),
            last_log_term: self.last_term(),
        };
        for peer in self.peers() {
            ctx.send(peer, request.clone());
        }
        Ok(())
    }

    fn become_leader(&mut self, ctx: &mut RaftContext<'_>) -> Result<(), SyncError> {
        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.votes.clear();
        if let Some(t) = self.election_timer.take() {
            ctx.cancel_timer(t);
        }
        let next = self.last_index() + 1;
        for peer in self.peers() {
            self.next_index.insert(peer, next);
            self.match_index.insert(peer, 0);
            self.acked_rounds.insert(peer, 0);
        }
        // A no-op from this term: committing it also commits every earlier entry (Raft §5.4.2).
        let noop = Entry { term: self.durable.term, data: Vec::new() };
        self.durable.append(ctx.storage(), &[noop], self.sync_appends())?;
        self.advance_commit();
        self.broadcast_append(ctx);
        if let Some(t) = self.heartbeat_timer.take() {
            ctx.cancel_timer(t);
        }
        self.heartbeat_timer = Some(ctx.set_timer(self.config.heartbeat, RaftTimer::Heartbeat));
        Ok(())
    }

    fn step_down(&mut self, ctx: &mut RaftContext<'_>, term: u64) -> Result<(), SyncError> {
        let was_leader = self.role == Role::Leader;
        self.durable.term = term;
        self.durable.voted_for = None;
        self.durable.save_meta(ctx.storage())?;
        self.role = Role::Follower;
        self.leader = None;
        self.votes.clear();
        if was_leader {
            if let Some(t) = self.heartbeat_timer.take() {
                ctx.cancel_timer(t);
            }
            self.reset_election_timer(ctx);
        }
        Ok(())
    }

    fn broadcast_append(&self, ctx: &mut RaftContext<'_>) {
        for peer in self.peers() {
            self.send_append(ctx, peer);
        }
    }

    fn send_append(&self, ctx: &mut RaftContext<'_>, peer: NodeId) {
        let prev = self.next_index[&peer] - 1;
        let snapshot = &self.durable.snapshot;
        // The entries this peer needs next are compacted away: send the snapshot instead.
        let Some(prev_log_term) = self.term_at(prev) else {
            let msg = RaftMsg::InstallSnapshot {
                term: self.durable.term,
                index: snapshot.index,
                last_term: snapshot.term,
                data: snapshot.data.clone(),
                read_round: self.read_round,
            };
            return ctx.send(peer, msg);
        };
        // BUGGIFY: one entry at a time, the slowest way to catch a follower up.
        let batch = if ctx.buggify() { 1 } else { self.config.max_batch as u64 };
        let last = (prev + batch).min(self.last_index());
        let entries = (prev + 1..=last).filter_map(|i| self.durable.entry(i).cloned()).collect();
        ctx.send(
            peer,
            RaftMsg::AppendEntries {
                term: self.durable.term,
                prev_log_index: prev,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
                read_round: self.read_round,
            },
        );
    }

    /// Leader: commit the highest index stored on a majority, if it is from the current term.
    /// Entries from earlier terms are never committed by counting replicas (Figure 8 of the paper
    /// shows how that loses data); they commit along with a later current-term entry.
    fn advance_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let mut matched: Vec<u64> = self.match_index.values().copied().collect();
        matched.push(self.last_index());
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let candidate = matched[self.majority() - 1];
        let current_term = self.term_at(candidate) == Some(self.durable.term);
        if candidate > self.commit_index && (current_term || self.config.bugs.commit_old_terms) {
            self.commit_index = candidate;
        }
    }

    fn reset_election_timer(&mut self, ctx: &mut RaftContext<'_>) {
        if let Some(t) = self.election_timer.take() {
            ctx.cancel_timer(t);
        }
        let range = &self.config.election_timeout;
        let nanos = ctx.random_range(range.start.as_nanos() as u64..range.end.as_nanos() as u64);
        // BUGGIFY: the shortest timeout, so candidates collide and split votes more often.
        let timeout = if ctx.buggify() { range.start } else { Duration::from_nanos(nanos) };
        self.election_timer = Some(ctx.set_timer(timeout, RaftTimer::Election));
    }

    /// Whether log appends are synced before being acknowledged (always, unless a planted bug).
    fn sync_appends(&self) -> bool {
        !self.config.bugs.skip_append_fsync
    }

    fn peers(&self) -> Vec<NodeId> {
        self.config.members.iter().copied().filter(|&m| m != self.id).collect()
    }

    fn majority(&self) -> usize {
        self.config.members.len() / 2 + 1
    }

    fn last_index(&self) -> u64 {
        self.durable.last_index()
    }

    fn last_term(&self) -> u64 {
        self.term_at(self.last_index()).expect("the last entry is in the log or the snapshot")
    }

    /// Term of the entry at `index` (1-based): the snapshot's last entry counts (index 0 being an
    /// empty snapshot, term 0), entries compacted before it don't.
    fn term_at(&self, index: u64) -> Option<u64> {
        let snapshot = &self.durable.snapshot;
        if index == snapshot.index {
            Some(snapshot.term)
        } else {
            self.durable.entry(index).map(|e| e.term)
        }
    }
}
