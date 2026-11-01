use std::collections::BTreeMap;

use raft::{Applied, NotLeader, Raft, RaftConfig, RaftMsg, RaftTimer, ReadOutcome};
use sim_api::{NodeId, SubContext, TimerId};

use crate::command::{Command, Op, OpResult, RequestId, Sessions, decode_state, encode_state};
use crate::{Bugs, KvContext, KvMsg, KvTimer, Reply};

/// A replica: Raft plus the key-value map it builds by applying committed commands in log order.
#[derive(Debug)]
pub struct Server {
    raft: Raft,
    store: BTreeMap<u8, u64>,
    /// Per client session: the last applied sequence number and its result. A retried operation
    /// gets this cached result instead of being applied twice. Built from the log, so every
    /// replica has the same table.
    sessions: Sessions,
    /// Log index → client to answer once that index is applied (only on the proposing leader).
    pending: BTreeMap<u64, (NodeId, RequestId)>,
    /// Reads waiting for Raft to confirm them (see [`Raft::read_index`]), by read id.
    reads: BTreeMap<u64, (NodeId, RequestId, u8)>,
    next_read: u64,
    bugs: Bugs,
}

/// Compact the log once this many applied entries are not yet in the snapshot.
const COMPACT_AFTER: u64 = 32;

fn raft_ctx<'a>(ctx: &'a mut KvContext<'_>) -> SubContext<'a, KvMsg, KvTimer, RaftMsg, RaftTimer> {
    SubContext::new(ctx, KvMsg::Raft, KvTimer::Raft)
}

impl Server {
    pub fn new(id: NodeId, members: Vec<NodeId>) -> Self {
        Self::with_bugs(id, members, Bugs::default())
    }

    /// A server with deliberately planted bugs, for checking that the tests catch them.
    pub fn with_bugs(id: NodeId, members: Vec<NodeId>, bugs: Bugs) -> Self {
        Server {
            raft: Raft::new(id, RaftConfig { bugs: bugs.raft, ..RaftConfig::new(members) }),
            store: BTreeMap::new(),
            sessions: BTreeMap::new(),
            pending: BTreeMap::new(),
            reads: BTreeMap::new(),
            next_read: 0,
            bugs,
        }
    }

    pub fn raft(&self) -> &Raft {
        &self.raft
    }

    pub fn store(&self) -> &BTreeMap<u8, u64> {
        &self.store
    }

    pub(crate) fn on_start(&mut self, ctx: &mut KvContext<'_>) {
        self.raft.start(&mut raft_ctx(ctx));
        self.apply_committed(ctx); // restores the snapshot, if there is one
    }

    pub(crate) fn on_raft_message(&mut self, ctx: &mut KvContext<'_>, from: NodeId, msg: RaftMsg) {
        self.maybe_compact(ctx);
        self.raft.handle_message(&mut raft_ctx(ctx), from, msg);
        self.apply_committed(ctx);
        self.answer_reads(ctx);
    }

    pub(crate) fn on_raft_timer(&mut self, ctx: &mut KvContext<'_>, id: TimerId, timer: RaftTimer) {
        self.maybe_compact(ctx);
        self.raft.handle_timer(&mut raft_ctx(ctx), id, timer);
        self.apply_committed(ctx);
        self.answer_reads(ctx);
    }

    pub(crate) fn on_request(
        &mut self,
        ctx: &mut KvContext<'_>,
        from: NodeId,
        id: RequestId,
        op: Op,
    ) {
        self.maybe_compact(ctx);
        if self.bugs.local_reads
            && let Op::Get { key } = op
        {
            // Bug: answer from this replica's own copy, which may lag behind the leader (or be a
            // deposed leader's), instead of ordering the read through the log.
            let value = self.store.get(&key).copied();
            ctx.send(from, KvMsg::Response { id, reply: Reply::Done(OpResult::Value(value)) });
            return;
        }
        if let Op::Get { key } = op {
            // Reads skip the log: Raft confirms this node still leads, then it answers from its
            // own copy (ReadIndex). A read changes nothing, so a retry needs no deduplication.
            let read = self.next_read;
            self.next_read += 1;
            match self.raft.read_index(&mut raft_ctx(ctx), read) {
                Ok(()) => {
                    self.reads.insert(read, (from, id, key));
                    self.answer_reads(ctx);
                }
                Err(NotLeader { leader }) => {
                    ctx.send(from, KvMsg::Response { id, reply: Reply::NotLeader(leader) });
                }
            }
            return;
        }
        // BUGGIFY: skip the cache and propose the retry again, so deduplication at apply time
        // has to do the work.
        let use_cache = !ctx.buggify();
        match self.sessions.get(&id.session) {
            // Already applied: answer from the cache.
            Some((seq, result)) if *seq == id.seq && use_cache => {
                ctx.send(from, KvMsg::Response { id, reply: Reply::Done(result.clone()) });
                return;
            }
            // A retry of an operation the client has since moved past.
            Some((seq, _)) if *seq > id.seq => return,
            _ => {}
        }
        match self.raft.propose(&mut raft_ctx(ctx), Command { id, op }.encode()) {
            Ok(index) => {
                self.pending.insert(index, (from, id));
                self.apply_committed(ctx);
            }
            Err(NotLeader { leader }) => {
                ctx.send(from, KvMsg::Response { id, reply: Reply::NotLeader(leader) });
            }
        }
    }

    /// Answers the reads Raft has decided (after `apply_committed`, so the store is current).
    fn answer_reads(&mut self, ctx: &mut KvContext<'_>) {
        for (read, outcome) in self.raft.take_reads() {
            let (client, id, key) = self.reads.remove(&read).expect("every read is tracked");
            let reply = match outcome {
                ReadOutcome::Ready => Reply::Done(OpResult::Value(self.store.get(&key).copied())),
                ReadOutcome::NotLeader => Reply::NotLeader(self.raft.leader()),
            };
            ctx.send(client, KvMsg::Response { id, reply });
        }
    }

    /// Compacts the log once enough applied entries pile up. Called at the start of an event,
    /// before anything new is applied, so the state handed over is exactly the state as of
    /// Raft's last applied entry (and the invariant checker has seen those entries committed).
    fn maybe_compact(&mut self, ctx: &mut KvContext<'_>) {
        let behind = self.raft.last_applied() - self.raft.snapshot_index();
        // BUGGIFY: compact after every applied entry, so snapshots get sent and installed a lot.
        if behind >= COMPACT_AFTER || (behind > 0 && ctx.buggify()) {
            let empty = Sessions::new();
            // Planted bug: leave the session table out, so retries apply twice after a restore.
            let sessions =
                if self.bugs.snapshot_without_sessions { &empty } else { &self.sessions };
            let data = encode_state(&self.store, sessions);
            self.raft.compact(&mut raft_ctx(ctx), data);
        }
    }

    fn apply_committed(&mut self, ctx: &mut KvContext<'_>) {
        for applied in self.raft.take_committed() {
            let (index, entry) = match applied {
                Applied::Snapshot { index, data } => {
                    (self.store, self.sessions) = decode_state(&data);
                    // Proposals the snapshot covers were applied elsewhere; their clients retry.
                    self.pending.retain(|&i, _| i > index);
                    continue;
                }
                Applied::Entry { index, entry } => (index, entry),
            };
            let waiting = self.pending.remove(&index);
            if entry.data.is_empty() {
                continue; // a new leader's no-op
            }
            let cmd = Command::decode(&entry.data);
            let result = match self.sessions.get(&cmd.id.session) {
                // Duplicate of an applied operation: don't apply it again.
                Some((seq, cached)) if *seq >= cmd.id.seq => {
                    (*seq == cmd.id.seq).then(|| cached.clone())
                }
                _ => {
                    let result = cmd.op.apply(&mut self.store);
                    self.sessions.insert(cmd.id.session, (cmd.id.seq, result.clone()));
                    Some(result)
                }
            };
            // Answer only if this index still holds the request we proposed there; after a
            // leadership change it may hold another, and the client will retry.
            if let (Some((client, id)), Some(result)) = (waiting, result)
                && id == cmd.id
            {
                ctx.send(client, KvMsg::Response { id, reply: Reply::Done(result) });
            }
        }
    }
}
