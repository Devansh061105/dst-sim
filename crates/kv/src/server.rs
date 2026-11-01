use std::collections::BTreeMap;

use raft::{NotLeader, Raft, RaftConfig, RaftMsg, RaftTimer};
use sim_api::{NodeId, SubContext, TimerId};

use crate::command::{Command, Op, OpResult, RequestId};
use crate::{Bugs, KvContext, KvMsg, KvTimer, Reply};

/// A replica: Raft plus the key-value map it builds by applying committed commands in log order.
#[derive(Debug)]
pub struct Server {
    raft: Raft,
    store: BTreeMap<u8, u64>,
    /// Per client session: the last applied sequence number and its result. A retried operation
    /// gets this cached result instead of being applied twice. Built from the log, so every
    /// replica has the same table.
    sessions: BTreeMap<u64, (u64, OpResult)>,
    /// Log index → client to answer once that index is applied (only on the proposing leader).
    pending: BTreeMap<u64, (NodeId, RequestId)>,
    bugs: Bugs,
}

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
            raft: Raft::new(id, RaftConfig::new(members)),
            store: BTreeMap::new(),
            sessions: BTreeMap::new(),
            pending: BTreeMap::new(),
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
    }

    pub(crate) fn on_raft_message(&mut self, ctx: &mut KvContext<'_>, from: NodeId, msg: RaftMsg) {
        self.raft.handle_message(&mut raft_ctx(ctx), from, msg);
        self.apply_committed(ctx);
    }

    pub(crate) fn on_raft_timer(&mut self, ctx: &mut KvContext<'_>, id: TimerId, timer: RaftTimer) {
        self.raft.handle_timer(&mut raft_ctx(ctx), id, timer);
        self.apply_committed(ctx);
    }

    pub(crate) fn on_request(
        &mut self,
        ctx: &mut KvContext<'_>,
        from: NodeId,
        id: RequestId,
        op: Op,
    ) {
        if self.bugs.local_reads
            && let Op::Get { key } = op
        {
            // Bug: answer from this replica's own copy, which may lag behind the leader (or be a
            // deposed leader's), instead of ordering the read through the log.
            let value = self.store.get(&key).copied();
            ctx.send(from, KvMsg::Response { id, reply: Reply::Done(OpResult::Value(value)) });
            return;
        }
        match self.sessions.get(&id.session) {
            // Already applied: answer from the cache.
            Some((seq, result)) if *seq == id.seq => {
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

    fn apply_committed(&mut self, ctx: &mut KvContext<'_>) {
        for (index, entry) in self.raft.take_committed() {
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
