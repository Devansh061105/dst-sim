//! Linearizable key-value store (get/put/cas) replicated with Raft, plus a retrying client.
//!
//! A cluster is `servers` Raft servers (node ids `0..servers`) followed by clients. Writes go
//! through the Raft log; reads are answered by the leader once Raft has confirmed it still leads
//! (ReadIndex), without writing to the log. Either way the store behaves like a single machine
//! (linearizable). Clients tag each operation with a random session id and a sequence number;
//! servers remember the last result per session, so a retried write is applied exactly once.

mod admin;
mod client;
mod command;
mod model;
mod server;
mod wire;

pub use admin::Admin;
pub use client::{Client, ClientEvent};
pub use command::{Op, OpResult, RequestId};
pub use model::{KvModel, Recorder};
use raft::{RaftMsg, RaftTimer};
pub use server::Server;
use sim_api::{Context, Node, NodeId, TimerId};

/// Deliberately planted bugs. Each must be caught by the tests; all are off by default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Bugs {
    /// Any server answers reads from its own copy at once, instead of only the leader answering
    /// after confirming it still leads (ReadIndex).
    pub local_reads: bool,
    /// Snapshots hold the store but not the session table, so a server restored from one
    /// applies a retried write a second time.
    pub snapshot_without_sessions: bool,
    /// Bugs planted in the Raft layer underneath.
    pub raft: raft::Bugs,
}

/// Everything sent between nodes: Raft traffic between servers, requests and replies
/// between clients and servers, and configuration changes from the admin.
// New variants go at the end: the derived hash includes the variant's position, and runs
// without membership changes must keep their trace hashes.
#[derive(Clone, Debug, Hash)]
pub enum KvMsg {
    Raft(RaftMsg),
    Request {
        id: RequestId,
        op: Op,
    },
    Response {
        id: RequestId,
        reply: Reply,
    },
    /// Admin → server: please make this change.
    Reconfigure {
        id: u64,
        change: Change,
    },
    /// Server → admin: the answer, with the configuration the server now has.
    Reconfigured {
        id: u64,
        reply: ReconfigReply,
        members: Vec<NodeId>,
    },
}

/// One configuration change: add or remove a single server.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Change {
    /// Make this server a member.
    Add(NodeId),
    /// Take this server out.
    Remove(NodeId),
}

/// A server's answer to a configuration change.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ReconfigReply {
    /// In the leader's log (or already the configuration); it commits like any entry.
    Accepted,
    /// The leader can't start a change yet (the previous one hasn't committed, or it hasn't
    /// committed in its own term yet): ask again.
    Busy,
    /// This server isn't the leader; the payload is its guess at who is.
    NotLeader(Option<NodeId>),
}

/// A server's answer to a request.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum Reply {
    Done(OpResult),
    /// This server isn't the leader; the payload is its guess at who is.
    NotLeader(Option<NodeId>),
}

/// Timers: Raft's own, and a client's retry timer.
#[derive(Clone, Debug, Hash)]
pub enum KvTimer {
    Raft(RaftTimer),
    Retry,
    /// The admin's next step.
    Admin,
}

/// The host context a key-value node runs in.
pub type KvContext<'a> = dyn Context<KvMsg, KvTimer> + 'a;

/// One node of a cluster. The simulator runs a single node type, so servers and clients share it.
// A cluster has a handful of nodes, so the space a client wastes on the larger server variant is
// negligible, and boxing would add an indirection to every event.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum KvNode {
    Server(Server),
    Client(Client),
    Admin(Admin),
}

impl KvNode {
    pub fn as_server(&self) -> Option<&Server> {
        match self {
            KvNode::Server(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_client(&self) -> Option<&Client> {
        match self {
            KvNode::Client(c) => Some(c),
            _ => None,
        }
    }

    /// The admin, if this node is it.
    pub fn as_admin(&self) -> Option<&Admin> {
        match self {
            KvNode::Admin(a) => Some(a),
            _ => None,
        }
    }

    pub fn as_client_mut(&mut self) -> Option<&mut Client> {
        match self {
            KvNode::Client(c) => Some(c),
            _ => None,
        }
    }
}

/// Node factory for a cluster of `servers` servers (ids `0..servers`); every other node id is a
/// client that performs `ops_per_client` operations.
pub fn cluster(servers: u32, ops_per_client: u32) -> impl FnMut(NodeId) -> KvNode + 'static {
    cluster_with_bugs(servers, ops_per_client, Bugs::default())
}

/// Like [`cluster`], with planted bugs in every server.
pub fn cluster_with_bugs(
    servers: u32,
    ops_per_client: u32,
    bugs: Bugs,
) -> impl FnMut(NodeId) -> KvNode + 'static {
    let members: Vec<NodeId> = (0..servers).map(NodeId).collect();
    move |id| {
        if id.0 < servers {
            KvNode::Server(Server::with_bugs(id, members.clone(), bugs))
        } else {
            KvNode::Client(Client::new(members.clone(), ops_per_client))
        }
    }
}

/// Node factory for a cluster whose membership changes while it runs: `pool` servers (ids
/// `0..pool`) of which the first `initial` start as the configuration, then an [`Admin`] (id
/// `pool`) that makes `changes` configuration changes, then clients doing `ops_per_client`
/// operations each against any server of the pool.
pub fn cluster_with_membership(
    pool: u32,
    initial: u32,
    changes: u32,
    ops_per_client: u32,
    bugs: Bugs,
) -> impl FnMut(NodeId) -> KvNode + 'static {
    let servers: Vec<NodeId> = (0..pool).map(NodeId).collect();
    let members: Vec<NodeId> = (0..initial).map(NodeId).collect();
    move |id| {
        if id.0 < pool {
            KvNode::Server(Server::with_bugs(id, members.clone(), bugs))
        } else if id.0 == pool {
            KvNode::Admin(Admin::new(servers.clone(), members.clone(), changes))
        } else {
            KvNode::Client(Client::new(servers.clone(), ops_per_client).with_hint_limit(HINTS))
        }
    }
}

/// Leader hints a client follows per operation in a cluster whose membership changes.
const HINTS: u32 = 8;

impl Node for KvNode {
    type Message = KvMsg;
    type Timer = KvTimer;

    fn on_start(&mut self, ctx: &mut KvContext<'_>) {
        match self {
            KvNode::Server(s) => s.on_start(ctx),
            KvNode::Client(c) => c.on_start(ctx),
            KvNode::Admin(a) => a.on_start(ctx),
        }
    }

    fn on_message(&mut self, ctx: &mut KvContext<'_>, from: NodeId, msg: KvMsg) {
        match (self, msg) {
            (KvNode::Server(s), KvMsg::Raft(m)) => s.on_raft_message(ctx, from, m),
            (KvNode::Server(s), KvMsg::Request { id, op }) => s.on_request(ctx, from, id, op),
            (KvNode::Client(c), KvMsg::Response { id, reply }) => c.on_response(ctx, id, reply),
            (KvNode::Server(s), KvMsg::Reconfigure { id, change }) => {
                s.on_reconfigure(ctx, from, id, change);
            }
            (KvNode::Admin(a), KvMsg::Reconfigured { id, reply, members }) => {
                a.on_reply(ctx, from, id, reply, members);
            }
            _ => {}
        }
    }

    fn on_timer(&mut self, ctx: &mut KvContext<'_>, id: TimerId, timer: KvTimer) {
        match (self, timer) {
            (KvNode::Server(s), KvTimer::Raft(t)) => s.on_raft_timer(ctx, id, t),
            (KvNode::Client(c), KvTimer::Retry) => c.on_retry(ctx, id),
            (KvNode::Admin(a), KvTimer::Admin) => a.on_timer(ctx, id),
            _ => {}
        }
    }
}
