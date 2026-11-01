//! Linearizable key-value store (get/put/cas) replicated with Raft, plus a retrying client.
//!
//! A cluster is `servers` Raft servers (node ids `0..servers`) followed by clients. Writes go
//! through the Raft log; reads are answered by the leader once Raft has confirmed it still leads
//! (ReadIndex), without writing to the log. Either way the store behaves like a single machine
//! (linearizable). Clients tag each operation with a random session id and a sequence number;
//! servers remember the last result per session, so a retried write is applied exactly once.

mod client;
mod command;
mod model;
mod server;
mod wire;

pub use client::{Client, ClientEvent};
pub use command::{Op, OpResult, RequestId};
pub use model::{KvModel, Recorder};
use raft::{RaftMsg, RaftTimer};
pub use server::Server;
use sim_api::{Context, Node, NodeId, TimerId};

/// Deliberately planted bugs. Each must be caught by the tests; all are off by default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Bugs {
    /// Any server answers reads from its own copy instead of going through the Raft log.
    pub local_reads: bool,
    /// Snapshots hold the store but not the session table, so a server restored from one
    /// applies a retried write a second time.
    pub snapshot_without_sessions: bool,
    /// Bugs planted in the Raft layer underneath.
    pub raft: raft::Bugs,
}

#[derive(Clone, Debug, Hash)]
pub enum KvMsg {
    Raft(RaftMsg),
    Request { id: RequestId, op: Op },
    Response { id: RequestId, reply: Reply },
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum Reply {
    Done(OpResult),
    /// This server isn't the leader; the payload is its guess at who is.
    NotLeader(Option<NodeId>),
}

#[derive(Clone, Debug, Hash)]
pub enum KvTimer {
    Raft(RaftTimer),
    Retry,
}

pub type KvContext<'a> = dyn Context<KvMsg, KvTimer> + 'a;

/// One node of a cluster. The simulator runs a single node type, so servers and clients share it.
// A cluster has a handful of nodes, so the space a client wastes on the larger server variant is
// negligible, and boxing would add an indirection to every event.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum KvNode {
    Server(Server),
    Client(Client),
}

impl KvNode {
    pub fn as_server(&self) -> Option<&Server> {
        match self {
            KvNode::Server(s) => Some(s),
            KvNode::Client(_) => None,
        }
    }

    pub fn as_client(&self) -> Option<&Client> {
        match self {
            KvNode::Client(c) => Some(c),
            KvNode::Server(_) => None,
        }
    }

    pub fn as_client_mut(&mut self) -> Option<&mut Client> {
        match self {
            KvNode::Client(c) => Some(c),
            KvNode::Server(_) => None,
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

impl Node for KvNode {
    type Message = KvMsg;
    type Timer = KvTimer;

    fn on_start(&mut self, ctx: &mut KvContext<'_>) {
        match self {
            KvNode::Server(s) => s.on_start(ctx),
            KvNode::Client(c) => c.on_start(ctx),
        }
    }

    fn on_message(&mut self, ctx: &mut KvContext<'_>, from: NodeId, msg: KvMsg) {
        match (self, msg) {
            (KvNode::Server(s), KvMsg::Raft(m)) => s.on_raft_message(ctx, from, m),
            (KvNode::Server(s), KvMsg::Request { id, op }) => s.on_request(ctx, from, id, op),
            (KvNode::Client(c), KvMsg::Response { id, reply }) => c.on_response(ctx, id, reply),
            _ => {}
        }
    }

    fn on_timer(&mut self, ctx: &mut KvContext<'_>, id: TimerId, timer: KvTimer) {
        match (self, timer) {
            (KvNode::Server(s), KvTimer::Raft(t)) => s.on_raft_timer(ctx, id, t),
            (KvNode::Client(c), KvTimer::Retry) => c.on_retry(ctx, id),
            _ => {}
        }
    }
}
