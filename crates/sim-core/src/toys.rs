//! Small protocols used to test the simulator itself, and as examples of writing a [`Node`].

use std::collections::BTreeSet;
use std::time::Duration;

use sim_api::{Context, Node, NodeId, TimerId};

/// Node 0 sends `Ping(0)`, node 1 answers `Pong(0)`, node 0 sends `Ping(1)`, … for `rounds`
/// rounds. Needs at least two nodes; nodes other than 0 and 1 stay idle.
#[derive(Debug)]
pub struct PingPong {
    rounds: u32,
    completed: u32,
}

#[derive(Clone, Debug, Hash)]
pub enum PingMsg {
    Ping(u32),
    Pong(u32),
}

impl PingPong {
    pub fn new(rounds: u32) -> Self {
        PingPong { rounds, completed: 0 }
    }

    /// Round trips node 0 has seen finish.
    pub fn completed(&self) -> u32 {
        self.completed
    }
}

impl Node for PingPong {
    type Message = PingMsg;
    type Timer = ();

    fn on_start(&mut self, ctx: &mut dyn Context<PingMsg, ()>) {
        if ctx.id() == NodeId(0) && self.rounds > 0 {
            ctx.send(NodeId(1), PingMsg::Ping(0));
        }
    }

    fn on_message(&mut self, ctx: &mut dyn Context<PingMsg, ()>, from: NodeId, msg: PingMsg) {
        match msg {
            PingMsg::Ping(k) => ctx.send(from, PingMsg::Pong(k)),
            PingMsg::Pong(k) => {
                self.completed = k + 1;
                if self.completed < self.rounds {
                    ctx.send(from, PingMsg::Ping(k + 1));
                }
            }
        }
    }

    fn on_timer(&mut self, _ctx: &mut dyn Context<PingMsg, ()>, _id: TimerId, _timer: ()) {}
}

/// Push-pull gossip of node ids.
///
/// Every node starts knowing only itself. On a jittered timer it pushes everything it knows to a
/// random peer; a node that receives a set smaller than its own replies with its own (the pull).
/// A node stops gossiping once it knows every node, so a correct run ends with everyone knowing
/// everyone and no events left.
#[derive(Debug, Default)]
pub struct Gossip {
    known: BTreeSet<NodeId>,
}

#[derive(Clone, Debug, Hash)]
pub struct Rumors(pub BTreeSet<NodeId>);

#[derive(Clone, Debug, Hash)]
pub struct GossipTick;

impl Gossip {
    pub fn known(&self) -> &BTreeSet<NodeId> {
        &self.known
    }

    fn arm(ctx: &mut dyn Context<Rumors, GossipTick>) {
        let jitter = ctx.random_range(50..150);
        ctx.set_timer(Duration::from_millis(jitter), GossipTick);
    }
}

impl Node for Gossip {
    type Message = Rumors;
    type Timer = GossipTick;

    fn on_start(&mut self, ctx: &mut dyn Context<Rumors, GossipTick>) {
        self.known.insert(ctx.id());
        if ctx.nodes().len() > 1 {
            Self::arm(ctx);
        }
    }

    fn on_message(&mut self, ctx: &mut dyn Context<Rumors, GossipTick>, from: NodeId, msg: Rumors) {
        let Rumors(theirs) = msg;
        self.known.extend(&theirs);
        if self.known.len() > theirs.len() {
            ctx.send(from, Rumors(self.known.clone()));
        }
    }

    fn on_timer(&mut self, ctx: &mut dyn Context<Rumors, GossipTick>, _id: TimerId, _: GossipTick) {
        let me = ctx.id();
        let peer_count = ctx.nodes().len() as u64 - 1;
        if self.known.len() as u64 > peer_count {
            return;
        }
        let pick = ctx.random_range(0..peer_count) as usize;
        let peer = ctx.nodes().iter().copied().filter(|&n| n != me).nth(pick).expect("peer exists");
        ctx.send(peer, Rumors(self.known.clone()));
        Self::arm(ctx);
    }
}
