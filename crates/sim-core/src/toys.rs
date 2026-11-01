//! Small protocols used to test the simulator itself, and as examples of writing a [`Node`].

use std::collections::BTreeSet;
use std::time::Duration;

use sim_api::{Context, Node, NodeId, TimerId, record};

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

/// Appends numbered entries to a checksummed log and acknowledges each one, the way a database
/// acknowledges a committed write. The durability property: after any crash, every acknowledged
/// entry is still on disk. Two modes reproduce real-world durability bugs.
#[derive(Debug)]
pub struct Ledger {
    mode: LedgerMode,
    appended: u64,
    acked: u64,
    recovered: u64,
    /// Set after a failed sync: the unsynced entries may be gone, so stop until a restart
    /// re-reads what is really on disk.
    poisoned: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LedgerMode {
    /// Sync before acknowledging; after a failed sync, stop writing.
    Correct,
    /// Bug: acknowledge without syncing.
    NoSync,
    /// Bug: retry a failed sync and acknowledge once the retry succeeds ("fsyncgate").
    RetrySync,
}

#[derive(Clone, Debug, Hash)]
pub struct LedgerTick;

const LEDGER_FILE: &str = "ledger";

impl Ledger {
    pub fn new(mode: LedgerMode) -> Self {
        Ledger { mode, appended: 0, acked: 0, recovered: 0, poisoned: false }
    }

    /// Entries acknowledged as durable since the last (re)start, counting recovered ones.
    pub fn acked(&self) -> u64 {
        self.acked
    }

    /// Entries found on disk at the last (re)start.
    pub fn recovered(&self) -> u64 {
        self.recovered
    }

    fn arm(ctx: &mut dyn Context<(), LedgerTick>) {
        ctx.set_timer(Duration::from_millis(10), LedgerTick);
    }
}

impl Node for Ledger {
    type Message = ();
    type Timer = LedgerTick;

    fn on_start(&mut self, ctx: &mut dyn Context<(), LedgerTick>) {
        let storage = ctx.storage();
        let bytes = storage.read(LEDGER_FILE);
        let decoded = record::decode(&bytes);
        for (i, entry) in decoded.records.iter().enumerate() {
            assert_eq!(entry.as_slice(), (i as u64).to_le_bytes(), "ledger entries out of order");
        }
        if decoded.valid_len < bytes.len() as u64 {
            // A torn or partly written tail: cut it off so new records start on a clean boundary.
            storage.truncate(LEDGER_FILE, decoded.valid_len);
            self.poisoned = storage.sync(LEDGER_FILE).is_err();
        }
        let count = decoded.records.len() as u64;
        self.recovered = count;
        self.appended = count;
        self.acked = count;
        Self::arm(ctx);
    }

    fn on_message(&mut self, _ctx: &mut dyn Context<(), LedgerTick>, _from: NodeId, _msg: ()) {}

    fn on_timer(&mut self, ctx: &mut dyn Context<(), LedgerTick>, _id: TimerId, _: LedgerTick) {
        if !self.poisoned {
            let storage = ctx.storage();
            storage.append(LEDGER_FILE, &record::encode(&self.appended.to_le_bytes()));
            self.appended += 1;
            match self.mode {
                LedgerMode::Correct => match storage.sync(LEDGER_FILE) {
                    Ok(()) => self.acked = self.appended,
                    Err(_) => self.poisoned = true,
                },
                LedgerMode::NoSync => self.acked = self.appended,
                LedgerMode::RetrySync => {
                    while storage.sync(LEDGER_FILE).is_err() {}
                    self.acked = self.appended;
                }
            }
        }
        Self::arm(ctx);
    }
}
