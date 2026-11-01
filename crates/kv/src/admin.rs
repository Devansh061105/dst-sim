use sim_api::{NodeId, TimerId};

use crate::server::MIN_MEMBERS;
use crate::{Change, KvContext, KvMsg, KvTimer, ReconfigReply};

/// How long the admin waits for an answer before asking another server.
const RETRY_MS: u64 = 100;
/// The pause between one accepted change and the next, in milliseconds.
const PAUSE_MS: std::ops::Range<u64> = 200..1000;

/// An operator that adds and removes servers, one per request, while clients run.
///
/// It asks for the next change as soon as the previous one is *accepted* (in the leader's log),
/// not once it has committed: a hasty operator. Correct Raft answers `Busy` until the previous
/// change commits; a Raft with the `concurrent_changes` bug takes it, and two changes are then in
/// flight at once.
#[derive(Debug)]
pub struct Admin {
    servers: Vec<NodeId>,
    /// Its latest view of the configuration (from the servers' answers).
    members: Vec<NodeId>,
    total: u32,
    accepted: u32,
    next_id: u64,
    /// The change being requested, and its request id.
    current: Option<(u64, Change)>,
    /// Which server to ask next (an index into `servers`).
    target: usize,
    /// Round-robin position, used when hints run out: removed servers can hold stale views and
    /// point at each other, so hints alone may go round in circles.
    cursor: usize,
    /// Leader hints followed in a row.
    hops: u32,
    /// Whether a request is out and unanswered (the timer then means: no answer came).
    asked: bool,
    timer: Option<TimerId>,
    initial: Vec<NodeId>,
}

impl Admin {
    /// An admin for the server pool `servers`, which starts with `members` as the configuration
    /// and makes `total` changes.
    pub fn new(servers: Vec<NodeId>, members: Vec<NodeId>, total: u32) -> Self {
        Admin {
            servers,
            members: members.clone(),
            total,
            accepted: 0,
            next_id: 0,
            current: None,
            target: 0,
            cursor: 0,
            hops: 0,
            asked: false,
            timer: None,
            initial: members,
        }
    }

    /// Whether every change has been accepted.
    pub fn is_done(&self) -> bool {
        self.accepted == self.total
    }

    pub(crate) fn on_start(&mut self, ctx: &mut KvContext<'_>) {
        // A restart forgets everything, like any process; start over.
        self.members = self.initial.clone();
        (self.accepted, self.current, self.target, self.asked) = (0, None, 0, false);
        let pause = ctx.random_range(PAUSE_MS);
        self.wait(ctx, pause);
    }

    pub(crate) fn on_timer(&mut self, ctx: &mut KvContext<'_>, id: TimerId) {
        if self.timer != Some(id) {
            return;
        }
        self.timer = None;
        if self.current.is_none() {
            if self.is_done() {
                return;
            }
            let change = self.choose(ctx);
            self.current = Some((self.next_id, change));
            self.next_id += 1;
        } else if self.asked {
            // No answer: try another server.
            self.next_server();
        }
        self.ask(ctx);
    }

    pub(crate) fn on_reply(
        &mut self,
        ctx: &mut KvContext<'_>,
        from: NodeId,
        id: u64,
        reply: ReconfigReply,
        members: Vec<NodeId>,
    ) {
        if self.current.is_none_or(|(current, _)| current != id) {
            return;
        }
        if let Some(t) = self.timer.take() {
            ctx.cancel_timer(t);
        }
        self.asked = false;
        self.members = members;
        match reply {
            ReconfigReply::Accepted => {
                self.accepted += 1;
                self.current = None;
                if !self.is_done() {
                    let pause = ctx.random_range(PAUSE_MS);
                    self.wait(ctx, pause);
                }
            }
            ReconfigReply::Busy => {
                self.target = self.index_of(from);
                self.hops = 0;
                self.wait(ctx, 50);
            }
            ReconfigReply::NotLeader(Some(leader)) if self.hops < 2 => {
                self.target = self.index_of(leader);
                self.hops += 1;
                self.wait(ctx, 10);
            }
            ReconfigReply::NotLeader(_) => {
                self.next_server();
                self.wait(ctx, 10);
            }
        }
    }

    /// Grow when small, shrink when everyone is in, otherwise either.
    fn choose(&self, ctx: &mut KvContext<'_>) -> Change {
        let outside: Vec<NodeId> =
            self.servers.iter().copied().filter(|s| !self.members.contains(s)).collect();
        let add = if outside.is_empty() {
            false
        } else if self.members.len() <= MIN_MEMBERS {
            true
        } else {
            ctx.random_range(0..2) == 0
        };
        if add {
            Change::Add(outside[ctx.random_range(0..outside.len() as u64) as usize])
        } else {
            Change::Remove(self.members[ctx.random_range(0..self.members.len() as u64) as usize])
        }
    }

    fn ask(&mut self, ctx: &mut KvContext<'_>) {
        let Some((id, change)) = self.current else { return };
        ctx.send(self.servers[self.target], KvMsg::Reconfigure { id, change });
        self.asked = true;
        self.timer =
            Some(ctx.set_timer(std::time::Duration::from_millis(RETRY_MS), KvTimer::Admin));
    }

    fn wait(&mut self, ctx: &mut KvContext<'_>, ms: u64) {
        self.timer = Some(ctx.set_timer(std::time::Duration::from_millis(ms), KvTimer::Admin));
    }

    fn next_server(&mut self) {
        self.cursor = (self.cursor + 1) % self.servers.len();
        self.target = self.cursor;
        self.hops = 0;
    }

    fn index_of(&self, server: NodeId) -> usize {
        self.servers.iter().position(|&s| s == server).unwrap_or(0)
    }
}
