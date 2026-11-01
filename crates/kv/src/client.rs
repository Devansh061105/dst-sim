use std::collections::BTreeMap;
use std::time::Duration;

use sim_api::{NodeId, TimerId};

use crate::command::{Op, OpResult, RequestId};
use crate::{KvContext, KvMsg, KvTimer, Reply};

/// How long to wait for an answer before trying the next server.
const RETRY: Duration = Duration::from_millis(200);
/// How long to wait before following a `NotLeader` hint.
const REDIRECT: Duration = Duration::from_millis(10);
/// BUGGIFY: an impatient retry, which floods servers with duplicates.
const IMPATIENT_RETRY: Duration = Duration::from_millis(20);

/// What a client did, in order. Phase 5's linearizability checker consumes this history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientEvent {
    Invoke { id: RequestId, op: Op },
    Complete { id: RequestId, result: OpResult },
}

/// Performs a fixed number of random operations, one at a time, retrying each until it completes.
#[derive(Debug)]
pub struct Client {
    servers: Vec<NodeId>,
    ops_total: u32,
    session: u64,
    seq: u64,
    current: Option<Op>,
    completed: u32,
    /// Index into `servers` of where requests go.
    target: usize,
    /// The next retry goes to `target` as-is (it came from a hint) instead of the next server.
    redirected: bool,
    /// Hints followed for the current operation.
    hints: u32,
    /// After this many hints for one operation, ignore hints and try servers in turn.
    max_hints: u32,
    retry_timer: Option<TimerId>,
    /// Latest value this client knows per key, used to pick plausible `Cas` expectations.
    last_seen: BTreeMap<u8, u64>,
    events: Vec<ClientEvent>,
}

impl Client {
    pub fn new(servers: Vec<NodeId>, ops_total: u32) -> Self {
        Client {
            servers,
            ops_total,
            session: 0,
            seq: 0,
            current: None,
            completed: 0,
            target: 0,
            redirected: false,
            hints: 0,
            max_hints: u32::MAX,
            retry_timer: None,
            last_seen: BTreeMap::new(),
            events: Vec::new(),
        }
    }

    /// Follows at most `max` leader hints per operation, then tries servers in turn. Needed
    /// when servers are removed from the cluster: they keep stale views and can point at each
    /// other in a circle. Off by default, which keeps every run without membership changes
    /// exactly as it was (such runs can also meet a hint cycle, but they always escape it).
    pub fn with_hint_limit(self, max: u32) -> Self {
        Client { max_hints: max, ..self }
    }

    pub fn is_done(&self) -> bool {
        self.completed == self.ops_total
    }

    pub fn completed(&self) -> u32 {
        self.completed
    }

    /// Takes the events recorded since the last call.
    pub fn drain_events(&mut self) -> Vec<ClientEvent> {
        std::mem::take(&mut self.events)
    }

    pub(crate) fn on_start(&mut self, ctx: &mut KvContext<'_>) {
        // A fresh session after every (re)start, so new sequence numbers can't collide with the
        // ones a previous incarnation used.
        self.session = ctx.random_u64();
        self.target = ctx.random_range(0..self.servers.len() as u64) as usize;
        self.next_op(ctx);
    }

    pub(crate) fn on_response(&mut self, ctx: &mut KvContext<'_>, id: RequestId, reply: Reply) {
        if self.current.is_none() || id != self.request_id() {
            return; // a late or duplicate answer to an earlier operation
        }
        match reply {
            Reply::Done(result) => {
                let op = self.current.take().expect("an operation is in flight");
                self.remember(&op, &result);
                self.events.push(ClientEvent::Complete { id, result });
                self.completed += 1;
                if let Some(t) = self.retry_timer.take() {
                    ctx.cancel_timer(t);
                }
                self.next_op(ctx);
            }
            Reply::NotLeader(hint) => {
                // See `with_hint_limit`.
                let hint = hint.filter(|_| self.hints < self.max_hints);
                if let Some(i) = hint.and_then(|h| self.servers.iter().position(|&s| s == h)) {
                    self.target = i;
                    self.redirected = true;
                    self.hints += 1;
                    self.arm_retry(ctx, REDIRECT);
                }
                // Without a hint, the pending retry timer moves on to the next server.
            }
        }
    }

    pub(crate) fn on_retry(&mut self, ctx: &mut KvContext<'_>, id: TimerId) {
        if self.retry_timer != Some(id) || self.current.is_none() {
            return;
        }
        self.retry_timer = None;
        if !std::mem::take(&mut self.redirected) {
            self.target = (self.target + 1) % self.servers.len();
        }
        self.send(ctx);
    }

    fn next_op(&mut self, ctx: &mut KvContext<'_>) {
        self.hints = 0;
        if self.is_done() {
            return;
        }
        self.seq += 1;
        let op = self.random_op(ctx);
        self.events.push(ClientEvent::Invoke { id: self.request_id(), op: op.clone() });
        self.current = Some(op);
        self.send(ctx);
    }

    fn send(&mut self, ctx: &mut KvContext<'_>) {
        let op = self.current.clone().expect("an operation is in flight");
        ctx.send(self.servers[self.target], KvMsg::Request { id: self.request_id(), op });
        let retry = if ctx.buggify() { IMPATIENT_RETRY } else { RETRY };
        self.arm_retry(ctx, retry);
    }

    fn arm_retry(&mut self, ctx: &mut KvContext<'_>, after: Duration) {
        if let Some(t) = self.retry_timer.take() {
            ctx.cancel_timer(t);
        }
        self.retry_timer = Some(ctx.set_timer(after, KvTimer::Retry));
    }

    fn random_op(&self, ctx: &mut KvContext<'_>) -> Op {
        // Three keys, so clients contend; random values, so writes are distinguishable.
        let key = ctx.random_range(0..3) as u8;
        let value = ctx.random_u64() >> 40;
        match ctx.random_range(0..3) {
            0 => Op::Get { key },
            1 => Op::Put { key, value },
            _ => Op::Cas { key, expected: self.last_seen.get(&key).copied(), new: value },
        }
    }

    fn remember(&mut self, op: &Op, result: &OpResult) {
        match (op, result) {
            (Op::Get { key }, OpResult::Value(Some(v))) => {
                self.last_seen.insert(*key, *v);
            }
            (Op::Get { key }, OpResult::Value(None)) => {
                self.last_seen.remove(key);
            }
            (Op::Put { key, value }, _)
            | (Op::Cas { key, new: value, .. }, OpResult::Cas { swapped: true }) => {
                self.last_seen.insert(*key, *value);
            }
            _ => {}
        }
    }

    fn request_id(&self) -> RequestId {
        RequestId { session: self.session, seq: self.seq }
    }
}
