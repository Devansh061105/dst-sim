use std::collections::BTreeSet;
use std::ops::Range;
use std::time::Duration;

use sim_api::{Context, Node, NodeId, Time, TimerId};

use crate::queue::EventQueue;
use crate::rng::{SimRng, Stream};
use crate::trace::{Entry, Trace};

/// Everything that determines a run. Same config and node code, same run.
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub nodes: u32,
    /// One-way message delay is drawn uniformly from this range.
    pub latency: Range<Duration>,
    /// Stop once the next event is later than this much virtual time.
    pub max_time: Duration,
    /// Stop after this many events: a guard against protocols that never go quiet.
    pub max_steps: u64,
    /// Keep a readable line per event (costs memory; the trace hash is always computed).
    pub keep_trace_lines: bool,
}

impl SimConfig {
    pub fn new(seed: u64, nodes: u32) -> Self {
        SimConfig {
            seed,
            nodes,
            latency: Duration::from_millis(1)..Duration::from_millis(10),
            max_time: Duration::from_secs(60),
            max_steps: 1_000_000,
            keep_trace_lines: false,
        }
    }
}

/// Why a run ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopReason {
    /// No events left: every node is idle and nothing is in flight.
    Quiescent,
    TimeLimit,
    StepLimit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunSummary {
    pub stop: StopReason,
    pub steps: u64,
    pub end_time: Time,
    pub trace_hash: u64,
}

enum Event<M, T> {
    Start { node: NodeId },
    Deliver { from: NodeId, to: NodeId, msg: M },
    Timer { node: NodeId, id: TimerId, timer: T },
}

/// What a node asked for during one callback. The simulator decides what actually happens.
enum Effect<M, T> {
    Send { to: NodeId, msg: M },
    SetTimer { id: TimerId, after: Duration, timer: T },
    CancelTimer { id: TimerId },
}

/// A whole cluster running on one thread in virtual time.
pub struct Simulation<N: Node> {
    config: SimConfig,
    deadline: Time,
    now: Time,
    steps: u64,
    ids: Vec<NodeId>,
    nodes: Vec<N>,
    node_rngs: Vec<SimRng>,
    net_rng: SimRng,
    queue: EventQueue<Event<N::Message, N::Timer>>,
    active_timers: BTreeSet<TimerId>,
    next_timer_id: u64,
    trace: Trace,
}

impl<N: Node> Simulation<N> {
    /// Builds the cluster, calling `make_node` once per node id, and schedules every node's start.
    pub fn new(config: SimConfig, mut make_node: impl FnMut(NodeId) -> N) -> Self {
        assert!(config.nodes > 0, "a simulation needs at least one node");
        let ids: Vec<NodeId> = (0..config.nodes).map(NodeId).collect();
        let nodes = ids.iter().map(|&id| make_node(id)).collect();
        let node_rngs = ids.iter().map(|&id| SimRng::new(config.seed, Stream::Node(id))).collect();
        let mut queue = EventQueue::new();
        for &node in &ids {
            queue.push(Time::ZERO, Event::Start { node });
        }
        Simulation {
            deadline: Time::ZERO + config.max_time,
            now: Time::ZERO,
            steps: 0,
            net_rng: SimRng::new(config.seed, Stream::Network),
            trace: Trace::new(config.keep_trace_lines),
            ids,
            nodes,
            node_rngs,
            queue,
            active_timers: BTreeSet::new(),
            next_timer_id: 0,
            config,
        }
    }

    /// Runs until the system goes quiet or a limit is hit.
    pub fn run(&mut self) -> RunSummary {
        let stop = loop {
            if let Some(reason) = self.step() {
                break reason;
            }
        };
        RunSummary { stop, steps: self.steps, end_time: self.now, trace_hash: self.trace.hash() }
    }

    /// Processes the next event. Returns why the run is over, or `None` if it can continue.
    pub fn step(&mut self) -> Option<StopReason> {
        if self.steps >= self.config.max_steps {
            return Some(StopReason::StepLimit);
        }
        let Some(at) = self.queue.peek_time() else {
            return Some(StopReason::Quiescent);
        };
        if at > self.deadline {
            return Some(StopReason::TimeLimit);
        }
        let (at, event) = self.queue.pop().expect("peeked event is present");

        // Cancelled timers stay in the heap (removing from the middle is expensive) and are
        // skipped here instead.
        if let Event::Timer { id, .. } = &event
            && !self.active_timers.remove(id)
        {
            return None;
        }

        self.now = at;
        self.steps += 1;
        match event {
            Event::Start { node } => {
                self.trace.record::<N::Message, N::Timer>(at, &Entry::Start { node });
                self.dispatch(node, |n, ctx| n.on_start(ctx));
            }
            Event::Deliver { from, to, msg } => {
                self.trace.record::<_, N::Timer>(at, &Entry::Deliver { from, to, msg: &msg });
                self.dispatch(to, |n, ctx| n.on_message(ctx, from, msg));
            }
            Event::Timer { node, id, timer } => {
                self.trace.record::<N::Message, _>(at, &Entry::Timer { node, id, timer: &timer });
                self.dispatch(node, |n, ctx| n.on_timer(ctx, id, timer));
            }
        }
        None
    }

    pub fn now(&self) -> Time {
        self.now
    }

    pub fn node(&self, id: NodeId) -> &N {
        &self.nodes[id.0 as usize]
    }

    pub fn nodes(&self) -> impl Iterator<Item = (NodeId, &N)> {
        self.ids.iter().copied().zip(&self.nodes)
    }

    pub fn trace(&self) -> &Trace {
        &self.trace
    }

    fn dispatch(
        &mut self,
        node: NodeId,
        callback: impl FnOnce(&mut N, &mut dyn Context<N::Message, N::Timer>),
    ) {
        let idx = node.0 as usize;
        let mut ctx = SimContext {
            id: node,
            nodes: &self.ids,
            now: self.now,
            rng: &mut self.node_rngs[idx],
            next_timer_id: &mut self.next_timer_id,
            effects: Vec::new(),
        };
        callback(&mut self.nodes[idx], &mut ctx);
        for effect in ctx.effects {
            self.apply(node, effect);
        }
    }

    fn apply(&mut self, from: NodeId, effect: Effect<N::Message, N::Timer>) {
        match effect {
            Effect::Send { to, msg } => {
                assert!(to.0 < self.config.nodes, "{from} sent to unknown node {to}");
                let latency = self.net_rng.duration_in(self.config.latency.clone());
                self.queue.push(self.now + latency, Event::Deliver { from, to, msg });
            }
            Effect::SetTimer { id, after, timer } => {
                self.active_timers.insert(id);
                self.queue.push(self.now + after, Event::Timer { node: from, id, timer });
            }
            Effect::CancelTimer { id } => {
                self.active_timers.remove(&id);
            }
        }
    }
}

/// The simulator's [`Context`]: records what the node asks for instead of doing it.
struct SimContext<'a, M, T> {
    id: NodeId,
    nodes: &'a [NodeId],
    now: Time,
    rng: &'a mut SimRng,
    next_timer_id: &'a mut u64,
    effects: Vec<Effect<M, T>>,
}

impl<M, T> Context<M, T> for SimContext<'_, M, T> {
    fn id(&self) -> NodeId {
        self.id
    }

    fn nodes(&self) -> &[NodeId] {
        self.nodes
    }

    fn now(&self) -> Time {
        self.now
    }

    fn send(&mut self, to: NodeId, msg: M) {
        self.effects.push(Effect::Send { to, msg });
    }

    fn set_timer(&mut self, after: Duration, timer: T) -> TimerId {
        let id = TimerId(*self.next_timer_id);
        *self.next_timer_id += 1;
        self.effects.push(Effect::SetTimer { id, after, timer });
        id
    }

    fn cancel_timer(&mut self, id: TimerId) {
        self.effects.push(Effect::CancelTimer { id });
    }

    fn random_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }
}
