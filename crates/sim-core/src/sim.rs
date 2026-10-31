use std::collections::BTreeMap;
use std::time::Duration;

use sim_api::{Context, Node, NodeId, Rand, Time, TimerId};
use sim_net::{NetConfig, Network, Route};

use crate::clock::{Clock, ClockConfig};
use crate::faults::{self, Fault, FaultConfig};
use crate::queue::EventQueue;
use crate::rng::{SimRng, Stream};
use crate::trace::{DropReason, Entry, Trace};

/// Everything that determines a run. Same config and node code, same run.
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub nodes: u32,
    pub net: NetConfig,
    pub faults: FaultConfig,
    pub clocks: ClockConfig,
    /// Stop once the next event is later than this much virtual time.
    pub max_time: Duration,
    /// Stop after this many events: a guard against protocols that never go quiet.
    pub max_steps: u64,
    /// Keep a readable line per event (costs memory; the trace hash is always computed).
    pub keep_trace_lines: bool,
}

impl SimConfig {
    /// A fault-free cluster: messages are only delayed (1–10 ms), nodes never crash, clocks are
    /// perfect.
    pub fn new(seed: u64, nodes: u32) -> Self {
        SimConfig {
            seed,
            nodes,
            net: NetConfig::default(),
            faults: FaultConfig::none(),
            clocks: ClockConfig::default(),
            max_time: Duration::from_secs(60),
            max_steps: 1_000_000,
            keep_trace_lines: false,
        }
    }

    /// Everything on: 5% message loss, 2% duplication, 2% delay spikes, a partition or crash every
    /// 0.1–1 s for the first 10 s (at most a minority of nodes down at once), clocks up to 1 s
    /// ahead and drifting up to ±5%. After 10 s the network heals and every node restarts, so
    /// tests can check the system recovers.
    pub fn chaos(seed: u64, nodes: u32) -> Self {
        SimConfig {
            net: NetConfig { drop: 0.05, duplicate: 0.02, spike: 0.02, ..NetConfig::default() },
            faults: FaultConfig {
                partitions: true,
                crashes: true,
                max_down: nodes.saturating_sub(1) / 2,
                ..FaultConfig::none()
            },
            clocks: ClockConfig { max_offset: Duration::from_secs(1), max_drift_ppm: 50_000 },
            ..SimConfig::new(seed, nodes)
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

/// Counts of what happened during a run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub delivered: u64,
    pub lost: u64,
    pub partitioned: u64,
    /// Messages that arrived while their destination was crashed.
    pub to_down_node: u64,
    pub duplicated: u64,
    pub crashes: u64,
    pub restarts: u64,
    /// Splits and one-way link failures injected.
    pub partitions: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunSummary {
    pub stop: StopReason,
    pub steps: u64,
    pub end_time: Time,
    pub trace_hash: u64,
    pub stats: Stats,
}

enum Event<M, T> {
    Start { node: NodeId, incarnation: u64 },
    Deliver { from: NodeId, to: NodeId, msg: M },
    Timer { node: NodeId, id: TimerId, timer: T },
    Nemesis,
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
    faults_end: Time,
    now: Time,
    steps: u64,
    ids: Vec<NodeId>,
    nodes: Vec<N>,
    make_node: Box<dyn FnMut(NodeId) -> N>,
    up: Vec<bool>,
    /// Bumped on every restart, so a start event from before a crash can't boot the node twice.
    incarnations: Vec<u64>,
    clocks: Vec<Clock>,
    node_rngs: Vec<SimRng>,
    net_rng: SimRng,
    fault_rng: SimRng,
    network: Network,
    queue: EventQueue<Event<N::Message, N::Timer>>,
    /// Pending timers and the node that owns each.
    active_timers: BTreeMap<TimerId, NodeId>,
    next_timer_id: u64,
    trace: Trace,
    stats: Stats,
}

impl<N: Node> Simulation<N> {
    /// Builds the cluster and schedules every node's start. `make_node` is called once per node
    /// now, and again whenever a crashed node restarts.
    pub fn new(config: SimConfig, mut make_node: impl FnMut(NodeId) -> N + 'static) -> Self {
        assert!(config.nodes > 0, "a simulation needs at least one node");
        assert!(config.faults.max_down < config.nodes, "max_down must leave a node running");
        let ids: Vec<NodeId> = (0..config.nodes).map(NodeId).collect();
        let nodes = ids.iter().map(|&id| make_node(id)).collect();
        let node_rngs = ids.iter().map(|&id| SimRng::new(config.seed, Stream::Node(id))).collect();
        let mut clock_rng = SimRng::new(config.seed, Stream::Clocks);
        let clocks = ids.iter().map(|_| Clock::random(&config.clocks, &mut clock_rng)).collect();
        let mut fault_rng = SimRng::new(config.seed, Stream::Faults);

        let mut queue = EventQueue::new();
        for &node in &ids {
            queue.push(Time::ZERO, Event::Start { node, incarnation: 0 });
        }
        if config.faults.enabled() {
            let first = fault_rng.duration_in(config.faults.interval.clone());
            queue.push(Time::ZERO + first, Event::Nemesis);
        }

        Simulation {
            deadline: Time::ZERO + config.max_time,
            faults_end: Time::ZERO + config.faults.active_for,
            now: Time::ZERO,
            steps: 0,
            nodes,
            make_node: Box::new(make_node),
            up: vec![true; ids.len()],
            incarnations: vec![0; ids.len()],
            clocks,
            node_rngs,
            net_rng: SimRng::new(config.seed, Stream::Network),
            fault_rng,
            network: Network::new(config.net.clone()),
            queue,
            active_timers: BTreeMap::new(),
            next_timer_id: 0,
            trace: Trace::new(config.keep_trace_lines),
            stats: Stats::default(),
            ids,
            config,
        }
    }

    /// Runs until the system goes quiet or a limit from the config is hit.
    pub fn run(&mut self) -> RunSummary {
        self.run_until(Time::ZERO + self.config.max_time)
    }

    /// Runs until the system goes quiet, the step limit is hit, or the next event is after
    /// `deadline`. Can be called repeatedly, e.g. to inject a fault at a chosen moment.
    pub fn run_until(&mut self, deadline: Time) -> RunSummary {
        self.deadline = deadline;
        let stop = loop {
            if let Some(reason) = self.step() {
                break reason;
            }
        };
        RunSummary {
            stop,
            steps: self.steps,
            end_time: self.now,
            trace_hash: self.trace.hash(),
            stats: self.stats,
        }
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

        let live = match &event {
            // Cancelled timers (and timers of crashed nodes) stay in the heap, because removing
            // from the middle is expensive, and are skipped here instead.
            Event::Timer { id, .. } => self.active_timers.remove(id).is_some(),
            Event::Start { node, incarnation } => {
                let i = node.0 as usize;
                self.up[i] && self.incarnations[i] == *incarnation
            }
            Event::Deliver { .. } | Event::Nemesis => true,
        };
        if !live {
            return None;
        }

        self.now = at;
        self.steps += 1;
        match event {
            Event::Start { node, .. } => {
                self.trace.record::<N::Message, N::Timer>(at, &Entry::Start { node });
                self.dispatch(node, |n, ctx| n.on_start(ctx));
            }
            Event::Deliver { from, to, msg } => {
                if self.up[to.0 as usize] {
                    self.stats.delivered += 1;
                    self.trace.record::<_, N::Timer>(at, &Entry::Deliver { from, to, msg: &msg });
                    self.dispatch(to, |n, ctx| n.on_message(ctx, from, msg));
                } else {
                    self.drop_message(from, to, DropReason::NodeDown, &msg);
                }
            }
            Event::Timer { node, id, timer } => {
                self.trace.record::<N::Message, _>(at, &Entry::Timer { node, id, timer: &timer });
                self.dispatch(node, |n, ctx| n.on_timer(ctx, id, timer));
            }
            Event::Nemesis => self.nemesis(),
        }
        None
    }

    /// Applies a fault right now. The nemesis calls this on its own schedule; tests call it to
    /// script exact scenarios.
    pub fn inject(&mut self, fault: Fault) {
        match &fault {
            Fault::Heal => self.network.heal(),
            Fault::Split(side) => {
                self.network.heal();
                self.network.split(side, &self.ids);
                self.stats.partitions += 1;
            }
            Fault::OneWay { from, to } => {
                self.network.heal();
                self.network.block(*from, *to);
                self.stats.partitions += 1;
            }
            Fault::Crash(node) => {
                let i = node.0 as usize;
                assert!(self.up[i], "{node} is already down");
                self.up[i] = false;
                // A dead process has no timers.
                self.active_timers.retain(|_, owner| owner != node);
                self.stats.crashes += 1;
            }
            Fault::Restart(node) => {
                let i = node.0 as usize;
                assert!(!self.up[i], "{node} is already up");
                self.nodes[i] = (self.make_node)(*node);
                self.up[i] = true;
                self.incarnations[i] += 1;
                let incarnation = self.incarnations[i];
                self.queue.push(self.now, Event::Start { node: *node, incarnation });
                self.stats.restarts += 1;
            }
        }
        self.trace.record::<N::Message, N::Timer>(self.now, &Entry::Fault(&fault));
    }

    pub fn now(&self) -> Time {
        self.now
    }

    pub fn is_up(&self, id: NodeId) -> bool {
        self.up[id.0 as usize]
    }

    pub fn node(&self, id: NodeId) -> &N {
        &self.nodes[id.0 as usize]
    }

    pub fn nodes(&self) -> impl Iterator<Item = (NodeId, &N)> {
        self.ids.iter().copied().zip(&self.nodes)
    }

    pub fn stats(&self) -> Stats {
        self.stats
    }

    pub fn trace(&self) -> &Trace {
        &self.trace
    }

    fn nemesis(&mut self) {
        if self.now >= self.faults_end {
            // Faults are over: undo them all so tests can check the system recovers.
            if !self.network.is_healed() {
                self.inject(Fault::Heal);
            }
            let down: Vec<NodeId> = self.ids.iter().copied().filter(|&n| !self.is_up(n)).collect();
            for node in down {
                self.inject(Fault::Restart(node));
            }
            return;
        }
        if let Some(fault) =
            faults::choose(&self.config.faults, &self.ids, &self.up, &mut self.fault_rng)
        {
            self.inject(fault);
        }
        let gap = self.fault_rng.duration_in(self.config.faults.interval.clone());
        self.queue.push(self.now + gap, Event::Nemesis);
    }

    fn dispatch(
        &mut self,
        node: NodeId,
        callback: impl FnOnce(&mut N, &mut dyn Context<N::Message, N::Timer>),
    ) {
        let idx = node.0 as usize;
        debug_assert!(self.up[idx], "dispatched an event to crashed node {node}");
        let mut ctx = SimContext {
            id: node,
            nodes: &self.ids,
            now: self.clocks[idx].local(self.now),
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
                match self.network.route(from, to, &mut self.net_rng) {
                    Route::Deliver(delay) => {
                        self.queue.push(self.now + delay, Event::Deliver { from, to, msg });
                    }
                    Route::Duplicate(first, second) => {
                        self.stats.duplicated += 1;
                        let copy = Event::Deliver { from, to, msg: msg.clone() };
                        self.queue.push(self.now + first, copy);
                        self.queue.push(self.now + second, Event::Deliver { from, to, msg });
                    }
                    Route::Lost => self.drop_message(from, to, DropReason::Lost, &msg),
                    Route::Partitioned => {
                        self.drop_message(from, to, DropReason::Partitioned, &msg)
                    }
                }
            }
            Effect::SetTimer { id, after, timer } => {
                // `after` is measured on the node's own, possibly drifting, clock.
                let delay = self.clocks[from.0 as usize].real_delay(after);
                self.active_timers.insert(id, from);
                self.queue.push(self.now + delay, Event::Timer { node: from, id, timer });
            }
            Effect::CancelTimer { id } => {
                self.active_timers.remove(&id);
            }
        }
    }

    fn drop_message(&mut self, from: NodeId, to: NodeId, reason: DropReason, msg: &N::Message) {
        match reason {
            DropReason::Lost => self.stats.lost += 1,
            DropReason::Partitioned => self.stats.partitioned += 1,
            DropReason::NodeDown => self.stats.to_down_node += 1,
        }
        self.trace.record::<_, N::Timer>(self.now, &Entry::Drop { from, to, reason, msg });
    }
}

/// The simulator's [`Context`]: records what the node asks for instead of doing it.
struct SimContext<'a, M, T> {
    id: NodeId,
    nodes: &'a [NodeId],
    /// The node's local clock reading, not real time.
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
