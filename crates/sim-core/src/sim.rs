use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use sim_api::{Context, Node, NodeId, Rand, Storage, SyncError, Time, TimerId};
use sim_disk::{CrashReport, Disk, DiskConfig, DiskIo};
use sim_net::{NetConfig, Network, Route};

use crate::clock::{Clock, ClockConfig};
use crate::faults::{self, Fault, FaultConfig, Script};
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
    pub disk: DiskConfig,
    /// Probability that a BUGGIFY point (`Context::buggify`) fires. 0 turns BUGGIFY off.
    pub buggify: f64,
    /// Stop once the next event is later than this much virtual time.
    pub max_time: Duration,
    /// Stop after this many events: a guard against protocols that never go quiet.
    pub max_steps: u64,
    /// Keep a readable line per event (costs memory; the trace hash is always computed).
    pub keep_trace_lines: bool,
}

impl SimConfig {
    /// A fault-free cluster: messages are only delayed (1–10 ms), nodes never crash, clocks are
    /// perfect, and a crash (if injected) loses exactly the unsynced disk writes.
    pub fn new(seed: u64, nodes: u32) -> Self {
        SimConfig {
            seed,
            nodes,
            net: NetConfig::default(),
            faults: FaultConfig::none(),
            clocks: ClockConfig::default(),
            disk: DiskConfig::default(),
            buggify: 0.0,
            max_time: Duration::from_secs(60),
            max_steps: 1_000_000,
            keep_trace_lines: false,
        }
    }

    /// Everything on: 5% message loss, 2% duplication, 2% delay spikes, a partition or crash every
    /// 0.1–1 s for the first 10 s (at most a minority of nodes down at once), clocks up to 1 s
    /// ahead and drifting up to ±5%, and disks where 0.5% of syncs fail, 0.5% of syncs are cut
    /// short by the process dying, and a crash keeps half the unsynced writes (30% of those torn).
    /// After 10 s the network heals and every node restarts, so tests can check the system
    /// recovers.
    pub fn chaos(seed: u64, nodes: u32) -> Self {
        SimConfig {
            net: NetConfig { drop: 0.05, duplicate: 0.02, spike: 0.02, ..NetConfig::default() },
            faults: FaultConfig {
                partitions: true,
                crashes: true,
                max_down: nodes.saturating_sub(1) / 2,
                max_corrupted: nodes.saturating_sub(1) / 2,
                ..FaultConfig::none()
            },
            clocks: ClockConfig { max_offset: Duration::from_secs(1), max_drift_ppm: 50_000 },
            disk: DiskConfig {
                unsynced_survive: 0.5,
                torn_write: 0.3,
                sync_fail: 0.005,
                crash_during_sync: 0.005,
                corrupt: 0.05,
            },
            ..SimConfig::new(seed, nodes)
        }
    }

    /// Swarm testing: every seed also picks *which* faults are on and how hard they hit, from its
    /// own random stream. A single fixed mix tends to hide bugs that need one fault to be absent
    /// so another can reach a rare state (Groce et al., "Swarm Testing", ISSTA 2012). Every mix
    /// still ends with the calm period, so a correct system always finishes.
    pub fn swarm(seed: u64, nodes: u32) -> Self {
        let mut r = SimRng::new(seed, Stream::Swarm);
        let mut pick = |options: &[f64]| options[r.below(options.len() as u64) as usize];
        let net = NetConfig {
            drop: pick(&[0.0, 0.01, 0.05, 0.1]),
            duplicate: pick(&[0.0, 0.02, 0.1]),
            spike: pick(&[0.0, 0.02, 0.1]),
            ..NetConfig::default()
        };
        let disk = DiskConfig {
            unsynced_survive: pick(&[0.0, 0.5, 1.0]),
            torn_write: pick(&[0.0, 0.3, 1.0]),
            sync_fail: pick(&[0.0, 0.005, 0.02]),
            crash_during_sync: pick(&[0.0, 0.005, 0.02]),
            corrupt: pick(&[0.0, 0.05, 0.3]),
        };
        let buggify = pick(&[0.0, 0.05]);
        let max_drift_ppm = pick(&[0.0, 10_000.0, 50_000.0]) as u32;
        let max_offset = Duration::from_millis(pick(&[0.0, 1_000.0]) as u64);
        let ms = Duration::from_millis;
        let paces = [ms(50)..ms(200), ms(100)..ms(1_000), ms(1_000)..ms(3_000)];
        let interval = paces[r.below(paces.len() as u64) as usize].clone();
        let faults = FaultConfig {
            partitions: r.chance(0.5),
            crashes: r.chance(0.5),
            max_down: nodes.saturating_sub(1) / 2,
            max_corrupted: nodes.saturating_sub(1) / 2,
            interval,
            ..FaultConfig::none()
        };
        SimConfig {
            net,
            faults,
            clocks: ClockConfig { max_offset, max_drift_ppm },
            disk,
            buggify,
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
    /// Nodes that stopped themselves with `Context::halt` (e.g. after a failed sync).
    pub halts: u64,
    pub restarts: u64,
    /// Splits and one-way link failures injected.
    pub partitions: u64,
    pub sync_failures: u64,
    /// Processes that died in the middle of a sync.
    pub sync_crashes: u64,
    /// Unsynced disk operations that crashes threw away.
    pub unsynced_lost: u64,
    /// Unsynced disk operations that reached the disk before a crash anyway.
    pub unsynced_survived: u64,
    pub torn_writes: u64,
    /// Crashes that also damaged data already on disk.
    pub corruptions: u64,
}

/// Adds up the counts of several runs.
impl std::ops::AddAssign for Stats {
    fn add_assign(&mut self, other: Stats) {
        let Stats {
            delivered,
            lost,
            partitioned,
            to_down_node,
            duplicated,
            crashes,
            halts,
            restarts,
            partitions,
            sync_failures,
            sync_crashes,
            unsynced_lost,
            unsynced_survived,
            torn_writes,
            corruptions,
        } = other;
        self.delivered += delivered;
        self.lost += lost;
        self.partitioned += partitioned;
        self.to_down_node += to_down_node;
        self.duplicated += duplicated;
        self.crashes += crashes;
        self.halts += halts;
        self.restarts += restarts;
        self.partitions += partitions;
        self.sync_failures += sync_failures;
        self.sync_crashes += sync_crashes;
        self.unsynced_lost += unsynced_lost;
        self.unsynced_survived += unsynced_survived;
        self.torn_writes += torn_writes;
        self.corruptions += corruptions;
    }
}

/// How a run ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunSummary {
    pub stop: StopReason,
    pub steps: u64,
    pub end_time: Time,
    pub trace_hash: u64,
    pub stats: Stats,
}

enum Event<M, T> {
    Start {
        node: NodeId,
        incarnation: u64,
    },
    Deliver {
        from: NodeId,
        to: NodeId,
        sent: Time,
        msg: M,
    },
    Timer {
        node: NodeId,
        id: TimerId,
        timer: T,
    },
    Nemesis,
    /// A fault from a written-out schedule ([`FaultConfig::script`]).
    Scripted(Fault),
    /// The supervisor restarting a node that halted itself.
    Restart {
        node: NodeId,
    },
}

/// What a node asked for during one callback. The simulator decides what actually happens.
enum Effect<M, T> {
    Send { to: NodeId, msg: M },
    SetTimer { id: TimerId, after: Duration, timer: T },
    CancelTimer { id: TimerId },
    Halt,
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
    /// One disk per node. Disks outlive crashes; that is how a restarted node recovers.
    disks: Vec<Disk>,
    disk_rngs: Vec<SimRng>,
    /// Nodes whose disks have been corrupted, once each (at most `faults.max_corrupted`).
    corrupted: BTreeSet<NodeId>,
    node_rngs: Vec<SimRng>,
    /// Kept apart from `node_rngs` so turning BUGGIFY on doesn't shift a node's own randomness.
    buggify_rngs: Vec<SimRng>,
    net_rng: SimRng,
    fault_rng: SimRng,
    supervisor_rng: SimRng,
    /// Every fault the nemesis (or a script) injected, so the run can be replayed from a script.
    injected: Vec<(Time, Fault)>,
    /// When the faults ended, once they have.
    calmed_at: Option<Time>,
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
        let buggify_rngs =
            ids.iter().map(|&id| SimRng::new(config.seed, Stream::Buggify(id))).collect();
        let mut clock_rng = SimRng::new(config.seed, Stream::Clocks);
        let clocks = ids.iter().map(|_| Clock::random(&config.clocks, &mut clock_rng)).collect();
        let disks = ids.iter().map(|_| Disk::new(config.disk.clone())).collect();
        let disk_rngs = ids.iter().map(|&id| SimRng::new(config.seed, Stream::Disk(id))).collect();
        let mut fault_rng = SimRng::new(config.seed, Stream::Faults);

        let mut queue = EventQueue::new();
        for &node in &ids {
            queue.push(Time::ZERO, Event::Start { node, incarnation: 0 });
        }
        let mut faults_end = Time::ZERO + config.faults.active_for;
        if let Some(script) = &config.faults.script {
            for (at, fault) in script.faults.iter().filter(|(at, _)| *at < script.calm_at) {
                queue.push(*at, Event::Scripted(fault.clone()));
            }
            // The one nemesis event: the calm period.
            faults_end = script.calm_at;
            queue.push(faults_end, Event::Nemesis);
        } else if config.faults.enabled() {
            let first = fault_rng.duration_in(config.faults.interval.clone());
            queue.push(Time::ZERO + first, Event::Nemesis);
        }

        Simulation {
            deadline: Time::ZERO + config.max_time,
            faults_end,
            now: Time::ZERO,
            steps: 0,
            nodes,
            make_node: Box::new(make_node),
            up: vec![true; ids.len()],
            incarnations: vec![0; ids.len()],
            clocks,
            disks,
            disk_rngs,
            corrupted: BTreeSet::new(),
            node_rngs,
            buggify_rngs,
            net_rng: SimRng::new(config.seed, Stream::Network),
            fault_rng,
            supervisor_rng: SimRng::new(config.seed, Stream::Supervisor),
            injected: Vec::new(),
            calmed_at: None,
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
            // The nemesis may already have restarted it.
            Event::Restart { node } => !self.up[node.0 as usize],
            // A scripted fault that no longer applies (the schedule was edited) is skipped.
            Event::Scripted(Fault::Crash(node)) => self.up[node.0 as usize],
            Event::Scripted(Fault::Restart(node)) => !self.up[node.0 as usize],
            Event::Deliver { .. } | Event::Nemesis | Event::Scripted(_) => true,
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
            Event::Deliver { from, to, sent, msg } => {
                if self.up[to.0 as usize] {
                    self.stats.delivered += 1;
                    let entry = Entry::Deliver { from, to, sent, msg: &msg };
                    self.trace.record::<_, N::Timer>(at, &entry);
                    self.dispatch(to, |n, ctx| n.on_message(ctx, from, msg));
                } else {
                    self.drop_message(from, to, sent, DropReason::NodeDown, &msg);
                }
            }
            Event::Timer { node, id, timer } => {
                self.trace.record::<N::Message, _>(at, &Entry::Timer { node, id, timer: &timer });
                self.dispatch(node, |n, ctx| n.on_timer(ctx, id, timer));
            }
            Event::Nemesis => self.nemesis(),
            Event::Scripted(fault) => {
                self.injected.push((at, fault.clone()));
                self.inject(fault);
            }
            Event::Restart { node } => self.inject(Fault::Restart(node)),
        }
        None
    }

    /// The faults injected so far, written out. Replaying it (as [`FaultConfig::script`], with
    /// the rest of the config unchanged) reproduces this run.
    pub fn script(&self) -> Script {
        Script { faults: self.injected.clone(), calm_at: self.calmed_at.unwrap_or(self.faults_end) }
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
                self.stats.crashes += 1;
                self.trace.record::<N::Message, N::Timer>(self.now, &Entry::Fault(&fault));
                self.crash_node(*node);
                return;
            }
            Fault::Restart(node) => {
                let i = node.0 as usize;
                assert!(!self.up[i], "{node} is already up");
                self.nodes[i] = (self.make_node)(*node);
                self.up[i] = true;
                self.incarnations[i] += 1;
                self.stats.restarts += 1;
                self.trace.record::<N::Message, N::Timer>(self.now, &Entry::Fault(&fault));
                // Boot right away rather than queueing a start event: otherwise a message due at
                // this same instant could reach the new process before its `on_start` (recovery)
                // had run.
                self.trace.record::<N::Message, N::Timer>(self.now, &Entry::Start { node: *node });
                self.dispatch(*node, |n, ctx| n.on_start(ctx));
                return;
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

    /// Mutable access for test harnesses, e.g. to collect events a node recorded.
    pub fn node_mut(&mut self, id: NodeId) -> &mut N {
        &mut self.nodes[id.0 as usize]
    }

    pub fn nodes(&self) -> impl Iterator<Item = (NodeId, &N)> {
        self.ids.iter().copied().zip(&self.nodes)
    }

    pub fn stats(&self) -> Stats {
        self.stats
    }

    /// Whether this node's disk has been corrupted ([`DiskConfig::corrupt`](sim_disk::DiskConfig)).
    pub fn was_corrupted(&self, id: NodeId) -> bool {
        self.corrupted.contains(&id)
    }

    /// A node's disk, for inspecting what is durable.
    pub fn disk(&self, id: NodeId) -> &Disk {
        &self.disks[id.0 as usize]
    }

    pub fn trace(&self) -> &Trace {
        &self.trace
    }

    /// The process dies: its timers are gone and its disk loses the page cache.
    fn crash_node(&mut self, node: NodeId) {
        let i = node.0 as usize;
        assert!(self.up[i], "{node} is already down");
        self.up[i] = false;
        self.active_timers.retain(|_, owner| *owner != node);
        let report = self.disks[i].crash(&mut self.disk_rngs[i]);
        self.stats.unsynced_lost += report.lost;
        self.stats.unsynced_survived += report.survived;
        self.stats.torn_writes += report.torn;
        if report != CrashReport::default() {
            self.trace.record::<N::Message, N::Timer>(self.now, &Entry::DiskCrash { node, report });
        } else if !self.corrupted.contains(&node)
            && (self.corrupted.len() as u32) < self.config.faults.max_corrupted
        {
            // Only after a crash that left nothing unsynced, so corruption and torn writes stay
            // separate faults: corruption only ever hits data that was already on disk.
            if let Some(corruption) = self.disks[i].corrupt_at_rest(&mut self.disk_rngs[i]) {
                self.corrupted.insert(node);
                self.stats.corruptions += 1;
                self.trace.record::<N::Message, N::Timer>(
                    self.now,
                    &Entry::Corrupt { node, corruption: &corruption },
                );
            }
        }
    }

    /// The node stopped itself; a supervisor restarts it after `restart_delay`.
    fn halt(&mut self, node: NodeId) {
        self.stats.halts += 1;
        self.trace.record::<N::Message, N::Timer>(self.now, &Entry::Halt { node });
        self.crash_node(node);
        let delay = self.supervisor_rng.duration_in(self.config.faults.restart_delay.clone());
        self.queue.push(self.now + delay, Event::Restart { node });
    }

    fn nemesis(&mut self) {
        if self.now >= self.faults_end {
            // Faults are over: undo them all so tests can check the system recovers. Disks stop
            // failing too: a disk that fails a few percent of fsyncs forever is a fault that never
            // ends, and no system can promise progress through that.
            self.calmed_at = Some(self.now);
            for disk in &mut self.disks {
                disk.calm();
            }
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
            self.injected.push((self.now, fault.clone()));
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
            buggify: self.config.buggify,
            buggify_rng: &mut self.buggify_rngs[idx],
            storage: DiskIo { disk: &mut self.disks[idx], rand: &mut self.disk_rngs[idx] },
            next_timer_id: &mut self.next_timer_id,
            effects: Vec::new(),
            sync_crashes: 0,
            halted: false,
        };
        callback(&mut self.nodes[idx], &mut ctx);
        self.stats.sync_crashes += ctx.sync_crashes;
        let effects = ctx.effects;
        for failure in self.disks[idx].take_sync_failures() {
            self.stats.sync_failures += 1;
            let entry = Entry::SyncFailed { node, file: &failure.file, dropped: failure.dropped };
            self.trace.record::<N::Message, N::Timer>(self.now, &entry);
        }
        for effect in effects {
            self.apply(node, effect);
            if !self.up[idx] {
                // The node halted: nothing it asked for afterwards happens.
                break;
            }
        }
    }

    fn apply(&mut self, from: NodeId, effect: Effect<N::Message, N::Timer>) {
        match effect {
            Effect::Send { to, msg } => {
                assert!(to.0 < self.config.nodes, "{from} sent to unknown node {to}");
                let sent = self.now;
                match self.network.route(from, to, &mut self.net_rng) {
                    Route::Deliver(delay) => {
                        self.queue.push(sent + delay, Event::Deliver { from, to, sent, msg });
                    }
                    Route::Duplicate(first, second) => {
                        self.stats.duplicated += 1;
                        let copy = Event::Deliver { from, to, sent, msg: msg.clone() };
                        self.queue.push(sent + first, copy);
                        self.queue.push(sent + second, Event::Deliver { from, to, sent, msg });
                    }
                    Route::Lost => self.drop_message(from, to, sent, DropReason::Lost, &msg),
                    Route::Partitioned => {
                        self.drop_message(from, to, sent, DropReason::Partitioned, &msg)
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
            Effect::Halt => self.halt(from),
        }
    }

    fn drop_message(
        &mut self,
        from: NodeId,
        to: NodeId,
        sent: Time,
        reason: DropReason,
        msg: &N::Message,
    ) {
        match reason {
            DropReason::Lost => self.stats.lost += 1,
            DropReason::Partitioned => self.stats.partitioned += 1,
            DropReason::NodeDown => self.stats.to_down_node += 1,
        }
        self.trace.record::<_, N::Timer>(self.now, &Entry::Drop { from, to, sent, reason, msg });
    }
}

/// The simulator's [`Context`]: records what the node asks for instead of doing it.
struct SimContext<'a, M, T> {
    id: NodeId,
    nodes: &'a [NodeId],
    /// The node's local clock reading, not real time.
    now: Time,
    rng: &'a mut SimRng,
    buggify: f64,
    buggify_rng: &'a mut SimRng,
    storage: DiskIo<'a, SimRng>,
    next_timer_id: &'a mut u64,
    effects: Vec<Effect<M, T>>,
    sync_crashes: u64,
    /// The node halted (or died mid-sync): the process is gone, so the rest of the callback
    /// touches nothing. In particular it must not keep writing after a failed sync.
    halted: bool,
}

// Disk calls go through the context (rather than straight to the `DiskIo`) so that a crash in the
// middle of a sync can stop the node at exactly that point: everything it asks for afterwards
// lands behind the halt and is discarded.
impl<M, T> Storage for SimContext<'_, M, T> {
    fn read(&self, file: &str) -> Vec<u8> {
        self.storage.read(file)
    }

    fn file_len(&self, file: &str) -> u64 {
        self.storage.file_len(file)
    }

    fn write(&mut self, file: &str, offset: u64, data: &[u8]) {
        if !self.halted {
            self.storage.write(file, offset, data);
        }
    }

    fn truncate(&mut self, file: &str, len: u64) {
        if !self.halted {
            self.storage.truncate(file, len);
        }
    }

    fn sync(&mut self, file: &str) -> Result<(), SyncError> {
        if self.halted {
            return Err(SyncError);
        }
        let result = self.storage.sync(file);
        if self.storage.disk.take_crash_in_sync() {
            self.sync_crashes += 1;
            self.halt();
        }
        result
    }
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

    fn storage(&mut self) -> &mut dyn Storage {
        self
    }

    fn halt(&mut self) {
        self.halted = true;
        self.effects.push(Effect::Halt);
    }

    fn random_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }

    fn buggify(&mut self) -> bool {
        // `chance(0.0)` draws nothing, so runs without BUGGIFY are unaffected.
        self.buggify_rng.chance(self.buggify)
    }
}
