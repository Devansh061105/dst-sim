//! Write a node as one `async` function instead of a set of event callbacks.
//!
//! A protocol with retries and timeouts is a state machine when written as callbacks: what was
//! I waiting for, which timer belongs to it, what do I do when it fires. As an `async` function it
//! reads top to bottom: send, wait for the reply or a timeout, retry. This crate runs such a
//! function on the unchanged simulator, as an ordinary [`Node`].
//!
//! How it stays deterministic: there are no threads and no runtime. The node's logic is one
//! future, which talks to the world only through an [`Io`] handle. `Io` doesn't act directly; it
//! queues requests (send, set a timer, cancel one) that the adapter applies to the real
//! [`Context`] after each poll, in the order they were made. Every event (start, message, timer)
//! polls the future exactly once with a no-op waker: an `await` can only be waiting for a message
//! or a timer, and each of those arrives as an event. So the simulator still decides the order of
//! everything, and a seed still replays bit for bit.
//!
//! Limits, on purpose: one task per node (no spawning), no disk access (protocols that need
//! durable state are written as callbacks), and a node whose function returns ignores further
//! events.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

use sim_api::{Context, Node, NodeId, Time, TimerId};

pub mod echo;

/// The timer payload of an async node: which [`Timer`] fired.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Wake(u64);

/// A timer started with [`Io::start_timer`]. Await it with [`Io::recv_or`] or [`Io::wait`].
#[derive(Debug, PartialEq, Eq)]
pub struct Timer(u64);

enum Request<M> {
    Send(NodeId, M),
    SetTimer(u64, Duration),
    Cancel(u64),
}

struct Shared<M> {
    id: NodeId,
    nodes: Vec<NodeId>,
    now: Time,
    inbox: VecDeque<(NodeId, M)>,
    /// Timers that fired and haven't been awaited yet.
    fired: BTreeSet<u64>,
    requests: Vec<Request<M>>,
    next_timer: u64,
    /// SplitMix64 state, seeded from the node's own random stream at start.
    rng: u64,
}

/// What an async node can do: the [`Context`] operations, plus futures that wait for messages
/// and timers.
pub struct Io<M> {
    shared: Rc<RefCell<Shared<M>>>,
}

impl<M> Clone for Io<M> {
    fn clone(&self) -> Self {
        Io { shared: Rc::clone(&self.shared) }
    }
}

impl<M> Io<M> {
    /// This node's id.
    pub fn id(&self) -> NodeId {
        self.shared.borrow().id
    }

    /// All nodes in the cluster, in id order.
    pub fn nodes(&self) -> Vec<NodeId> {
        self.shared.borrow().nodes.clone()
    }

    /// Virtual time at the event that resumed this node.
    pub fn now(&self) -> Time {
        self.shared.borrow().now
    }

    /// Sends `msg` to `to` (applied when the node next yields).
    pub fn send(&self, to: NodeId, msg: M) {
        self.shared.borrow_mut().requests.push(Request::Send(to, msg));
    }

    /// A deterministic random number.
    pub fn random_u64(&self) -> u64 {
        let mut shared = self.shared.borrow_mut();
        shared.rng = shared.rng.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = shared.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Starts a timer that fires after `after`.
    pub fn start_timer(&self, after: Duration) -> Timer {
        let mut shared = self.shared.borrow_mut();
        let token = shared.next_timer;
        shared.next_timer += 1;
        shared.requests.push(Request::SetTimer(token, after));
        Timer(token)
    }

    /// Cancels a timer; it will never fire.
    pub fn cancel(&self, timer: Timer) {
        let mut shared = self.shared.borrow_mut();
        shared.fired.remove(&timer.0);
        shared.requests.push(Request::Cancel(timer.0));
    }

    /// Waits for the next message.
    pub fn recv(&self) -> impl Future<Output = (NodeId, M)> + '_ {
        std::future::poll_fn(move |_| match self.shared.borrow_mut().inbox.pop_front() {
            Some(message) => Poll::Ready(message),
            None => Poll::Pending,
        })
    }

    /// Waits for the next message, or for `timer` to fire (`None`). The timer keeps running
    /// when a message arrives first, so a loop can wait against one deadline.
    pub fn recv_or<'a>(
        &'a self,
        timer: &'a Timer,
    ) -> impl Future<Output = Option<(NodeId, M)>> + 'a {
        std::future::poll_fn(move |_| {
            let mut shared = self.shared.borrow_mut();
            if let Some(message) = shared.inbox.pop_front() {
                Poll::Ready(Some(message))
            } else if shared.fired.remove(&timer.0) {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        })
    }

    /// Waits for `timer` to fire.
    pub fn wait(&self, timer: Timer) -> impl Future<Output = ()> + '_ {
        std::future::poll_fn(move |_| {
            if self.shared.borrow_mut().fired.remove(&timer.0) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
    }

    /// Waits for `after` to pass.
    pub async fn sleep(&self, after: Duration) {
        let timer = self.start_timer(after);
        self.wait(timer).await;
    }
}

type Task = Pin<Box<dyn Future<Output = ()>>>;

/// Runs an async function as a [`Node`]. The function is called again, from scratch, every time
/// the node starts or restarts after a crash (memory is lost; there is no disk access).
pub struct AsyncNode<M> {
    start: Box<dyn Fn(Io<M>) -> Task>,
    running: Option<(Io<M>, Task)>,
    /// Timer token → the simulator's id for it, so it can be cancelled.
    timers: BTreeMap<u64, TimerId>,
    finished: bool,
}

impl<M> fmt::Debug for AsyncNode<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncNode").field("finished", &self.finished).finish_non_exhaustive()
    }
}

impl<M: 'static> AsyncNode<M> {
    /// A node running `body`.
    pub fn new<F: Future<Output = ()> + 'static>(body: impl Fn(Io<M>) -> F + 'static) -> Self {
        AsyncNode {
            start: Box::new(move |io| Box::pin(body(io))),
            running: None,
            timers: BTreeMap::new(),
            finished: false,
        }
    }

    /// Whether the node's function has returned.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Polls the task once, then applies what it asked for, in order.
    fn poll(&mut self, ctx: &mut dyn Context<M, Wake>) {
        let Some((io, task)) = &mut self.running else { return };
        io.shared.borrow_mut().now = ctx.now();
        let mut cx = std::task::Context::from_waker(Waker::noop());
        if task.as_mut().poll(&mut cx).is_ready() {
            self.finished = true;
        }
        let requests = std::mem::take(&mut io.shared.borrow_mut().requests);
        for request in requests {
            match request {
                Request::Send(to, msg) => ctx.send(to, msg),
                Request::SetTimer(token, after) => {
                    let id = ctx.set_timer(after, Wake(token));
                    self.timers.insert(token, id);
                }
                Request::Cancel(token) => {
                    if let Some(id) = self.timers.remove(&token) {
                        ctx.cancel_timer(id);
                    }
                }
            }
        }
        if self.finished {
            self.running = None;
        }
    }
}

impl<M: Clone + fmt::Debug + Hash + 'static> Node for AsyncNode<M> {
    type Message = M;
    type Timer = Wake;

    fn on_start(&mut self, ctx: &mut dyn Context<M, Wake>) {
        let shared = Shared {
            id: ctx.id(),
            nodes: ctx.nodes().to_vec(),
            now: ctx.now(),
            inbox: VecDeque::new(),
            fired: BTreeSet::new(),
            requests: Vec::new(),
            next_timer: 0,
            rng: ctx.random_u64(),
        };
        let io = Io { shared: Rc::new(RefCell::new(shared)) };
        let task = (self.start)(io.clone());
        self.running = Some((io, task));
        self.timers.clear();
        self.finished = false;
        self.poll(ctx);
    }

    fn on_message(&mut self, ctx: &mut dyn Context<M, Wake>, from: NodeId, msg: M) {
        if let Some((io, _)) = &self.running {
            io.shared.borrow_mut().inbox.push_back((from, msg));
            self.poll(ctx);
        }
    }

    fn on_timer(&mut self, ctx: &mut dyn Context<M, Wake>, _id: TimerId, Wake(token): Wake) {
        if let Some((io, _)) = &self.running {
            self.timers.remove(&token);
            io.shared.borrow_mut().fired.insert(token);
            self.poll(ctx);
        }
    }
}
