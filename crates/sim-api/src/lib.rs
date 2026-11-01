//! Traits and shared types that systems under test use to talk to the world: Node, Context, Storage, NodeId, virtual time.
//!
//! A system under test is written only against this crate. The simulator (`sim-core`) provides one
//! implementation of [`Context`]; a real-network runtime can provide another, so the same node code
//! runs in both.

mod random;
pub mod record;
mod storage;
mod sub_context;
mod time;
pub mod wire;

use std::fmt;
use std::hash::Hash;
use std::ops::Range;
use std::time::Duration;

pub use random::Rand;
pub use storage::{Storage, SyncError};
pub use sub_context::SubContext;
pub use time::Time;

/// Identifies a node in the cluster. Nodes are numbered `0..cluster_size`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u32);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n{}", self.0)
    }
}

/// Handle to a pending timer, used to cancel it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimerId(pub u64);

/// A participant in the system under test, written as an event handler (actor).
///
/// Nodes never block, sleep or spawn threads: they react to one event at a time and act on the
/// world only through the [`Context`]. That is what lets the simulator decide the order of every
/// event, and therefore reproduce any run from its seed.
pub trait Node {
    /// Messages nodes exchange. `Clone` lets the network duplicate them; `Hash` feeds the trace hash.
    type Message: Clone + fmt::Debug + Hash;
    /// Payload of the timers this node sets, e.g. an election timeout.
    type Timer: Clone + fmt::Debug + Hash;

    /// Called when the node boots (and, once crashes exist, after every restart).
    fn on_start(&mut self, ctx: &mut dyn Context<Self::Message, Self::Timer>);

    /// Called when a message from `from` arrives.
    fn on_message(
        &mut self,
        ctx: &mut dyn Context<Self::Message, Self::Timer>,
        from: NodeId,
        msg: Self::Message,
    );

    /// Called when a timer set with [`Context::set_timer`] fires (cancelled timers never fire).
    fn on_timer(
        &mut self,
        ctx: &mut dyn Context<Self::Message, Self::Timer>,
        id: TimerId,
        timer: Self::Timer,
    );
}

/// Everything a node may do to the outside world. `M` is the message type, `T` the timer payload.
pub trait Context<M, T> {
    /// This node's id.
    fn id(&self) -> NodeId;

    /// All nodes in the cluster, including this one, in id order.
    fn nodes(&self) -> &[NodeId];

    /// Current virtual time.
    fn now(&self) -> Time;

    /// Send `msg` to `to`. Delivery is up to the network: it may be delayed (and later, dropped).
    fn send(&mut self, to: NodeId, msg: M);

    /// Fire `timer` back at this node after `after` has elapsed.
    fn set_timer(&mut self, after: Duration, timer: T) -> TimerId;

    /// Cancel a pending timer. Cancelling one that already fired is a no-op.
    fn cancel_timer(&mut self, id: TimerId);

    /// This node's disk. It outlives crashes; only synced data is guaranteed to survive one.
    fn storage(&mut self) -> &mut dyn Storage;

    /// Stops this process at once, as if it had crashed, and has it restarted shortly after (like
    /// a supervisor restarting a process that panicked). Anything requested after this call in
    /// the same callback is discarded. This is the safe reaction to a failed sync.
    fn halt(&mut self);

    /// A random number from this node's own deterministic stream.
    fn random_u64(&mut self) -> u64;

    /// BUGGIFY (from FoundationDB): `true` occasionally in simulation runs that enable it, and
    /// always `false` otherwise. Code uses it to take rare-but-correct paths (tiny batches,
    /// skipped heartbeats, aggressive timeouts) that normal timing seldom reaches.
    fn buggify(&mut self) -> bool {
        false
    }

    /// A random number in `range`. Modulo bias is negligible for the small ranges nodes use.
    fn random_range(&mut self, range: Range<u64>) -> u64 {
        assert!(!range.is_empty(), "random_range called with an empty range");
        range.start + self.random_u64() % (range.end - range.start)
    }
}
