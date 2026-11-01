use std::time::Duration;

use crate::{Context, NodeId, Storage, Time, TimerId};

/// Lets a component written against its own message and timer types (say, a Raft library) run
/// inside a node whose types are larger enums wrapping them.
///
/// Everything passes straight through to the node's context, except that messages and timers the
/// component creates are wrapped with `wrap_msg` and `wrap_timer` (typically enum variant
/// constructors such as `KvMsg::Raft`).
pub struct SubContext<'a, M, T, SM, ST> {
    inner: &'a mut dyn Context<M, T>,
    wrap_msg: fn(SM) -> M,
    wrap_timer: fn(ST) -> T,
}

impl<'a, M, T, SM, ST> SubContext<'a, M, T, SM, ST> {
    pub fn new(
        inner: &'a mut dyn Context<M, T>,
        wrap_msg: fn(SM) -> M,
        wrap_timer: fn(ST) -> T,
    ) -> Self {
        SubContext { inner, wrap_msg, wrap_timer }
    }
}

impl<M, T, SM, ST> Context<SM, ST> for SubContext<'_, M, T, SM, ST> {
    fn id(&self) -> NodeId {
        self.inner.id()
    }

    fn nodes(&self) -> &[NodeId] {
        self.inner.nodes()
    }

    fn now(&self) -> Time {
        self.inner.now()
    }

    fn send(&mut self, to: NodeId, msg: SM) {
        self.inner.send(to, (self.wrap_msg)(msg));
    }

    fn set_timer(&mut self, after: Duration, timer: ST) -> TimerId {
        self.inner.set_timer(after, (self.wrap_timer)(timer))
    }

    fn cancel_timer(&mut self, id: TimerId) {
        self.inner.cancel_timer(id);
    }

    fn storage(&mut self) -> &mut dyn Storage {
        self.inner.storage()
    }

    fn halt(&mut self) {
        self.inner.halt();
    }

    fn random_u64(&mut self) -> u64 {
        self.inner.random_u64()
    }
}
