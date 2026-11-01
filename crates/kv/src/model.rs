use std::collections::BTreeMap;

use checker::{History, Model, OpId};

use crate::client::ClientEvent;
use crate::command::{Op, OpResult, RequestId};

/// The single-machine specification of the store: one register per key.
#[derive(Clone, Copy, Debug, Default)]
pub struct KvModel;

impl Model for KvModel {
    type State = Option<u64>;
    type Input = Op;
    type Output = OpResult;

    fn init(&self) -> Option<u64> {
        None
    }

    fn apply(&self, state: &Option<u64>, op: &Op) -> (Option<u64>, OpResult) {
        match *op {
            Op::Get { .. } => (*state, OpResult::Value(*state)),
            Op::Put { value, .. } => (Some(value), OpResult::Written),
            Op::Cas { expected, new, .. } => {
                let swapped = *state == expected;
                (if swapped { Some(new) } else { *state }, OpResult::Cas { swapped })
            }
        }
    }

    fn partition(&self, op: &Op) -> u64 {
        match *op {
            Op::Get { key } | Op::Put { key, .. } | Op::Cas { key, .. } => u64::from(key),
        }
    }
}

/// Builds a [`History`] from client events, in the order a test harness observes them.
#[derive(Debug, Default)]
pub struct Recorder {
    history: History<Op, OpResult>,
    in_flight: BTreeMap<RequestId, OpId>,
}

impl Recorder {
    pub fn record(&mut self, event: ClientEvent) {
        match event {
            ClientEvent::Invoke { id, op } => {
                let op_id = self.history.invoke(op);
                self.in_flight.insert(id, op_id);
            }
            ClientEvent::Complete { id, result } => {
                let op_id = self.in_flight.remove(&id).expect("completion without an invocation");
                self.history.complete(op_id, result);
            }
        }
    }

    /// Everything recorded so far. Operations still in flight count as having an unknown outcome.
    pub fn history(&self) -> &History<Op, OpResult> {
        &self.history
    }
}
