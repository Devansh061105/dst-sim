//! Linearizability checker (WGL algorithm) over recorded operation histories.
//!
//! A history is linearizable if every operation can be given a single instant between its
//! invocation and its response such that, replayed in that order on a sequential [`Model`], the
//! model produces exactly the results clients observed. It is the standard correctness condition
//! for a replicated store that should behave like one machine.
//!
//! The search is the Wing & Gong algorithm with Lowe's memoisation, as in Porcupine: try to
//! linearize the earliest pending call; on reaching the response of an operation that is not
//! linearized yet, backtrack; never revisit a (linearized set, model state) pair. Operations that
//! never completed (the client gave up or crashed) have an unknown outcome: they may take effect at
//! any point after their invocation, or not at all.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

/// The sequential specification a history is checked against.
pub trait Model {
    /// The object's state. `Ord` so visited states can be memoised.
    type State: Clone + Ord + Debug;
    /// An operation as invoked.
    type Input: Clone + Debug;
    /// What an operation returned.
    type Output: Clone + PartialEq + Debug;

    /// The state before any operation.
    fn init(&self) -> Self::State;

    /// Runs `input` against `state` as a single machine would.
    fn apply(&self, state: &Self::State, input: &Self::Input) -> (Self::State, Self::Output);

    /// Operations in different partitions never affect each other (say, different keys), so each
    /// partition is checked on its own, which keeps every search small.
    fn partition(&self, _input: &Self::Input) -> u64 {
        0
    }
}

/// One operation: its input, when it was invoked, and when it returned with what (if it did).
/// Times are positions on the history's own logical clock.
#[derive(Clone, Debug)]
pub struct Operation<I, O> {
    pub input: I,
    pub call: u64,
    pub ret: Option<(u64, O)>,
}

/// Names an invoked operation so its response can be recorded later.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct OpId(usize);

/// Invocations and responses in the order they happened.
#[derive(Clone, Debug)]
pub struct History<I, O> {
    ops: Vec<Operation<I, O>>,
    clock: u64,
}

impl<I, O> Default for History<I, O> {
    fn default() -> Self {
        History { ops: Vec::new(), clock: 0 }
    }
}

impl<I, O> History<I, O> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn invoke(&mut self, input: I) -> OpId {
        let call = self.tick();
        self.ops.push(Operation { input, call, ret: None });
        OpId(self.ops.len() - 1)
    }

    pub fn complete(&mut self, id: OpId, output: O) {
        let ret = self.tick();
        let op = &mut self.ops[id.0];
        assert!(op.ret.is_none(), "operation {id:?} completed twice");
        op.ret = Some((ret, output));
    }

    pub fn ops(&self) -> &[Operation<I, O>] {
        &self.ops
    }

    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }
}

/// The result of [`check`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    Linearizable,
    /// No valid order exists for this partition's operations (indexes into [`History::ops`]).
    NotLinearizable {
        partition: u64,
        ops: Vec<usize>,
    },
    /// The search used up its step budget before deciding.
    Unknown {
        partition: u64,
    },
}

/// Checks `history` against `model`, spending at most `budget` search steps per partition.
pub fn check<M: Model>(model: &M, history: &History<M::Input, M::Output>, budget: u64) -> Verdict {
    let mut partitions: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
    for (i, op) in history.ops.iter().enumerate() {
        partitions.entry(model.partition(&op.input)).or_default().push(i);
    }
    for (partition, ops) in partitions {
        match search(model, &history.ops, &ops, budget) {
            Some(true) => {}
            Some(false) => return Verdict::NotLinearizable { partition, ops },
            None => return Verdict::Unknown { partition },
        }
    }
    Verdict::Linearizable
}

/// The WGL search over the operations `ops` (indexes into `all`). `None` means out of budget.
fn search<M: Model>(
    model: &M,
    all: &[Operation<M::Input, M::Output>],
    ops: &[usize],
    budget: u64,
) -> Option<bool> {
    // The timeline of calls and returns. An operation that never returned is given a return after
    // everything else, so it may be placed anywhere after its call, including "after the end"
    // where it changes nothing observable (it never happened).
    let mut events: Vec<(u64, usize, bool)> = Vec::with_capacity(2 * ops.len());
    for (local, &i) in ops.iter().enumerate() {
        events.push((all[i].call, local, true));
        events.push((all[i].ret.as_ref().map_or(u64::MAX, |(t, _)| *t), local, false));
    }
    events.sort_by_key(|&(time, local, is_call)| (time, local, !is_call));
    let mut return_of = vec![0; ops.len()];
    for (index, &(_, local, is_call)) in events.iter().enumerate() {
        if !is_call {
            return_of[local] = index;
        }
    }

    // A doubly linked list over the events, with sentinels, so linearized operations can be
    // unlinked and relinked in O(1) ("dancing links"). Relinking in reverse order of unlinking
    // restores the list exactly, which the backtracking stack guarantees.
    let n = events.len();
    let (head, tail) = (n, n + 1);
    let mut next = vec![0; n + 2];
    let mut prev = vec![0; n + 2];
    for i in 0..n {
        next[i] = if i + 1 < n { i + 1 } else { tail };
        prev[i] = if i > 0 { i - 1 } else { head };
    }
    next[head] = if n > 0 { 0 } else { tail };
    prev[tail] = if n > 0 { n - 1 } else { head };

    let mut state = model.init();
    let mut linearized = vec![0u64; ops.len().div_ceil(64)];
    let mut seen: BTreeSet<(Vec<u64>, M::State)> = BTreeSet::new();
    let mut stack: Vec<(usize, M::State)> = Vec::new();
    let mut cur = next[head];
    let mut steps = 0;

    while next[head] != tail {
        steps += 1;
        if steps > budget {
            return None;
        }
        if cur != tail && events[cur].2 {
            let local = events[cur].1;
            let op = &all[ops[local]];
            let (new_state, output) = model.apply(&state, &op.input);
            if op.ret.as_ref().is_none_or(|(_, observed)| *observed == output) {
                set_bit(&mut linearized, local, true);
                if seen.insert((linearized.clone(), new_state.clone())) {
                    stack.push((cur, std::mem::replace(&mut state, new_state)));
                    unlink(&mut next, &mut prev, cur);
                    unlink(&mut next, &mut prev, return_of[local]);
                    cur = next[head];
                    continue;
                }
                set_bit(&mut linearized, local, false);
            }
            cur = next[cur];
        } else {
            // A response whose call isn't linearized yet: an earlier choice was wrong. With no
            // choice left to undo, no valid order exists.
            let Some((call, old_state)) = stack.pop() else { return Some(false) };
            let local = events[call].1;
            state = old_state;
            set_bit(&mut linearized, local, false);
            relink(&mut next, &mut prev, return_of[local]);
            relink(&mut next, &mut prev, call);
            cur = next[call];
        }
    }
    Some(true)
}

fn unlink(next: &mut [usize], prev: &mut [usize], x: usize) {
    next[prev[x]] = next[x];
    prev[next[x]] = prev[x];
}

fn relink(next: &mut [usize], prev: &mut [usize], x: usize) {
    next[prev[x]] = x;
    prev[next[x]] = x;
}

fn set_bit(bits: &mut [u64], i: usize, on: bool) {
    if on {
        bits[i / 64] |= 1 << (i % 64);
    } else {
        bits[i / 64] &= !(1 << (i % 64));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A register per key, starting at 0.
    struct Register;

    #[derive(Clone, Debug)]
    enum In {
        Write(u8, u64),
        Read(u8),
    }

    impl Model for Register {
        type State = u64;
        type Input = In;
        type Output = Option<u64>;

        fn init(&self) -> u64 {
            0
        }

        fn apply(&self, state: &u64, input: &In) -> (u64, Option<u64>) {
            match *input {
                In::Write(_, v) => (v, None),
                In::Read(_) => (*state, Some(*state)),
            }
        }

        fn partition(&self, input: &In) -> u64 {
            match *input {
                In::Write(key, _) | In::Read(key) => u64::from(key),
            }
        }
    }

    const BUDGET: u64 = 100_000;

    fn verdict(h: &History<In, Option<u64>>) -> Verdict {
        check(&Register, h, BUDGET)
    }

    fn done(h: &mut History<In, Option<u64>>, input: In, output: Option<u64>) {
        let id = h.invoke(input);
        h.complete(id, output);
    }

    #[test]
    fn sequential_history_is_linearizable() {
        let mut h = History::new();
        done(&mut h, In::Write(0, 1), None);
        done(&mut h, In::Read(0), Some(1));
        assert_eq!(verdict(&h), Verdict::Linearizable);
    }

    #[test]
    fn stale_read_after_completed_write_is_caught() {
        let mut h = History::new();
        done(&mut h, In::Write(0, 1), None);
        done(&mut h, In::Read(0), Some(0));
        assert!(matches!(verdict(&h), Verdict::NotLinearizable { .. }));
    }

    #[test]
    fn read_overlapping_a_write_may_see_either_value() {
        for seen in [0, 1] {
            let mut h = History::new();
            let w = h.invoke(In::Write(0, 1));
            done(&mut h, In::Read(0), Some(seen));
            h.complete(w, None);
            assert_eq!(verdict(&h), Verdict::Linearizable, "read saw {seen}");
        }
    }

    #[test]
    fn value_never_written_is_caught() {
        let mut h = History::new();
        done(&mut h, In::Read(0), Some(7));
        assert!(matches!(verdict(&h), Verdict::NotLinearizable { .. }));
    }

    #[test]
    fn unanswered_write_may_or_may_not_have_happened() {
        for seen in [0, 1] {
            let mut h = History::new();
            h.invoke(In::Write(0, 1)); // the client never heard back
            done(&mut h, In::Read(0), Some(seen));
            assert_eq!(verdict(&h), Verdict::Linearizable, "read saw {seen}");
        }
    }

    #[test]
    fn reads_cannot_travel_back_in_time() {
        let mut h = History::new();
        h.invoke(In::Write(0, 1)); // still pending throughout
        done(&mut h, In::Read(0), Some(1));
        done(&mut h, In::Read(0), Some(0)); // starts after the first read finished
        assert!(matches!(verdict(&h), Verdict::NotLinearizable { .. }));
    }

    #[test]
    fn finds_orders_that_need_backtracking() {
        // Concurrent writes of 1 and 2, then a read of 1: only "2 then 1" works, and the search
        // tries "1 then 2" first.
        let mut h = History::new();
        let w1 = h.invoke(In::Write(0, 1));
        let w2 = h.invoke(In::Write(0, 2));
        h.complete(w1, None);
        h.complete(w2, None);
        done(&mut h, In::Read(0), Some(1));
        assert_eq!(verdict(&h), Verdict::Linearizable);
    }

    #[test]
    fn partitions_are_checked_independently() {
        let mut h = History::new();
        done(&mut h, In::Write(5, 1), None);
        done(&mut h, In::Read(5), Some(1)); // key 5 is fine
        done(&mut h, In::Write(9, 1), None);
        done(&mut h, In::Read(9), Some(0)); // key 9 is stale
        match verdict(&h) {
            Verdict::NotLinearizable { partition, ops } => {
                assert_eq!(partition, 9);
                assert_eq!(ops, [2, 3]);
            }
            other => panic!("expected a violation on key 9, got {other:?}"),
        }
    }

    #[test]
    fn running_out_of_budget_is_unknown_not_a_verdict() {
        let mut h = History::new();
        let writes: Vec<_> = (1..=12).map(|v| h.invoke(In::Write(0, v))).collect();
        for w in writes {
            h.complete(w, None);
        }
        done(&mut h, In::Read(0), Some(1));
        assert_eq!(check(&Register, &h, 3), Verdict::Unknown { partition: 0 });
        assert_eq!(check(&Register, &h, BUDGET), Verdict::Linearizable);
    }
}
