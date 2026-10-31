use std::cmp::Ordering;
use std::collections::BinaryHeap;

use sim_api::Time;

/// Pending events, popped in order of virtual time; ties go to whichever was pushed first.
///
/// The insertion sequence number is what makes the order fully deterministic: two events at the
/// same instant always run in the order they were scheduled, never in some arbitrary heap order.
#[derive(Debug)]
pub struct EventQueue<E> {
    heap: BinaryHeap<Scheduled<E>>,
    next_seq: u64,
}

#[derive(Debug)]
struct Scheduled<E> {
    at: Time,
    seq: u64,
    event: E,
}

impl<E> EventQueue<E> {
    pub fn new() -> Self {
        EventQueue { heap: BinaryHeap::new(), next_seq: 0 }
    }

    pub fn push(&mut self, at: Time, event: E) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.heap.push(Scheduled { at, seq, event });
    }

    pub fn pop(&mut self) -> Option<(Time, E)> {
        self.heap.pop().map(|s| (s.at, s.event))
    }

    pub fn peek_time(&self) -> Option<Time> {
        self.heap.peek().map(|s| s.at)
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

impl<E> Default for EventQueue<E> {
    fn default() -> Self {
        Self::new()
    }
}

// `BinaryHeap` is a max-heap, so the ordering is reversed: the earliest (time, seq) is "greatest".
impl<E> Ord for Scheduled<E> {
    fn cmp(&self, other: &Self) -> Ordering {
        (other.at, other.seq).cmp(&(self.at, self.seq))
    }
}

impl<E> PartialOrd for Scheduled<E> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<E> PartialEq for Scheduled<E> {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}

impl<E> Eq for Scheduled<E> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pops_in_time_order_with_fifo_ties() {
        let mut q = EventQueue::new();
        q.push(Time::from_nanos(30), "c");
        q.push(Time::from_nanos(10), "a1");
        q.push(Time::from_nanos(20), "b");
        q.push(Time::from_nanos(10), "a2");
        assert_eq!(q.peek_time(), Some(Time::from_nanos(10)));

        let order: Vec<_> = std::iter::from_fn(|| q.pop()).map(|(_, e)| e).collect();
        assert_eq!(order, ["a1", "a2", "b", "c"]);
        assert!(q.is_empty());
    }
}
