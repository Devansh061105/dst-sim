use std::collections::btree_map::Entry as MapEntry;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use sim_api::NodeId;

use crate::{Entry, Raft, Role};

/// A broken Raft safety property.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Violation {
    /// Election Safety: two different leaders were seen in the same term.
    TwoLeaders {
        term: u64,
        first: NodeId,
        second: NodeId,
    },
    /// State Machine Safety: a node committed a different entry at an index than another node.
    CommittedEntryDiffers {
        node: NodeId,
        index: u64,
    },
    /// An entry this node had committed is gone from its log or was replaced.
    CommittedEntryLost {
        node: NodeId,
        index: u64,
    },
    /// Leader Completeness: a new leader's log lacks an entry that was already committed.
    LeaderMissingCommitted {
        node: NodeId,
        term: u64,
        index: u64,
    },
    CommitBeyondLog {
        node: NodeId,
        commit: u64,
        len: u64,
    },
    /// Log Matching: an entry with this index and term was seen before with a different log
    /// before it.
    LogMatching {
        node: NodeId,
        index: u64,
        term: u64,
    },
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Violation::TwoLeaders { term, first, second } => {
                write!(f, "election safety: {first} and {second} both led term {term}")
            }
            Violation::CommittedEntryDiffers { node, index } => {
                write!(
                    f,
                    "state machine safety: {node} committed a different entry at index {index}"
                )
            }
            Violation::CommittedEntryLost { node, index } => {
                write!(f, "{node} lost committed entry {index}")
            }
            Violation::LeaderMissingCommitted { node, term, index } => {
                write!(
                    f,
                    "leader completeness: {node} leads term {term} without committed entry {index}"
                )
            }
            Violation::CommitBeyondLog { node, commit, len } => {
                write!(f, "{node} has commit index {commit} but only {len} log entries")
            }
            Violation::LogMatching { node, index, term } => {
                write!(
                    f,
                    "log matching: {node} has entry {index} of term {term} after a different log"
                )
            }
        }
    }
}

/// Checks Raft's safety properties (Figure 3 of the paper) across a cluster. Call
/// [`check`](Checker::check) after every simulation step with the nodes that are running.
///
/// Every check is incremental, so the cost per step stays small however long the logs get.
#[derive(Debug, Default)]
pub struct Checker {
    /// The leader seen for each term, over the whole run.
    leaders: BTreeMap<u64, NodeId>,
    /// The committed prefix of the log, as agreed so far.
    committed: Vec<Entry>,
    /// For each committed entry, the term of the node on which the checker first saw it
    /// committed: never earlier than the term it was actually committed in.
    committed_in: Vec<u64>,
    /// How much of the committed prefix each node's own log has been verified against.
    verified: BTreeMap<NodeId, u64>,
    /// (node, term) pairs whose log was already checked for leader completeness.
    leaders_checked: BTreeSet<(NodeId, u64)>,
    /// Per node, a hash of each log prefix (entry `i` and everything before it).
    prefix_hashes: BTreeMap<NodeId, Vec<u64>>,
    /// The prefix hash first seen for each (index, term). Only the leader of a term creates
    /// entries in it and never rewrites them, so an (index, term) pair identifies its whole
    /// prefix, across nodes and across time.
    entry_prefixes: BTreeMap<(u64, u64), u64>,
}

impl Checker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn check<'a>(
        &mut self,
        nodes: impl IntoIterator<Item = &'a Raft>,
    ) -> Result<(), Violation> {
        nodes.into_iter().try_for_each(|raft| self.check_node(raft))
    }

    /// The entries known to be committed, in order.
    pub fn committed(&self) -> &[Entry] {
        &self.committed
    }

    fn check_node(&mut self, raft: &Raft) -> Result<(), Violation> {
        let node = raft.id;
        let log = &raft.durable.log;
        let len = log.len() as u64;
        if raft.commit_index > len {
            return Err(Violation::CommitBeyondLog { node, commit: raft.commit_index, len });
        }

        // Log Matching, checked only from the first entry that changed since the last look.
        if let Some(from) = raft.durable.changed_from.take() {
            let hashes = self.prefix_hashes.entry(node).or_default();
            hashes.truncate((from - 1) as usize);
            hashes.truncate(log.len());
            let unhashed = hashes.len();
            for (i, entry) in log.iter().enumerate().skip(unhashed) {
                let prev = hashes.last().copied().unwrap_or(PREFIX_SEED);
                let hash = prefix_hash(prev, entry);
                hashes.push(hash);
                let (index, term) = (i as u64 + 1, entry.term);
                match self.entry_prefixes.entry((index, term)) {
                    MapEntry::Occupied(e) if *e.get() != hash => {
                        return Err(Violation::LogMatching { node, index, term });
                    }
                    MapEntry::Occupied(_) => {}
                    MapEntry::Vacant(e) => {
                        e.insert(hash);
                    }
                }
            }
        }

        // Entries this node committed must stay in its log, unchanged, even across restarts.
        // Checking the last verified one is enough: replacing an earlier one means truncating
        // past it.
        let verified = self.verified.get(&node).copied().unwrap_or(0);
        if verified > 0 {
            let i = (verified - 1) as usize;
            if log.get(i) != Some(&self.committed[i]) {
                return Err(Violation::CommittedEntryLost { node, index: verified });
            }
        }

        // State machine safety: newly committed entries match what others committed there.
        if raft.commit_index > verified {
            for index in verified + 1..=raft.commit_index {
                let i = (index - 1) as usize;
                match self.committed.get(i) {
                    Some(entry) if *entry != log[i] => {
                        return Err(Violation::CommittedEntryDiffers { node, index });
                    }
                    Some(_) => {}
                    None => {
                        self.committed.push(log[i].clone());
                        self.committed_in.push(raft.durable.term);
                    }
                }
            }
            self.verified.insert(node, raft.commit_index);
        }

        if raft.role == Role::Leader {
            let term = raft.durable.term;
            match self.leaders.entry(term) {
                MapEntry::Occupied(e) if *e.get() != node => {
                    return Err(Violation::TwoLeaders { term, first: *e.get(), second: node });
                }
                MapEntry::Occupied(_) => {}
                MapEntry::Vacant(e) => {
                    e.insert(node);
                }
            }
            // Leader Completeness only promises that leaders of *later* terms hold a committed
            // entry. A stale leader of an older term (cut off, elected by delayed votes) may
            // legitimately lack entries committed in newer terms; it can never commit anything.
            if self.leaders_checked.insert((node, term)) {
                let committed = self.committed.iter().zip(&self.committed_in);
                for (i, (entry, &committed_in)) in committed.enumerate() {
                    if committed_in < term && log.get(i) != Some(entry) {
                        let index = i as u64 + 1;
                        return Err(Violation::LeaderMissingCommitted { node, term, index });
                    }
                }
            }
        }
        Ok(())
    }
}

const PREFIX_SEED: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a over the previous prefix hash and one entry, so equal hashes mean equal prefixes.
fn prefix_hash(prev: u64, entry: &Entry) -> u64 {
    let mut h = PREFIX_SEED;
    let len = entry.data.len() as u64;
    for b in prev.to_le_bytes().iter().chain(&entry.term.to_le_bytes()).chain(&len.to_le_bytes()) {
        h = (h ^ u64::from(*b)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    for &b in &entry.data {
        h = (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RaftConfig;

    fn entry(term: u64, byte: u8) -> Entry {
        Entry { term, data: vec![byte] }
    }

    fn node(id: u32, term: u64, role: Role, log: Vec<Entry>, commit: u64) -> Raft {
        let members = (0..3).map(NodeId).collect();
        let mut raft = Raft::new(NodeId(id), RaftConfig::new(members));
        raft.durable.term = term;
        raft.durable.log = log;
        raft.durable.changed_from.set(Some(1));
        raft.role = role;
        raft.commit_index = commit;
        raft
    }

    #[test]
    fn log_matching_violation_is_caught() {
        // Entry 2 of term 2 on both nodes, but different entries (of different terms) before it.
        let a = node(0, 2, Role::Follower, vec![entry(1, 1), entry(2, 2)], 0);
        let b = node(1, 2, Role::Follower, vec![entry(2, 9), entry(2, 2)], 0);
        let err = Checker::new().check([&a, &b]).unwrap_err();
        assert_eq!(err, Violation::LogMatching { node: NodeId(1), index: 2, term: 2 });
    }

    #[test]
    fn same_node_rewriting_its_prefix_is_caught_across_time() {
        let mut checker = Checker::new();
        let before = node(0, 2, Role::Follower, vec![entry(1, 1), entry(2, 2)], 0);
        checker.check([&before]).unwrap();
        let after_splice = node(0, 2, Role::Follower, vec![entry(1, 7), entry(2, 2)], 0);
        assert!(matches!(checker.check([&after_splice]), Err(Violation::LogMatching { .. })));
    }

    #[test]
    fn consistent_cluster_passes() {
        let log = vec![entry(1, 1), entry(1, 2)];
        let a = node(0, 1, Role::Leader, log.clone(), 2);
        let b = node(1, 1, Role::Follower, log, 1);
        let mut checker = Checker::new();
        assert_eq!(checker.check([&a, &b]), Ok(()));
        assert_eq!(checker.committed().len(), 2);
    }

    #[test]
    fn two_leaders_in_one_term_are_caught() {
        let a = node(0, 3, Role::Leader, vec![], 0);
        let b = node(1, 3, Role::Leader, vec![], 0);
        let err = Checker::new().check([&a, &b]).unwrap_err();
        assert!(matches!(err, Violation::TwoLeaders { term: 3, .. }), "{err}");
    }

    #[test]
    fn divergent_commits_are_caught() {
        // Different terms at index 1, so Log Matching itself has nothing to compare.
        let a = node(0, 2, Role::Follower, vec![entry(1, 1)], 1);
        let b = node(1, 2, Role::Follower, vec![entry(2, 9)], 1);
        let err = Checker::new().check([&a, &b]).unwrap_err();
        assert_eq!(err, Violation::CommittedEntryDiffers { node: NodeId(1), index: 1 });
    }

    #[test]
    fn lost_committed_entry_is_caught() {
        let mut checker = Checker::new();
        let before = node(0, 1, Role::Follower, vec![entry(1, 1), entry(1, 2)], 2);
        checker.check([&before]).unwrap();
        let after_bad_recovery = node(0, 1, Role::Follower, vec![entry(1, 1)], 0);
        let err = checker.check([&after_bad_recovery]).unwrap_err();
        assert_eq!(err, Violation::CommittedEntryLost { node: NodeId(0), index: 2 });
    }

    #[test]
    fn leader_missing_committed_entry_is_caught() {
        let mut checker = Checker::new();
        let old = node(0, 1, Role::Follower, vec![entry(1, 1), entry(1, 2)], 2);
        checker.check([&old]).unwrap();
        let new_leader = node(1, 2, Role::Leader, vec![entry(1, 1)], 1);
        let err = checker.check([&new_leader]).unwrap_err();
        assert_eq!(err, Violation::LeaderMissingCommitted { node: NodeId(1), term: 2, index: 2 });
    }

    /// Regression (swarm seed 11644): a cut-off node won an older term through delayed votes
    /// after a newer term had already committed an entry it lacks. That is legal Raft.
    #[test]
    fn stale_leader_of_older_term_may_lack_newer_commits() {
        let mut checker = Checker::new();
        let newer = node(0, 10, Role::Leader, vec![entry(8, 1), entry(10, 0)], 2);
        checker.check([&newer]).unwrap();
        let stale = node(2, 9, Role::Leader, vec![entry(8, 1)], 1);
        assert_eq!(checker.check([&stale]), Ok(()));
    }
}
