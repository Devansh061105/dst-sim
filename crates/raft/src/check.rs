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
    /// How much of the committed prefix each node's own log has been verified against.
    verified: BTreeMap<NodeId, u64>,
    /// (node, term) pairs whose log was already checked for leader completeness.
    leaders_checked: BTreeSet<(NodeId, u64)>,
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
                    None => self.committed.push(log[i].clone()),
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
            if self.leaders_checked.insert((node, term)) {
                for (i, entry) in self.committed.iter().enumerate() {
                    if log.get(i) != Some(entry) {
                        let index = i as u64 + 1;
                        return Err(Violation::LeaderMissingCommitted { node, term, index });
                    }
                }
            }
        }
        Ok(())
    }
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
        raft.role = role;
        raft.commit_index = commit;
        raft
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
        let a = node(0, 1, Role::Follower, vec![entry(1, 1)], 1);
        let b = node(1, 1, Role::Follower, vec![entry(1, 9)], 1);
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
}
