use std::collections::btree_map::Entry as MapEntry;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use sim_api::{NodeId, Time};

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
    /// Lease safety: a server held a leader lease (by its own clock) while fewer than a majority
    /// of its configuration would refuse to vote, so a new leader could be elected under it.
    LeaseUnbacked {
        holder: NodeId,
        term: u64,
        refusing: usize,
        members: usize,
    },
    /// Lease safety: a server still held a leader lease (by its own clock) while another server
    /// already led a later term, so both could answer reads.
    OverlappingLeases {
        holder: NodeId,
        term: u64,
        newer: NodeId,
        newer_term: u64,
    },
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Violation::LeaseUnbacked { holder, term, refusing, members } => write!(
                f,
                "lease safety: {holder} held its lease of term {term} while only {refusing} of its                  {members} members would refuse to vote for someone else"
            ),
            Violation::OverlappingLeases { holder, term, newer, newer_term } => write!(
                f,
                "lease safety: {holder} still held its lease of term {term} while {newer} led                  term {newer_term}"
            ),
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
    /// The prefix hash of each committed entry (it and everything before it).
    committed_hashes: Vec<u64>,
    /// How much of the committed prefix each node's own log has been verified against.
    verified: BTreeMap<NodeId, u64>,
    /// (node, term) pairs whose log was already checked for leader completeness.
    leaders_checked: BTreeSet<(NodeId, u64)>,
    /// Per node: the snapshot index the hashes start after, and the prefix hash of each entry
    /// after it.
    prefix_hashes: BTreeMap<NodeId, (u64, Vec<u64>)>,
    /// The prefix hash first seen for each (index, term). Only the leader of a term creates
    /// entries in it and never rewrites them, so an (index, term) pair identifies its whole
    /// prefix, across nodes and across time.
    entry_prefixes: BTreeMap<(u64, u64), u64>,
}

impl Checker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Checks the servers' current state, and how it changed since the last call, against
    /// every property. Call it after every simulation step.
    pub fn check<'a>(
        &mut self,
        nodes: impl IntoIterator<Item = &'a Raft>,
    ) -> Result<(), Violation> {
        nodes.into_iter().try_for_each(|raft| self.check_node(raft))
    }

    /// Lease safety, at one instant. A lease rests on a promise: while a leader holds one, a
    /// majority of its configuration would refuse to vote for anyone else, so no other leader
    /// can be elected. This checks the promise itself (a new leader *could* be elected), not just
    /// its symptom (one *was*), so an unsafe lease is caught whether or not an election happens
    /// to win the race. Each server comes with its own clock's reading, or `None` while it is
    /// down: a down server votes for nobody, and with leases it refuses for an election timeout
    /// after it restarts. (Stateless: what matters is only the present.)
    pub fn check_leases<'a>(
        nodes: impl IntoIterator<Item = (&'a Raft, Option<Time>)>,
    ) -> Result<(), Violation> {
        let nodes: Vec<(&Raft, Option<Time>)> = nodes.into_iter().collect();
        for &(holder, local_now) in &nodes {
            if !local_now.is_some_and(|now| holder.holds_lease(now)) {
                continue;
            }
            let members = holder.members();
            let refusing = members
                .iter()
                .filter(|&&m| {
                    nodes
                        .iter()
                        .find(|(raft, _)| raft.id() == m)
                        .is_none_or(|&(raft, at)| at.is_none_or(|at| raft.refuses_votes(at)))
                })
                .count();
            if refusing < members.len() / 2 + 1 {
                return Err(Violation::LeaseUnbacked {
                    holder: holder.id(),
                    term: holder.term(),
                    refusing,
                    members: members.len(),
                });
            }
        }
        let newest = nodes
            .iter()
            .filter(|(raft, at)| at.is_some() && raft.role() == Role::Leader)
            .max_by_key(|(raft, _)| raft.term());
        let Some(&(newest, _)) = newest else { return Ok(()) };
        for &(raft, local_now) in &nodes {
            if raft.term() < newest.term() && local_now.is_some_and(|now| raft.holds_lease(now)) {
                return Err(Violation::OverlappingLeases {
                    holder: raft.id(),
                    term: raft.term(),
                    newer: newest.id(),
                    newer_term: newest.term(),
                });
            }
        }
        Ok(())
    }

    /// The entries known to be committed, in order.
    pub fn committed(&self) -> &[Entry] {
        &self.committed
    }

    /// The prefix hash of committed entry `index` (0: the empty prefix), if it is known.
    fn committed_hash(&self, index: u64) -> Option<u64> {
        match index {
            0 => Some(PREFIX_SEED),
            _ => self.committed_hashes.get(index as usize - 1).copied(),
        }
    }

    // A node's snapshot stands in for the entries up to its index: they were committed (a node
    // only compacts what it applied), so the checker compares the snapshot's last entry with the
    // committed prefix, and the rest of the log as usual. Nodes compact only entries applied
    // before the current event, so the checker has always seen them committed first.
    fn check_node(&mut self, raft: &Raft) -> Result<(), Violation> {
        let node = raft.id;
        let durable = &raft.durable;
        let base = durable.snapshot.index;
        let last = durable.last_index();
        if raft.commit_index > last {
            return Err(Violation::CommitBeyondLog { node, commit: raft.commit_index, len: last });
        }
        if base > 0
            && let Some(entry) = self.committed.get(base as usize - 1)
            && entry.term != durable.snapshot.term
        {
            return Err(Violation::CommittedEntryDiffers { node, index: base });
        }

        // Log Matching, checked only from the first entry that changed since the last look.
        if let Some(from) = durable.changed_from.take() {
            match self.committed_hash(base) {
                // A snapshot of entries not yet seen committed: look again later.
                None => durable.changed_from.set(Some(from)),
                Some(base_hash) => {
                    let (hashed_base, hashes) = self.prefix_hashes.entry(node).or_default();
                    if *hashed_base != base {
                        *hashed_base = base;
                        hashes.clear();
                    }
                    hashes.truncate(from.saturating_sub(base + 1) as usize);
                    hashes.truncate(durable.log.len());
                    let unhashed = hashes.len();
                    for (i, entry) in durable.log.iter().enumerate().skip(unhashed) {
                        let prev = hashes.last().copied().unwrap_or(base_hash);
                        let hash = prefix_hash(prev, entry);
                        hashes.push(hash);
                        let (index, term) = (base + 1 + i as u64, entry.term);
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
            }
        }

        // The one exception to the rule below: a node whose disk was damaged may lose entries,
        // as long as it knows (it is rejoining, and votes for nobody until it has caught up).
        // Its log can be shorter, or an older log of its own that disagrees with what was
        // committed since; only the prefix that still agrees counts as verified. Election safety
        // and leader completeness still hold the cluster to account.
        if raft.is_rejoining()
            && let Some(verified) = self.verified.get_mut(&node)
        {
            let mut agreed = (*verified).min(last);
            while agreed > base && durable.entry(agreed) != self.committed.get(agreed as usize - 1)
            {
                agreed -= 1;
            }
            *verified = agreed;
        }
        // Entries this node committed must stay in its log (or its snapshot), unchanged, even
        // across restarts. Checking the last verified one is enough: replacing an earlier one
        // means truncating past it.
        let verified = self.verified.get(&node).copied().unwrap_or(0);
        if verified > base
            && durable.entry(verified) != Some(&self.committed[verified as usize - 1])
        {
            return Err(Violation::CommittedEntryLost { node, index: verified });
        }

        // State machine safety: newly committed entries match what others committed there.
        let mut checked = verified;
        for index in verified + 1..=raft.commit_index {
            let i = (index - 1) as usize;
            match (self.committed.get(i), durable.entry(index)) {
                (Some(entry), Some(mine)) if entry != mine => {
                    return Err(Violation::CommittedEntryDiffers { node, index });
                }
                // Compacted here: covered by the snapshot check above.
                (Some(_), _) => {}
                (None, Some(mine)) => {
                    let prev = self.committed_hash(index - 1).expect("committed in order");
                    self.committed.push(mine.clone());
                    self.committed_in.push(durable.term);
                    self.committed_hashes.push(prefix_hash(prev, mine));
                }
                // Compacted, and never seen committed elsewhere: can't check it yet.
                (None, None) => break,
            }
            checked = index;
        }
        if checked > verified {
            self.verified.insert(node, checked);
        }

        if raft.role == Role::Leader {
            let term = durable.term;
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
            // Entries in its snapshot it holds by definition.
            if self.leaders_checked.insert((node, term)) {
                let committed = self.committed.iter().zip(&self.committed_in);
                for (i, (entry, &committed_in)) in committed.enumerate() {
                    let index = i as u64 + 1;
                    if committed_in < term && index > base && durable.entry(index) != Some(entry) {
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
        Entry { term, data: vec![byte], config: None }
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
