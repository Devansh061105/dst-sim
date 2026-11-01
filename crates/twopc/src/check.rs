//! The two-phase commit checker, run after every simulation step against every running node.
//!
//! - **Atomicity:** no transaction is committed anywhere and aborted anywhere else, and no node
//!   changes a decision it made.
//! - **Validity:** a transaction commits only if every participant voted yes; a participant that
//!   commits must itself have voted yes.
//! - **Durability:** a participant never forgets a vote or decision it has acted on (a restart
//!   must recover it from disk).

use std::collections::BTreeMap;
use std::fmt;

use sim_api::NodeId;

use crate::{COORDINATOR, Decision, TwoPcNode, TxState};

/// A broken promise.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Violation {
    /// `tx` was committed by one node and aborted by another (or the same node changed its mind).
    Atomicity {
        /// The transaction.
        tx: u32,
        /// A node that committed it.
        committed: NodeId,
        /// A node that aborted it.
        aborted: NodeId,
    },
    /// `tx` committed although `voter` did not vote yes.
    Validity {
        /// The transaction.
        tx: u32,
        /// A participant that voted no, or never voted, yet the transaction committed.
        voter: NodeId,
    },
    /// A participant lost a vote or decision across a restart.
    Forgot {
        /// The participant.
        node: NodeId,
        /// The transaction.
        tx: u32,
        /// What it had: `"vote"` or `"decision"`.
        what: &'static str,
    },
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Violation::Atomicity { tx, committed, aborted } => {
                write!(
                    f,
                    "atomicity: transaction {tx} committed at {committed} but aborted at {aborted}"
                )
            }
            Violation::Validity { tx, voter } => {
                write!(f, "validity: transaction {tx} committed without a yes vote from {voter}")
            }
            Violation::Forgot { node, tx, what } => {
                write!(
                    f,
                    "durability: {node} forgot its {what} on transaction {tx} after a restart"
                )
            }
        }
    }
}

impl Violation {
    /// Which property broke.
    pub fn property(&self) -> &'static str {
        match self {
            Violation::Atomicity { .. } => "atomicity",
            Violation::Validity { .. } => "validity",
            Violation::Forgot { .. } => "durability",
        }
    }
}

/// Remembers everything seen so far, so later states can be compared with it.
#[derive(Debug, Default)]
pub struct Checker {
    /// The first decision seen for each transaction, and who made it.
    decisions: BTreeMap<u32, (Decision, NodeId)>,
    /// A participant seen voting no, per transaction.
    no_votes: BTreeMap<u32, NodeId>,
    /// The last state seen for each participant's transactions.
    seen: BTreeMap<(NodeId, u32), TxState>,
}

impl Checker {
    /// Checks the running nodes.
    pub fn check<'a>(
        &mut self,
        nodes: impl IntoIterator<Item = (NodeId, &'a TwoPcNode)>,
    ) -> Result<(), Violation> {
        for (id, node) in nodes {
            match node {
                TwoPcNode::Coordinator(c) => {
                    for (tx, &decision) in c.decisions().iter().enumerate() {
                        self.decided(tx as u32, decision, COORDINATOR)?;
                    }
                }
                TwoPcNode::Participant(p) => {
                    for (&tx, &state) in p.txs() {
                        self.participant(id, tx, state)?;
                    }
                    // Anything seen before must still be there.
                    let lost = self
                        .seen
                        .iter()
                        .filter(|((node, _), _)| *node == id)
                        .find(|((_, tx), _)| !p.txs().contains_key(tx));
                    if let Some((&(node, tx), before)) = lost {
                        let what = if before.decision.is_some() { "decision" } else { "vote" };
                        return Err(Violation::Forgot { node, tx, what });
                    }
                }
            }
        }
        Ok(())
    }

    fn participant(&mut self, id: NodeId, tx: u32, now: TxState) -> Result<(), Violation> {
        let before = self.seen.insert((id, tx), now).unwrap_or_default();
        if before.vote.is_some() && now.vote != before.vote {
            return Err(Violation::Forgot { node: id, tx, what: "vote" });
        }
        if before.decision.is_some() && now.decision.is_none() {
            return Err(Violation::Forgot { node: id, tx, what: "decision" });
        }
        if now.vote == Some(false) {
            self.no_votes.entry(tx).or_insert(id);
        }
        if now.decision == Some(Decision::Commit) && now.vote != Some(true) {
            return Err(Violation::Validity { tx, voter: id });
        }
        if let Some(decision) = now.decision {
            self.decided(tx, decision, id)?;
        }
        Ok(())
    }

    fn decided(&mut self, tx: u32, decision: Decision, by: NodeId) -> Result<(), Violation> {
        let &mut (first, first_by) = self.decisions.entry(tx).or_insert((decision, by));
        if first != decision {
            let (committed, aborted) =
                if first == Decision::Commit { (first_by, by) } else { (by, first_by) };
            return Err(Violation::Atomicity { tx, committed, aborted });
        }
        if decision == Decision::Commit
            && let Some(&voter) = self.no_votes.get(&tx)
        {
            return Err(Violation::Validity { tx, voter });
        }
        Ok(())
    }
}
