//! State coverage: how much of the Raft cluster's behaviour a set of runs has explored.
//!
//! Exact states are useless for this (terms and indexes grow forever, so every state is new).
//! Instead each server is reduced to a few facts that matter to the protocol: is it up, its role,
//! whether its term, log and commit index lag behind the most advanced server, and whether it is
//! rejoining after disk damage. The servers' facts are sorted, so states that differ only in
//! which server is which count once, and hashed. A run's coverage is the set of these hashes over
//! all its steps; a campaign's is the union over its runs.

use raft::{Raft, Role};

/// The abstract state of a cluster; `None` for a server that is down.
pub fn abstract_state<'a>(servers: impl IntoIterator<Item = Option<&'a Raft>>) -> u64 {
    let servers: Vec<Option<&Raft>> = servers.into_iter().collect();
    let up = || servers.iter().flatten();
    let max_term = up().map(|r| r.term()).max().unwrap_or(0);
    let max_log = up().map(|r| r.last_log_index()).max().unwrap_or(0);
    let max_commit = up().map(|r| r.commit_index()).max().unwrap_or(0);
    let mut facts: Vec<u8> = servers
        .iter()
        .map(|server| match server {
            None => 0,
            Some(r) => {
                let role = match r.role() {
                    Role::Follower => 0,
                    Role::Candidate => 1,
                    Role::Leader => 2,
                };
                1 + (role << 4
                    | u8::from(r.term() < max_term) << 3
                    | u8::from(r.last_log_index() < max_log) << 2
                    | u8::from(r.commit_index() < max_commit) << 1
                    | u8::from(r.is_rejoining()))
            }
        })
        .collect();
    facts.sort_unstable();
    // FNV-1a: stable across Rust versions and platforms, unlike the std hasher.
    facts
        .iter()
        .fold(0xcbf2_9ce4_8422_2325, |h, &b| (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3))
}
