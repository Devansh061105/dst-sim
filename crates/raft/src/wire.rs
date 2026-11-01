//! Byte encoding of [`RaftMsg`], for running over a real network.

use sim_api::NodeId;
use sim_api::wire::{Reader, put_bytes, put_u32, put_u64};

use crate::{Entry, RaftMsg, SnapshotMembers};

fn put_members(out: &mut Vec<u8>, members: Option<&Vec<NodeId>>) {
    match members {
        None => out.push(0),
        Some(members) => {
            out.push(1);
            put_u32(out, members.len() as u32);
            for m in members {
                put_u32(out, m.0);
            }
        }
    }
}

fn members(r: &mut Reader<'_>) -> Option<Option<Vec<NodeId>>> {
    if !r.bool()? {
        return Some(None);
    }
    let count = r.u32()?;
    // Grown as ids are read (not sized from `count`), so a corrupt count runs out of bytes and
    // fails instead of asking for a huge allocation.
    let mut members = Vec::new();
    for _ in 0..count {
        members.push(NodeId(r.u32()?));
    }
    Some(Some(members))
}

impl RaftMsg {
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            RaftMsg::RequestVote { term, last_log_index, last_log_term } => {
                out.push(0);
                for v in [term, last_log_index, last_log_term] {
                    put_u64(out, *v);
                }
            }
            RaftMsg::Vote { term, granted } => {
                out.push(1);
                put_u64(out, *term);
                out.push(u8::from(*granted));
            }
            RaftMsg::AppendEntries {
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_round,
            } => {
                out.push(2);
                for v in [term, prev_log_index, prev_log_term, leader_commit, read_round] {
                    put_u64(out, *v);
                }
                put_u64(out, entries.len() as u64);
                for entry in entries {
                    put_u64(out, entry.term);
                    put_bytes(out, &entry.data);
                    put_members(out, entry.config.as_ref());
                }
            }
            RaftMsg::AppendResult { term, success, index, read_round } => {
                out.push(3);
                put_u64(out, *term);
                out.push(u8::from(*success));
                put_u64(out, *index);
                put_u64(out, *read_round);
            }
            RaftMsg::InstallSnapshot { term, index, last_term, data, read_round, members } => {
                out.push(4);
                for v in [term, index, last_term, read_round] {
                    put_u64(out, *v);
                }
                put_bytes(out, data);
                put_members(out, members.0.as_ref());
            }
        }
    }

    /// Reads a message [`encode`](RaftMsg::encode) wrote; `None` if the bytes are not one.
    pub fn decode(r: &mut Reader<'_>) -> Option<RaftMsg> {
        Some(match r.u8()? {
            0 => RaftMsg::RequestVote {
                term: r.u64()?,
                last_log_index: r.u64()?,
                last_log_term: r.u64()?,
            },
            1 => RaftMsg::Vote { term: r.u64()?, granted: r.bool()? },
            2 => {
                let (term, prev_log_index, prev_log_term) = (r.u64()?, r.u64()?, r.u64()?);
                let (leader_commit, read_round) = (r.u64()?, r.u64()?);
                let count = r.u64()?;
                let mut entries = Vec::new();
                for _ in 0..count {
                    let (term, data) = (r.u64()?, r.bytes()?.to_vec());
                    entries.push(Entry { term, data, config: members(r)? });
                }
                RaftMsg::AppendEntries {
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    read_round,
                }
            }
            3 => RaftMsg::AppendResult {
                term: r.u64()?,
                success: r.bool()?,
                index: r.u64()?,
                read_round: r.u64()?,
            },
            4 => {
                let (term, index, last_term, read_round) = (r.u64()?, r.u64()?, r.u64()?, r.u64()?);
                let data = r.bytes()?.to_vec();
                RaftMsg::InstallSnapshot {
                    term,
                    index,
                    last_term,
                    data,
                    read_round,
                    members: SnapshotMembers(members(r)?),
                }
            }
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_message_round_trips() {
        let entries = vec![
            Entry { term: 3, data: vec![], config: Some(vec![NodeId(0), NodeId(2)]) },
            Entry { term: 4, data: vec![9; 30], config: None },
        ];
        let messages = [
            RaftMsg::RequestVote { term: 5, last_log_index: 10, last_log_term: 4 },
            RaftMsg::Vote { term: 5, granted: true },
            RaftMsg::AppendEntries {
                term: 5,
                prev_log_index: 8,
                prev_log_term: 3,
                entries,
                leader_commit: 7,
                read_round: 2,
            },
            RaftMsg::AppendResult { term: 5, success: false, index: 4, read_round: 1 },
            RaftMsg::InstallSnapshot {
                term: 6,
                index: 40,
                last_term: 5,
                data: vec![1, 2],
                read_round: 3,
                members: SnapshotMembers(Some(vec![NodeId(1)])),
            },
        ];
        for msg in messages {
            let mut bytes = Vec::new();
            msg.encode(&mut bytes);
            let mut r = Reader::new(&bytes);
            let decoded = RaftMsg::decode(&mut r).and_then(|m| r.finish(m));
            assert_eq!(format!("{decoded:?}"), format!("{:?}", Some(msg)));
            // Every truncation is rejected rather than misread.
            for cut in 0..bytes.len() {
                let mut r = Reader::new(&bytes[..cut]);
                assert!(RaftMsg::decode(&mut r).and_then(|m| r.finish(m)).is_none(), "cut {cut}");
            }
        }
    }
}
