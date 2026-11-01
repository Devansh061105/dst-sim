//! Byte encoding of [`KvMsg`], for running over a real network.

use raft::RaftMsg;
use sim_api::NodeId;
use sim_api::wire::{Reader, put_u32, put_u64};

use crate::command::{Op, OpResult, RequestId};
use crate::{Change, KvMsg, ReconfigReply, Reply};

impl KvMsg {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            KvMsg::Raft(msg) => {
                out.push(0);
                msg.encode(&mut out);
            }
            KvMsg::Request { id, op } => {
                out.push(1);
                put_id(&mut out, *id);
                put_op(&mut out, op);
            }
            KvMsg::Response { id, reply } => {
                out.push(2);
                put_id(&mut out, *id);
                match reply {
                    Reply::Done(result) => {
                        out.push(0);
                        put_result(&mut out, result);
                    }
                    Reply::NotLeader(hint) => {
                        out.push(1);
                        put_u32(&mut out, hint.map_or(u32::MAX, |n| n.0));
                    }
                }
            }
            KvMsg::Reconfigure { id, change } => {
                out.push(3);
                put_u64(&mut out, *id);
                let (kind, server) = match change {
                    Change::Add(s) => (0, s),
                    Change::Remove(s) => (1, s),
                };
                out.push(kind);
                put_u32(&mut out, server.0);
            }
            KvMsg::Reconfigured { id, reply, members } => {
                out.push(4);
                put_u64(&mut out, *id);
                match reply {
                    ReconfigReply::Accepted => out.push(0),
                    ReconfigReply::Busy => out.push(1),
                    ReconfigReply::NotLeader(hint) => {
                        out.push(2);
                        put_u32(&mut out, hint.map_or(u32::MAX, |n| n.0));
                    }
                }
                put_u32(&mut out, members.len() as u32);
                for m in members {
                    put_u32(&mut out, m.0);
                }
            }
        }
        out
    }

    /// Reads a message [`to_bytes`](KvMsg::to_bytes) wrote; `None` if the bytes are not one.
    pub fn from_bytes(bytes: &[u8]) -> Option<KvMsg> {
        let mut r = Reader::new(bytes);
        let msg = match r.u8()? {
            0 => KvMsg::Raft(RaftMsg::decode(&mut r)?),
            1 => KvMsg::Request { id: id(&mut r)?, op: op(&mut r)? },
            2 => {
                let id = id(&mut r)?;
                let reply = match r.u8()? {
                    0 => Reply::Done(result(&mut r)?),
                    1 => Reply::NotLeader(Some(r.u32()?).filter(|&n| n != u32::MAX).map(NodeId)),
                    _ => return None,
                };
                KvMsg::Response { id, reply }
            }
            3 => {
                let id = r.u64()?;
                let change = match (r.u8()?, NodeId(r.u32()?)) {
                    (0, server) => Change::Add(server),
                    (1, server) => Change::Remove(server),
                    _ => return None,
                };
                KvMsg::Reconfigure { id, change }
            }
            4 => {
                let id = r.u64()?;
                let reply = match r.u8()? {
                    0 => ReconfigReply::Accepted,
                    1 => ReconfigReply::Busy,
                    2 => ReconfigReply::NotLeader(
                        Some(r.u32()?).filter(|&n| n != u32::MAX).map(NodeId),
                    ),
                    _ => return None,
                };
                let count = r.u32()?;
                let mut members = Vec::new();
                for _ in 0..count {
                    members.push(NodeId(r.u32()?));
                }
                KvMsg::Reconfigured { id, reply, members }
            }
            _ => return None,
        };
        r.finish(msg)
    }
}

fn put_id(out: &mut Vec<u8>, id: RequestId) {
    put_u64(out, id.session);
    put_u64(out, id.seq);
}

fn id(r: &mut Reader<'_>) -> Option<RequestId> {
    Some(RequestId { session: r.u64()?, seq: r.u64()? })
}

fn put_op(out: &mut Vec<u8>, op: &Op) {
    match *op {
        Op::Get { key } => out.extend([0, key]),
        Op::Put { key, value } => {
            out.extend([1, key]);
            put_u64(out, value);
        }
        Op::Cas { key, expected, new } => {
            out.extend([2, key]);
            put_option(out, expected);
            put_u64(out, new);
        }
    }
}

fn op(r: &mut Reader<'_>) -> Option<Op> {
    Some(match r.u8()? {
        0 => Op::Get { key: r.u8()? },
        1 => Op::Put { key: r.u8()?, value: r.u64()? },
        2 => Op::Cas { key: r.u8()?, expected: option(r)?, new: r.u64()? },
        _ => return None,
    })
}

fn put_result(out: &mut Vec<u8>, result: &OpResult) {
    match *result {
        OpResult::Value(v) => {
            out.push(0);
            put_option(out, v);
        }
        OpResult::Written => out.push(1),
        OpResult::Cas { swapped } => out.extend([2, u8::from(swapped)]),
    }
}

fn result(r: &mut Reader<'_>) -> Option<OpResult> {
    Some(match r.u8()? {
        0 => OpResult::Value(option(r)?),
        1 => OpResult::Written,
        2 => OpResult::Cas { swapped: r.bool()? },
        _ => return None,
    })
}

fn put_option(out: &mut Vec<u8>, v: Option<u64>) {
    match v {
        None => out.push(0),
        Some(v) => {
            out.push(1);
            put_u64(out, v);
        }
    }
}

fn option(r: &mut Reader<'_>) -> Option<Option<u64>> {
    match r.u8()? {
        0 => Some(None),
        1 => Some(Some(r.u64()?)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_message_round_trips() {
        let id = RequestId { session: 99, seq: 3 };
        let messages = [
            KvMsg::Raft(RaftMsg::Vote { term: 2, granted: true }),
            KvMsg::Request { id, op: Op::Get { key: 1 } },
            KvMsg::Request { id, op: Op::Put { key: 2, value: 5 } },
            KvMsg::Request { id, op: Op::Cas { key: 0, expected: None, new: 1 } },
            KvMsg::Request { id, op: Op::Cas { key: 0, expected: Some(1), new: 2 } },
            KvMsg::Response { id, reply: Reply::Done(OpResult::Value(None)) },
            KvMsg::Response { id, reply: Reply::Done(OpResult::Value(Some(4))) },
            KvMsg::Response { id, reply: Reply::Done(OpResult::Written) },
            KvMsg::Response { id, reply: Reply::Done(OpResult::Cas { swapped: true }) },
            KvMsg::Response { id, reply: Reply::NotLeader(None) },
            KvMsg::Response { id, reply: Reply::NotLeader(Some(NodeId(2))) },
            KvMsg::Reconfigure { id: 7, change: Change::Add(NodeId(3)) },
            KvMsg::Reconfigure { id: 8, change: Change::Remove(NodeId(0)) },
            KvMsg::Reconfigured { id: 7, reply: ReconfigReply::Accepted, members: vec![NodeId(1)] },
            KvMsg::Reconfigured {
                id: 9,
                reply: ReconfigReply::NotLeader(Some(NodeId(4))),
                members: vec![],
            },
        ];
        for msg in messages {
            let bytes = msg.to_bytes();
            let decoded = KvMsg::from_bytes(&bytes);
            assert_eq!(format!("{decoded:?}"), format!("{:?}", Some(&msg)));
            for cut in 0..bytes.len() {
                assert!(KvMsg::from_bytes(&bytes[..cut]).is_none(), "{msg:?} cut at {cut}");
            }
        }
    }
}
