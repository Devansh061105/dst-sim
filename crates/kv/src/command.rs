use std::collections::BTreeMap;

/// A client operation on the store. Keys and values are small numbers to keep traces readable.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum Op {
    Get {
        key: u8,
    },
    Put {
        key: u8,
        value: u64,
    },
    /// Set `key` to `new` only if it currently holds `expected` (`None` = absent).
    Cas {
        key: u8,
        expected: Option<u64>,
        new: u64,
    },
}

/// What an operation returned to the client.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum OpResult {
    Value(Option<u64>),
    Written,
    Cas { swapped: bool },
}

impl Op {
    /// Applies the operation to the store: the sequential specification of the service.
    pub fn apply(&self, store: &mut BTreeMap<u8, u64>) -> OpResult {
        match *self {
            Op::Get { key } => OpResult::Value(store.get(&key).copied()),
            Op::Put { key, value } => {
                store.insert(key, value);
                OpResult::Written
            }
            Op::Cas { key, expected, new } => {
                let swapped = store.get(&key).copied() == expected;
                if swapped {
                    store.insert(key, new);
                }
                OpResult::Cas { swapped }
            }
        }
    }
}

/// Identifies one client operation across retries: the client's random session id and the
/// operation's sequence number within that session.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct RequestId {
    pub session: u64,
    pub seq: u64,
}

/// What goes into a Raft log entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Command {
    pub id: RequestId,
    pub op: Op,
}

impl Command {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(36);
        out.extend(self.id.session.to_le_bytes());
        out.extend(self.id.seq.to_le_bytes());
        match self.op {
            Op::Get { key } => out.extend([0, key]),
            Op::Put { key, value } => {
                out.extend([1, key]);
                out.extend(value.to_le_bytes());
            }
            Op::Cas { key, expected, new } => {
                out.extend([2, key]);
                match expected {
                    None => out.push(0),
                    Some(v) => {
                        out.push(1);
                        out.extend(v.to_le_bytes());
                    }
                }
                out.extend(new.to_le_bytes());
            }
        }
        out
    }

    /// Decodes an entry this crate encoded. Log entries are checksummed on disk, so malformed
    /// input means a bug, and panics.
    pub fn decode(bytes: &[u8]) -> Command {
        let mut r = Reader(bytes);
        let id = RequestId { session: r.u64(), seq: r.u64() };
        let op = match r.u8() {
            0 => Op::Get { key: r.u8() },
            1 => Op::Put { key: r.u8(), value: r.u64() },
            2 => {
                let key = r.u8();
                let expected = if r.u8() == 1 { Some(r.u64()) } else { None };
                Op::Cas { key, expected, new: r.u64() }
            }
            tag => panic!("unknown command tag {tag}"),
        };
        assert!(r.0.is_empty(), "trailing bytes after command");
        Command { id, op }
    }
}

/// The replicated state, as a snapshot holds it: the store and the per-session table of the last
/// applied write and its result (without the table, a retried write would apply twice).
pub(crate) type Sessions = BTreeMap<u64, (u64, OpResult)>;

pub(crate) fn encode_state(store: &BTreeMap<u8, u64>, sessions: &Sessions) -> Vec<u8> {
    let mut out = (store.len() as u64).to_le_bytes().to_vec();
    for (&key, value) in store {
        out.push(key);
        out.extend(value.to_le_bytes());
    }
    out.extend((sessions.len() as u64).to_le_bytes());
    for (session, (seq, result)) in sessions {
        out.extend(session.to_le_bytes());
        out.extend(seq.to_le_bytes());
        match result {
            OpResult::Value(None) => out.push(0),
            OpResult::Value(Some(v)) => {
                out.push(1);
                out.extend(v.to_le_bytes());
            }
            OpResult::Written => out.push(2),
            OpResult::Cas { swapped } => out.push(3 + u8::from(*swapped)),
        }
    }
    out
}

/// Decodes a snapshot this crate encoded (checksummed on disk, so malformed input panics).
pub(crate) fn decode_state(bytes: &[u8]) -> (BTreeMap<u8, u64>, Sessions) {
    let mut r = Reader(bytes);
    let store = (0..r.u64()).map(|_| (r.u8(), r.u64())).collect();
    let sessions = (0..r.u64())
        .map(|_| {
            let (session, seq) = (r.u64(), r.u64());
            let result = match r.u8() {
                0 => OpResult::Value(None),
                1 => OpResult::Value(Some(r.u64())),
                2 => OpResult::Written,
                tag => OpResult::Cas { swapped: tag == 4 },
            };
            (session, (seq, result))
        })
        .collect();
    assert!(r.0.is_empty(), "trailing bytes after snapshot");
    (store, sessions)
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn u8(&mut self) -> u8 {
        let (&b, rest) = self.0.split_first().expect("command truncated");
        self.0 = rest;
        b
    }

    fn u64(&mut self) -> u64 {
        let (head, rest) = self.0.split_at(8);
        self.0 = rest;
        u64::from_le_bytes(head.try_into().expect("8 bytes"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_round_trip() {
        let id = RequestId { session: 0xdead_beef, seq: 7 };
        for op in [
            Op::Get { key: 1 },
            Op::Put { key: 2, value: 99 },
            Op::Cas { key: 3, expected: None, new: 5 },
            Op::Cas { key: 3, expected: Some(5), new: 6 },
        ] {
            let cmd = Command { id, op };
            assert_eq!(Command::decode(&cmd.encode()), cmd);
        }
    }

    #[test]
    fn state_round_trips() {
        let store = BTreeMap::from([(0, 7), (2, u64::MAX)]);
        let sessions = BTreeMap::from([
            (1, (3, OpResult::Value(None))),
            (2, (1, OpResult::Value(Some(9)))),
            (5, (8, OpResult::Written)),
            (6, (2, OpResult::Cas { swapped: true })),
            (7, (4, OpResult::Cas { swapped: false })),
        ]);
        assert_eq!(decode_state(&encode_state(&store, &sessions)), (store, sessions));
        assert_eq!(decode_state(&encode_state(&BTreeMap::new(), &BTreeMap::new())).0.len(), 0);
    }

    #[test]
    fn cas_only_swaps_on_match() {
        let mut store = BTreeMap::new();
        assert_eq!(
            Op::Cas { key: 1, expected: Some(1), new: 2 }.apply(&mut store),
            OpResult::Cas { swapped: false }
        );
        assert_eq!(
            Op::Cas { key: 1, expected: None, new: 2 }.apply(&mut store),
            OpResult::Cas { swapped: true }
        );
        assert_eq!(Op::Get { key: 1 }.apply(&mut store), OpResult::Value(Some(2)));
    }
}
