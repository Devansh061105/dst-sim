use sim_api::{NodeId, Storage, SyncError, record};

use crate::Entry;

const META_FILE: &str = "raft-meta";
const LOG_FILE: &str = "raft-log";
const NO_VOTE: u32 = u32::MAX;

/// Raft's persistent state (Figure 2): the current term, the vote, and the log, mirrored on disk.
///
/// The term and vote are appended to their own file as small records, newest last, instead of
/// being overwritten in place: a crash in the middle of a write then tears only the newest record,
/// and the previous synced one still decodes. The log file holds one record per entry.
#[derive(Debug, Default)]
pub(crate) struct Durable {
    pub term: u64,
    pub voted_for: Option<NodeId>,
    pub log: Vec<Entry>,
    /// Byte offset in the log file where each entry's record ends, for truncation.
    ends: Vec<u64>,
}

impl Durable {
    pub fn recover(storage: &mut dyn Storage) -> Result<Self, SyncError> {
        let mut durable = Durable::default();
        if let Some(meta) = read_valid(storage, META_FILE)?.last() {
            durable.term = u64::from_le_bytes(meta[..8].try_into().expect("8-byte term"));
            let vote = u32::from_le_bytes(meta[8..12].try_into().expect("4-byte vote"));
            durable.voted_for = (vote != NO_VOTE).then_some(NodeId(vote));
        }
        let mut end = 0;
        for payload in read_valid(storage, LOG_FILE)? {
            end += (record::HEADER_LEN + payload.len()) as u64;
            durable.ends.push(end);
            let term = u64::from_le_bytes(payload[..8].try_into().expect("8-byte entry term"));
            durable.log.push(Entry { term, data: payload[8..].to_vec() });
        }
        Ok(durable)
    }

    /// Persists the current term and vote.
    pub fn save_meta(&mut self, storage: &mut dyn Storage) -> Result<(), SyncError> {
        let mut payload = self.term.to_le_bytes().to_vec();
        payload.extend(self.voted_for.map_or(NO_VOTE, |n| n.0).to_le_bytes());
        storage.append(META_FILE, &record::encode(&payload));
        storage.sync(META_FILE)
    }

    /// Appends entries to the log and syncs them.
    pub fn append(
        &mut self,
        storage: &mut dyn Storage,
        entries: &[Entry],
    ) -> Result<(), SyncError> {
        let mut end = self.ends.last().copied().unwrap_or(0);
        for entry in entries {
            let mut payload = entry.term.to_le_bytes().to_vec();
            payload.extend(&entry.data);
            let bytes = record::encode(&payload);
            storage.append(LOG_FILE, &bytes);
            end += bytes.len() as u64;
            self.ends.push(end);
            self.log.push(entry.clone());
        }
        storage.sync(LOG_FILE)
    }

    /// Removes the entries from `index` (1-based) on, and syncs that on its own. If the truncate
    /// shared a sync with the appends that follow it, a crash could persist the appends but not
    /// the truncate, splicing new entries in front of stale ones and breaking Log Matching.
    pub fn truncate_from(
        &mut self,
        storage: &mut dyn Storage,
        index: u64,
    ) -> Result<(), SyncError> {
        let keep = (index - 1) as usize;
        self.log.truncate(keep);
        self.ends.truncate(keep);
        storage.truncate(LOG_FILE, self.ends.last().copied().unwrap_or(0));
        storage.sync(LOG_FILE)
    }
}

/// Reads a file's valid records and cuts off any torn or garbage tail, so that later appends land
/// right after the last good record (anything appended after garbage would never decode).
fn read_valid(storage: &mut dyn Storage, file: &str) -> Result<Vec<Vec<u8>>, SyncError> {
    let bytes = storage.read(file);
    let decoded = record::decode(&bytes);
    if decoded.valid_len < bytes.len() as u64 {
        storage.truncate(file, decoded.valid_len);
        storage.sync(file)?;
    }
    Ok(decoded.records)
}
