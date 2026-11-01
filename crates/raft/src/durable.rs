use std::cell::Cell;

use sim_api::{NodeId, Storage, SyncError, record};

use crate::Entry;

/// Two copies of the term and vote, so damage to one never loses them.
const META_FILES: [&str; 2] = ["raft-meta", "raft-meta-copy"];
/// The log lives in one of two files; compaction writes the kept entries to the other one and
/// then switches over with a snapshot record, so a crash at any point leaves one complete log.
const LOG_FILES: [&str; 2] = ["raft-log", "raft-log-b"];
/// The index of the last entry known to be on disk.
const LOG_END_FILE: &str = "raft-log-end";
const SNAPSHOT_FILE: &str = "raft-snapshot";
const NO_VOTE: u32 = u32::MAX;
const NOT_REJOINING: u64 = u64::MAX;
/// Sequence number, term, vote, rejoin-after term.
const META_LEN: usize = 8 + 8 + 4 + 8;

/// The state machine as of a log index, standing in for every entry up to it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    /// The last entry it covers (0: no snapshot).
    pub index: u64,
    /// That entry's term, for the consistency check of the entry after it.
    pub term: u64,
    /// The state machine, encoded by the host.
    pub data: Vec<u8>,
    /// The cluster configuration as of `index`; `None` while it is still the initial one.
    pub members: Option<Vec<NodeId>>,
}

/// Raft's persistent state (Figure 2): the current term, the vote, and the log, mirrored on disk.
///
/// The term and vote are appended to their files as small records, newest last, instead of being
/// overwritten in place: a crash in the middle of a write then tears only the newest record, and
/// the previous synced one still decodes. Both files hold the same records, so damage to one
/// (a flipped bit, a misplaced write) still leaves the latest value readable.
///
/// The log holds one record per entry after the snapshot, each carrying its own index. Once
/// appended entries are synced, the last entry's index and term go to a separate small file,
/// before anything is acknowledged. That is how recovery tells a torn tail (an interrupted write
/// that was never acknowledged, safe to drop) from damage to data that was synced: after damage
/// the log no longer holds the entry the other file recorded. Checksums alone can't tell them apart, since damage
/// at the end of the log looks just like a torn write.
#[derive(Debug, Default)]
pub(crate) struct Durable {
    pub term: u64,
    pub voted_for: Option<NodeId>,
    /// Set after recovery found the log damaged: the node may have lost entries it had
    /// acknowledged, so it must not vote or campaign until it holds an entry from a term later
    /// than this one (see [`crate::Raft::is_rejoining`]).
    pub rejoin_after: Option<u64>,
    pub snapshot: Snapshot,
    /// The entries after the snapshot: entry `i` is `log[i - snapshot.index - 1]`.
    pub log: Vec<Entry>,
    /// Which of [`LOG_FILES`] holds the log.
    log_file: usize,
    /// The last entry (index, term) the log held when the current snapshot was switched to, as
    /// recorded with it: what recovery must find if no later log-end record was written.
    switched_end: (u64, u64),
    /// Byte offset in the log file where each entry's record ends, for truncation.
    ends: Vec<u64>,
    /// Bumped on every save of the term and vote, so recovery can tell which copy is newer.
    meta_seq: u64,
    /// The lowest log index (1-based) changed since the invariant checker last looked, so it only
    /// re-examines what changed. A `Cell` lets the checker reset it through a shared reference.
    pub changed_from: Cell<Option<u64>>,
}

impl Durable {
    /// Reads the state back after a (re)start, repairing what it can. `trust_damage` is a planted
    /// bug: treat a damaged log like a torn tail and carry on voting.
    pub fn recover(storage: &mut dyn Storage, trust_damage: bool) -> Result<Self, SyncError> {
        let mut durable = Durable::default();
        durable.recover_meta(storage)?;
        durable.recover_snapshot(storage)?;
        durable.recover_log(storage, trust_damage)?;
        durable.mark_changed(1);
        Ok(durable)
    }

    /// The index of the last entry, compacted or not.
    pub fn last_index(&self) -> u64 {
        self.snapshot.index + self.log.len() as u64
    }

    /// The entry at `index`, unless it is compacted into the snapshot or beyond the log.
    pub fn entry(&self, index: u64) -> Option<&Entry> {
        let offset = index.checked_sub(self.snapshot.index + 1)?;
        self.log.get(offset as usize)
    }

    fn recover_meta(&mut self, storage: &mut dyn Storage) -> Result<(), SyncError> {
        let mut latest = Vec::new();
        for file in META_FILES {
            // The newest record by sequence number, not position: a misplaced write can leave a
            // valid copy of an older record at the end.
            let records = read_valid(storage, file)?;
            let metas = records.iter().filter(|r| r.len() == META_LEN).map(|r| decode_meta(r));
            latest.push(metas.max_by_key(|m| m.0));
        }
        let Some(best) = latest.iter().flatten().max_by_key(|m| m.0).copied() else {
            return Ok(()); // a fresh node
        };
        (self.meta_seq, self.term, self.voted_for, self.rejoin_after) = best;
        // Bring a damaged or lagging copy back up to date, so the next damage can't hit the
        // only good one.
        for (file, last) in META_FILES.iter().zip(&latest) {
            if *last != Some(best) {
                storage.append(file, &record::encode(&self.meta_payload()));
                storage.sync(file)?;
            }
        }
        Ok(())
    }

    fn recover_snapshot(&mut self, storage: &mut dyn Storage) -> Result<(), SyncError> {
        // Snapshots only move forward, so the one with the highest index is the latest. Should
        // damage destroy it, the previous one's log file is still intact, just shorter, and the
        // log-end check below notices.
        let records = read_valid(storage, SNAPSHOT_FILE)?;
        let well_formed = records.iter().filter_map(|r| decode_snapshot(r).map(|s| (r, s)));
        if let Some((r, (snapshot, data_start))) = well_formed.max_by_key(|(r, _)| read_u64(r)) {
            self.snapshot = Snapshot { data: r[data_start..].to_vec(), ..snapshot };
            self.log_file = usize::from(r[16] & 1);
            self.switched_end = (read_u64(&r[17..]), read_u64(&r[25..]));
        }
        Ok(())
    }

    fn recover_log(
        &mut self,
        storage: &mut dyn Storage,
        trust_damage: bool,
    ) -> Result<(), SyncError> {
        let file = LOG_FILES[self.log_file];
        let bytes = storage.read(file);
        let mut end = 0;
        for payload in record::decode(&bytes).records {
            // A valid record for another index is data that landed in the wrong place.
            if payload.len() < 16 || read_u64(&payload) != self.last_index() + 1 {
                break;
            }
            self.log.push(decode_entry(&payload));
            end += (record::HEADER_LEN + payload.len()) as u64;
            self.ends.push(end);
        }
        // Compare with the last entry recorded as synced, which also names its snapshot:
        // - a newer snapshot: its record was damaged and recovery fell back to an older one;
        // - an older snapshot: the switch to the current one happened, but no log-end record was
        //   written since; then the snapshot record says what the new log held at the switch;
        // - the same one: that entry.
        // The log must still hold the entry required, the same index *and* term (by Log Matching,
        // then the same entries before it), or synced entries were lost.
        let ends = read_valid(storage, LOG_END_FILE)?;
        let synced = ends.iter().rfind(|r| r.len() == 24);
        let synced =
            synced.map_or((0, 0, 0), |r| (read_u64(r), read_u64(&r[8..]), read_u64(&r[16..])));
        let (snapshot, index, term) = synced;
        let (index, term) =
            if snapshot < self.snapshot.index { self.switched_end } else { (index, term) };
        let damaged = snapshot > self.snapshot.index || self.term_at(index) != Some(term);
        if damaged && !trust_damage {
            // Persist "rejoining" before dropping anything: if the node crashed in between, the
            // next recovery would see a clean, shorter log and vote as if nothing was lost.
            self.rejoin_after = Some(self.term);
            self.save_meta(storage)?;
        }
        if end < bytes.len() as u64 {
            storage.truncate(file, end);
            storage.sync(file)?;
        }
        if synced != self.log_end() {
            self.save_log_end(storage)?;
        }
        Ok(())
    }

    fn mark_changed(&self, index: u64) {
        let lowest = self.changed_from.get().map_or(index, |c| c.min(index));
        self.changed_from.set(Some(lowest));
    }

    /// Persists the current term, vote and rejoining state, to both copies.
    pub fn save_meta(&mut self, storage: &mut dyn Storage) -> Result<(), SyncError> {
        self.meta_seq += 1;
        let bytes = record::encode(&self.meta_payload());
        for file in META_FILES {
            storage.append(file, &bytes);
            storage.sync(file)?;
        }
        Ok(())
    }

    fn meta_payload(&self) -> Vec<u8> {
        let mut payload = self.meta_seq.to_le_bytes().to_vec();
        payload.extend(self.term.to_le_bytes());
        payload.extend(self.voted_for.map_or(NO_VOTE, |n| n.0).to_le_bytes());
        payload.extend(self.rejoin_after.unwrap_or(NOT_REJOINING).to_le_bytes());
        payload
    }

    /// Records that the whole log, as it is now, is on disk.
    fn save_log_end(&self, storage: &mut dyn Storage) -> Result<(), SyncError> {
        let mut payload = Vec::new();
        for v in <[u64; 3]>::from(self.log_end()) {
            payload.extend(v.to_le_bytes());
        }
        storage.append(LOG_END_FILE, &record::encode(&payload));
        storage.sync(LOG_END_FILE)
    }

    /// The snapshot index, and the index and term of the last entry (the snapshot's, if the log
    /// after it is empty).
    fn log_end(&self) -> (u64, u64, u64) {
        let term = self.log.last().map_or(self.snapshot.term, |e| e.term);
        (self.snapshot.index, self.last_index(), term)
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.snapshot.index {
            Some(self.snapshot.term)
        } else {
            self.entry(index).map(|e| e.term)
        }
    }

    /// Appends entries to the log in one write, syncs them, then records the new last index.
    /// With `sync` off (only a planted bug does that) neither happens: the last index may only be
    /// recorded once the entries really are on disk.
    pub fn append(
        &mut self,
        storage: &mut dyn Storage,
        entries: &[Entry],
        sync: bool,
    ) -> Result<(), SyncError> {
        self.mark_changed(self.last_index() + 1);
        let start = self.ends.last().copied().unwrap_or(0);
        let bytes = encode_entries(entries, self.last_index() + 1, start, &mut self.ends);
        self.log.extend_from_slice(entries);
        storage.write(LOG_FILES[self.log_file], start, &bytes);
        if !sync {
            return Ok(());
        }
        storage.sync(LOG_FILES[self.log_file])?;
        self.save_log_end(storage)
    }

    /// Removes the entries from `index` (1-based, after the snapshot) on. The shorter log is
    /// recorded first, so a crash in between can't make the truncated log look damaged. The
    /// truncate is then synced on its own: if it shared a sync with the appends that follow it, a
    /// crash could persist the appends but not the truncate, splicing new entries in front of
    /// stale ones and breaking Log Matching. (`sync` off, leaving the truncate to share the next
    /// sync, is exactly that planted bug.)
    pub fn truncate_from(
        &mut self,
        storage: &mut dyn Storage,
        index: u64,
        sync: bool,
    ) -> Result<(), SyncError> {
        assert!(index > self.snapshot.index, "truncating into the snapshot (committed entries)");
        self.mark_changed(index);
        let keep = (index - self.snapshot.index - 1) as usize;
        self.log.truncate(keep);
        self.ends.truncate(keep);
        self.save_log_end(storage)?;
        let file = LOG_FILES[self.log_file];
        storage.truncate(file, self.ends.last().copied().unwrap_or(0));
        if sync { storage.sync(file) } else { Ok(()) }
    }

    /// Replaces the log up to `snapshot.index` with the snapshot, keeping the entries after it
    /// if `keep_tail` (compaction, or an installed snapshot that matches the log); otherwise the
    /// whole log goes (an installed snapshot the log doesn't match).
    ///
    /// Crash-safe in three synced steps: the kept entries go to the *other* log file; a snapshot
    /// record naming that file is appended (the switch: until it is on disk, recovery uses the
    /// old snapshot and old file, both untouched); the new last index is recorded. Memory only
    /// changes once the switch is on disk: after a failed sync the node halts, but the rest of
    /// the event still runs and must see a consistent state.
    pub fn install_snapshot(
        &mut self,
        storage: &mut dyn Storage,
        snapshot: Snapshot,
        keep_tail: bool,
    ) -> Result<(), SyncError> {
        let tail: Vec<Entry> = if keep_tail {
            let after = snapshot.index + 1..=self.last_index();
            after.filter_map(|i| self.entry(i).cloned()).collect()
        } else {
            Vec::new()
        };
        let file = 1 - self.log_file;
        let mut ends = Vec::new();
        let bytes = encode_entries(&tail, snapshot.index + 1, 0, &mut ends);
        storage.truncate(LOG_FILES[file], 0);
        storage.write(LOG_FILES[file], 0, &bytes);
        storage.sync(LOG_FILES[file])?;

        let switched_end =
            (snapshot.index + tail.len() as u64, tail.last().map_or(snapshot.term, |e| e.term));
        let mut payload = snapshot.index.to_le_bytes().to_vec();
        payload.extend(snapshot.term.to_le_bytes());
        // The log file number (0 or 1), with the top bit set when a configuration follows the
        // fixed fields (records without one read exactly as before).
        payload.push(file as u8 | if snapshot.members.is_some() { CONFIG_FLAG } else { 0 });
        payload.extend(switched_end.0.to_le_bytes());
        payload.extend(switched_end.1.to_le_bytes());
        if let Some(members) = &snapshot.members {
            payload.extend(encode_members(members));
        }
        payload.extend(&snapshot.data);
        storage.append(SNAPSHOT_FILE, &record::encode(&payload));
        storage.sync(SNAPSHOT_FILE)?;

        self.mark_changed(1);
        (self.snapshot, self.log, self.ends, self.log_file) = (snapshot, tail, ends, file);
        self.switched_end = switched_end;
        self.save_log_end(storage)
    }
}

/// Encodes log records for `entries`, the first of which has index `first`, to be written at byte
/// `start`; pushes where each record ends onto `ends`.
fn encode_entries(entries: &[Entry], first: u64, start: u64, ends: &mut Vec<u64>) -> Vec<u8> {
    let mut bytes = Vec::new();
    for (index, entry) in (first..).zip(entries) {
        let mut payload = index.to_le_bytes().to_vec();
        match &entry.config {
            None => {
                payload.extend(entry.term.to_le_bytes());
                payload.extend(&entry.data);
            }
            // A configuration entry: the term's top bit marks it (terms never get that high), and
            // the member list takes the place of the data. Data entries are stored as before.
            Some(members) => {
                payload.extend((entry.term | CONFIG_TERM_BIT).to_le_bytes());
                payload.extend(encode_members(members));
            }
        }
        bytes.extend(record::encode(&payload));
        ends.push(start + bytes.len() as u64);
    }
    bytes
}

type Meta = (u64, u64, Option<NodeId>, Option<u64>);

fn decode_meta(payload: &[u8]) -> Meta {
    let vote = u32::from_le_bytes(payload[16..20].try_into().expect("4-byte vote"));
    let rejoin = read_u64(&payload[20..]);
    (
        read_u64(payload),
        read_u64(&payload[8..]),
        (vote != NO_VOTE).then_some(NodeId(vote)),
        (rejoin != NOT_REJOINING).then_some(rejoin),
    )
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes"))
}

/// Marks a configuration entry in the term field of its log record.
const CONFIG_TERM_BIT: u64 = 1 << 63;
/// Marks a snapshot record that carries a configuration.
const CONFIG_FLAG: u8 = 0x80;

/// A member list: its length, then each id.
fn encode_members(members: &[NodeId]) -> Vec<u8> {
    let mut bytes = vec![u8::try_from(members.len()).expect("at most 255 members")];
    for m in members {
        bytes.extend(m.0.to_le_bytes());
    }
    bytes
}

/// The member list at the start of `bytes`, and where it ends.
fn decode_members(bytes: &[u8]) -> Option<(Vec<NodeId>, usize)> {
    let count = usize::from(*bytes.first()?);
    let members = (0..count)
        .map(|i| {
            let at = 1 + 4 * i;
            Some(NodeId(u32::from_le_bytes(bytes.get(at..at + 4)?.try_into().ok()?)))
        })
        .collect::<Option<Vec<_>>>()?;
    Some((members, 1 + 4 * count))
}

/// A log record's entry (the caller has checked the index).
fn decode_entry(payload: &[u8]) -> Entry {
    let term = read_u64(&payload[8..]);
    if term & CONFIG_TERM_BIT != 0
        && let Some((members, _)) = decode_members(&payload[16..])
    {
        return Entry { term: term & !CONFIG_TERM_BIT, data: Vec::new(), config: Some(members) };
    }
    Entry { term, data: payload[16..].to_vec(), config: None }
}

/// A snapshot record's fixed fields and configuration (data left empty), and where its data
/// starts; `None` if the record is not well formed.
fn decode_snapshot(r: &[u8]) -> Option<(Snapshot, usize)> {
    if r.len() < 33 || r[16] & !CONFIG_FLAG > 1 {
        return None;
    }
    let (index, term) = (read_u64(r), read_u64(&r[8..]));
    let (members, data_start) = if r[16] & CONFIG_FLAG != 0 {
        let (members, len) = decode_members(&r[33..])?;
        (Some(members), 33 + len)
    } else {
        (None, 33)
    };
    Some((Snapshot { index, term, data: Vec::new(), members }, data_start))
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    /// Where the log is until the first snapshot.
    const LOG_FILE: &str = LOG_FILES[0];

    /// Storage without faults: what is written is what recovery reads.
    #[derive(Default)]
    struct Files(BTreeMap<String, Vec<u8>>);

    impl Storage for Files {
        fn read(&self, file: &str) -> Vec<u8> {
            self.0.get(file).cloned().unwrap_or_default()
        }
        fn file_len(&self, file: &str) -> u64 {
            self.0.get(file).map_or(0, |f| f.len() as u64)
        }
        fn write(&mut self, file: &str, offset: u64, data: &[u8]) {
            let f = self.0.entry(file.to_owned()).or_default();
            let end = offset as usize + data.len();
            if f.len() < end {
                f.resize(end, 0);
            }
            f[offset as usize..end].copy_from_slice(data);
        }
        fn truncate(&mut self, file: &str, len: u64) {
            self.0.entry(file.to_owned()).or_default().resize(len as usize, 0);
        }
        fn sync(&mut self, _: &str) -> Result<(), SyncError> {
            Ok(())
        }
    }

    fn entry(term: u64, byte: u8) -> Entry {
        Entry { term, data: vec![byte; 10], config: None }
    }

    /// Term 3, voted for n1, five entries written in two batches.
    fn written() -> (Files, Vec<Entry>) {
        let mut files = Files::default();
        let mut d = Durable::recover(&mut files, false).unwrap();
        d.term = 3;
        d.voted_for = Some(NodeId(1));
        d.save_meta(&mut files).unwrap();
        let entries: Vec<Entry> = (1..=5).map(|i| entry(i / 2 + 1, i as u8)).collect();
        d.append(&mut files, &entries[..2], true).unwrap();
        d.append(&mut files, &entries[2..], true).unwrap();
        (files, entries)
    }

    fn recover(files: &mut Files) -> Durable {
        Durable::recover(files, false).unwrap()
    }

    /// Byte offset of the start of log entry `index`'s record.
    fn entry_offset(files: &mut Files, index: usize) -> usize {
        recover(files).ends.get(index.wrapping_sub(2)).map_or(0, |&e| e as usize)
    }

    #[test]
    fn round_trip_with_truncation() {
        let (mut files, entries) = written();
        let mut d = recover(&mut files);
        assert_eq!((d.term, d.voted_for, &d.log), (3, Some(NodeId(1)), &entries));
        d.truncate_from(&mut files, 4, true).unwrap();
        d.append(&mut files, &[entry(9, 9)], true).unwrap();
        let d = recover(&mut files);
        assert_eq!(d.log, [&entries[..3], &[entry(9, 9)]].concat());
        assert!(d.rejoin_after.is_none());
    }

    #[test]
    fn a_torn_unsynced_tail_is_dropped_without_suspicion() {
        let (mut files, entries) = written();
        let mut d = recover(&mut files);
        d.append(&mut files, &[entry(4, 6)], false).unwrap(); // never synced, never acknowledged
        let full = files.0[LOG_FILE].len();
        for cut in 1..full - entry_offset(&mut Files(files.0.clone()), 6) {
            let mut files = Files(files.0.clone());
            files.0.get_mut(LOG_FILE).unwrap().truncate(full - cut);
            let d = recover(&mut files);
            assert_eq!((d.rejoin_after, &d.log), (None, &entries), "cut {cut}");
        }
    }

    #[test]
    fn damage_to_a_synced_entry_makes_the_node_rejoin() {
        for index in 1..=5 {
            let (mut files, entries) = written();
            let at = entry_offset(&mut files, index) + 12;
            files.0.get_mut(LOG_FILE).unwrap()[at] ^= 0x40;
            let d = recover(&mut files);
            assert_eq!(d.rejoin_after, Some(3), "entry {index}");
            assert_eq!(d.log, entries[..index - 1], "entry {index}");
            // Remembered across another restart, although the log now looks clean.
            assert_eq!(recover(&mut files).rejoin_after, Some(3), "entry {index}");
        }
    }

    #[test]
    fn damage_to_the_log_end_record_loses_nothing() {
        let (mut files, entries) = written();
        let end = files.0.get_mut(LOG_END_FILE).unwrap();
        let last = end.len() - 3;
        end[last] ^= 1;
        let d = recover(&mut files);
        assert_eq!((d.rejoin_after, &d.log), (None, &entries));
    }

    #[test]
    fn a_record_in_the_wrong_place_is_damage() {
        let (mut files, entries) = written();
        // Entry 1's record (a valid record, but for index 1) copied over entry 3's.
        let (second, third) = (entry_offset(&mut files, 2), entry_offset(&mut files, 3));
        files.0.get_mut(LOG_FILE).unwrap().copy_within(..second, third);
        let d = recover(&mut files);
        assert_eq!((d.rejoin_after, &d.log), (Some(3), &entries[..2].to_vec()));
    }

    #[test]
    fn damage_to_one_meta_copy_is_repaired_from_the_other() {
        let (mut files, _) = written();
        let meta = files.0.get_mut(META_FILES[0]).unwrap();
        let last = meta.len() - 1;
        meta[last] ^= 0x80;
        let d = recover(&mut files);
        assert_eq!((d.term, d.voted_for), (3, Some(NodeId(1))));
        // Now the other copy can be damaged too, one at a time.
        let meta = files.0.get_mut(META_FILES[1]).unwrap();
        let last = meta.len() - 1;
        meta[last] ^= 0x80;
        let d = recover(&mut files);
        assert_eq!((d.term, d.voted_for), (3, Some(NodeId(1))));
    }

    #[test]
    fn trusting_a_damaged_log_is_the_planted_bug() {
        let (mut files, entries) = written();
        let at = entry_offset(&mut files, 2) + 12;
        files.0.get_mut(LOG_FILE).unwrap()[at] ^= 0x40;
        let d = Durable::recover(&mut files, true).unwrap();
        assert_eq!((d.rejoin_after, &d.log), (None, &entries[..1].to_vec()));
    }

    fn snapshot(index: u64, term: u64) -> Snapshot {
        Snapshot { index, term, data: vec![index as u8; 4], members: None }
    }

    #[test]
    fn compaction_keeps_the_tail_and_survives_restarts() {
        let (mut files, entries) = written();
        let mut d = recover(&mut files);
        d.install_snapshot(&mut files, snapshot(3, 2), true).unwrap();
        d.append(&mut files, &[entry(4, 6)], true).unwrap();
        for _ in 0..2 {
            let d = recover(&mut files);
            assert_eq!(d.snapshot, snapshot(3, 2));
            assert_eq!(d.log, [&entries[3..], &[entry(4, 6)]].concat());
            assert_eq!((d.last_index(), d.entry(3), d.entry(4)), (6, None, Some(&entries[3])));
            assert!(d.rejoin_after.is_none());
        }
        // Again, so both log files have been used.
        let mut d = recover(&mut files);
        d.install_snapshot(&mut files, snapshot(5, 3), true).unwrap();
        let d = recover(&mut files);
        assert_eq!((d.snapshot.index, d.log.clone()), (5, vec![entry(4, 6)]));
    }

    #[test]
    fn an_installed_snapshot_replaces_a_log_that_disagrees() {
        let (mut files, _) = written();
        let mut d = recover(&mut files);
        d.install_snapshot(&mut files, snapshot(8, 7), false).unwrap();
        let d = recover(&mut files);
        assert_eq!((d.snapshot.index, d.log.len(), d.last_index()), (8, 0, 8));
        assert!(d.rejoin_after.is_none());
    }

    /// A crash after the switch to a new snapshot but before the end record was rewritten: the
    /// end record still describes the old log. That is not damage.
    #[test]
    fn a_crash_right_after_a_snapshot_switch_is_not_damage() {
        let (mut files, _) = written();
        let mut d = recover(&mut files);
        d.install_snapshot(&mut files, snapshot(8, 7), false).unwrap();
        let end = files.0.get_mut(LOG_END_FILE).unwrap();
        end.truncate(end.len() - (record::HEADER_LEN + 24)); // the record written last
        let d = recover(&mut files);
        assert_eq!((d.snapshot.index, d.last_index(), d.rejoin_after), (8, 8, None));
    }

    /// Swarm seed 275843: compaction moved entries 4 and 5 to the other log file and switched to
    /// it, the log-end record was never rewritten, then the new file got damaged. The end record
    /// (from before the switch) can't tell; the snapshot record's own copy of the log end can.
    #[test]
    fn damage_right_after_a_snapshot_switch_is_still_damage() {
        let (mut files, _) = written();
        let mut d = recover(&mut files);
        d.install_snapshot(&mut files, snapshot(3, 2), true).unwrap();
        let end = files.0.get_mut(LOG_END_FILE).unwrap();
        end.truncate(end.len() - (record::HEADER_LEN + 24)); // the record written last
        files.0.get_mut(LOG_FILES[1]).unwrap()[12] ^= 0x40; // entry 4, in the new file
        let d = recover(&mut files);
        assert_eq!((d.snapshot.index, d.rejoin_after), (3, Some(3)), "{:?}", d.log);
    }

    /// Damage to the newest snapshot record makes recovery fall back to the previous snapshot
    /// and its log file, which may be *longer* than the current log (it predates a snapshot that
    /// replaced a disagreeing log). Only the recorded term of the last entry gives it away.
    #[test]
    fn falling_back_to_a_stale_log_is_damage() {
        let (mut files, _) = written(); // five entries, terms 1..=3
        let mut d = recover(&mut files);
        d.install_snapshot(&mut files, snapshot(2, 9), false).unwrap(); // a newer leader's history
        d.append(&mut files, &[entry(9, 1), entry(9, 2), entry(9, 3)], true).unwrap();
        assert_eq!(recover(&mut files).last_index(), 5);
        let snapshots = files.0.get_mut(SNAPSHOT_FILE).unwrap();
        let last = snapshots.len() - 1;
        snapshots[last] ^= 1;
        let d = recover(&mut files);
        assert_eq!((d.snapshot.index, d.rejoin_after), (0, Some(3)), "{:?}", d.log);
    }
}
