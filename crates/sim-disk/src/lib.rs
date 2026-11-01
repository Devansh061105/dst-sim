//! Simulated disk with fault injection: lost unsynced writes on crash, torn writes, fsync errors, corruption.
//!
//! Each file keeps what the process sees (`visible`, the page cache), what is physically on disk
//! (`durable`), and the operations issued since the last successful sync (`pending`). A sync
//! replays `pending` onto `durable`. A crash keeps a random subset of the pending operations
//! (the OS may have written some back already), possibly tearing writes, and resets `visible` to
//! `durable`. A failed sync drops the pending operations for good, as Linux does ("fsyncgate").
//!
//! Like `sim-net`, this is a pure model driven by a [`Rand`]; the simulator owns the clock.

use std::collections::BTreeMap;

use sim_api::{Rand, Storage, SyncError};

#[derive(Clone, Debug, Default)]
pub struct DiskConfig {
    /// On a crash, the probability that each unsynced operation reached the disk anyway.
    pub unsynced_survive: f64,
    /// The probability that a surviving unsynced write is torn: only a prefix of it landed.
    pub torn_write: f64,
    /// The probability that a sync with pending operations fails.
    pub sync_fail: f64,
    /// The probability that the process dies in the middle of a sync with pending operations:
    /// the sync never completes, and the crash decides which of those operations reached the
    /// disk (possibly torn). The simulator stops the node at that point.
    pub crash_during_sync: f64,
    /// The probability that a crash also damages data already on disk: one bit flips (bit rot)
    /// or a span of bytes lands in the wrong place (the effect of a misdirected write). Applied by
    /// the simulator, which decides which crashes may corrupt (see [`Disk::corrupt_at_rest`]).
    pub corrupt: f64,
}

#[derive(Clone, Debug)]
enum Op {
    Write { offset: u64, data: Vec<u8> },
    Truncate { len: u64 },
}

#[derive(Clone, Debug, Default)]
struct File {
    visible: Vec<u8>,
    durable: Vec<u8>,
    pending: Vec<Op>,
}

/// What a crash did to the unsynced operations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CrashReport {
    pub survived: u64,
    pub torn: u64,
    pub lost: u64,
}

/// Damage done to data at rest by [`Disk::corrupt_at_rest`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Corruption {
    pub file: String,
    /// First damaged byte.
    pub offset: u64,
    pub kind: CorruptionKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CorruptionKind {
    /// One bit of the byte at `offset` flipped.
    BitFlip,
    /// `len` bytes copied from offset `from` over the bytes at `offset`, as a write that went to
    /// the wrong place would leave them.
    Misplaced { from: u64, len: u64 },
}

/// A failed sync, kept until the simulator collects it for the trace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncFailure {
    pub file: String,
    /// Pending operations that will now never become durable.
    pub dropped: u64,
}

#[derive(Clone, Debug)]
pub struct Disk {
    config: DiskConfig,
    files: BTreeMap<String, File>,
    sync_failures: Vec<SyncFailure>,
    /// The process died mid-sync; set until the simulator takes it and crashes the node.
    crashed_in_sync: bool,
}

impl Disk {
    pub fn new(config: DiskConfig) -> Self {
        let DiskConfig { unsynced_survive, torn_write, sync_fail, crash_during_sync, corrupt } =
            config;
        for p in [unsynced_survive, torn_write, sync_fail, crash_during_sync, corrupt] {
            assert!((0.0..=1.0).contains(&p), "probability {p} is outside [0, 1]");
        }
        Disk { config, files: BTreeMap::new(), sync_failures: Vec::new(), crashed_in_sync: false }
    }

    pub fn read(&self, file: &str) -> &[u8] {
        self.files.get(file).map_or(&[], |f| &f.visible)
    }

    /// What would survive a crash right now if no unsynced operation made it.
    pub fn durable(&self, file: &str) -> &[u8] {
        self.files.get(file).map_or(&[], |f| &f.durable)
    }

    pub fn write(&mut self, file: &str, offset: u64, data: &[u8]) {
        let op = Op::Write { offset, data: data.to_vec() };
        let f = self.files.entry(file.to_owned()).or_default();
        apply(&mut f.visible, &op);
        f.pending.push(op);
    }

    pub fn truncate(&mut self, file: &str, len: u64) {
        let op = Op::Truncate { len };
        let f = self.files.entry(file.to_owned()).or_default();
        apply(&mut f.visible, &op);
        f.pending.push(op);
    }

    /// Makes `file`'s pending operations durable, unless the sync fails. A failed sync drops them:
    /// they stay visible to this process but will never reach the disk, and a retry "succeeds".
    pub fn sync(&mut self, file: &str, rand: &mut impl Rand) -> Result<(), SyncError> {
        if self.crashed_in_sync {
            return Err(SyncError); // the process is already dead
        }
        let Some(f) = self.files.get_mut(file) else { return Ok(()) };
        if f.pending.is_empty() {
            return Ok(());
        }
        if rand.chance(self.config.crash_during_sync) {
            // Leave the operations pending: the crash decides which of them made it.
            self.crashed_in_sync = true;
            return Err(SyncError);
        }
        if rand.chance(self.config.sync_fail) {
            let dropped = f.pending.len() as u64;
            f.pending.clear();
            self.sync_failures.push(SyncFailure { file: file.to_owned(), dropped });
            return Err(SyncError);
        }
        for op in f.pending.drain(..) {
            apply(&mut f.durable, &op);
        }
        Ok(())
    }

    /// The process died: decide which unsynced operations made it, then forget the page cache.
    pub fn crash(&mut self, rand: &mut impl Rand) -> CrashReport {
        let mut report = CrashReport::default();
        for f in self.files.values_mut() {
            for op in f.pending.drain(..) {
                if !rand.chance(self.config.unsynced_survive) {
                    report.lost += 1;
                    continue;
                }
                report.survived += 1;
                match op {
                    Op::Write { offset, data } if rand.chance(self.config.torn_write) => {
                        report.torn += 1;
                        let kept = rand.below(data.len() as u64 + 1) as usize;
                        apply(&mut f.durable, &Op::Write { offset, data: data[..kept].to_vec() });
                    }
                    op => apply(&mut f.durable, &op),
                }
            }
            f.visible.clone_from(&f.durable);
        }
        report
    }

    /// With probability `corrupt`, damages one random spot of one file: flips a bit, or copies a
    /// span of up to 64 bytes over another part of the same file. Meant to be called right after
    /// a crash that left nothing unsynced, so corruption only ever hits data that was on disk.
    pub fn corrupt_at_rest(&mut self, rand: &mut impl Rand) -> Option<Corruption> {
        if !rand.chance(self.config.corrupt) {
            return None;
        }
        let files: Vec<&String> = self
            .files
            .iter()
            .filter(|(_, f)| !f.durable.is_empty())
            .map(|(name, _)| name)
            .collect();
        if files.is_empty() {
            return None;
        }
        let name = files[rand.below(files.len() as u64) as usize].clone();
        let f = self.files.get_mut(&name).expect("file exists");
        let size = f.durable.len() as u64;
        let offset = rand.below(size);
        let kind = if rand.chance(0.5) {
            f.durable[offset as usize] ^= 1 << rand.below(8);
            CorruptionKind::BitFlip
        } else {
            let len = (1 + rand.below(64)).min(size - offset);
            let from = rand.below(size - len + 1);
            f.durable.copy_within(from as usize..(from + len) as usize, offset as usize);
            CorruptionKind::Misplaced { from, len }
        };
        f.visible.clone_from(&f.durable);
        Some(Corruption { file: name, offset, kind })
    }

    /// From now on, syncs never fail and data at rest is never damaged (the simulator's calm
    /// period: faults stop, so a correct system must recover).
    pub fn calm(&mut self) {
        self.config.sync_fail = 0.0;
        self.config.crash_during_sync = 0.0;
        self.config.corrupt = 0.0;
    }

    /// Sync failures since the last call, for the simulator's trace and stats.
    pub fn take_sync_failures(&mut self) -> Vec<SyncFailure> {
        std::mem::take(&mut self.sync_failures)
    }

    /// Whether the process died in the middle of a sync since the last call.
    pub fn take_crash_in_sync(&mut self) -> bool {
        std::mem::take(&mut self.crashed_in_sync)
    }
}

fn apply(buf: &mut Vec<u8>, op: &Op) {
    match op {
        Op::Write { offset, data } => {
            let start = *offset as usize;
            let end = start + data.len();
            if buf.len() < end {
                buf.resize(end, 0);
            }
            buf[start..end].copy_from_slice(data);
        }
        Op::Truncate { len } => buf.resize(*len as usize, 0),
    }
}

/// A [`Disk`] paired with the randomness its faults need, as the [`Storage`] a node sees.
pub struct DiskIo<'a, R> {
    pub disk: &'a mut Disk,
    pub rand: &'a mut R,
}

impl<R: Rand> Storage for DiskIo<'_, R> {
    fn read(&self, file: &str) -> Vec<u8> {
        self.disk.read(file).to_vec()
    }

    fn file_len(&self, file: &str) -> u64 {
        self.disk.read(file).len() as u64
    }

    fn write(&mut self, file: &str, offset: u64, data: &[u8]) {
        self.disk.write(file, offset, data);
    }

    fn truncate(&mut self, file: &str, len: u64) {
        self.disk.truncate(file, len);
    }

    fn sync(&mut self, file: &str) -> Result<(), SyncError> {
        self.disk.sync(file, self.rand)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Lcg(u64);

    impl Rand for Lcg {
        fn next_u64(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0
        }
    }

    fn disk(config: DiskConfig) -> Disk {
        Disk::new(config)
    }

    #[test]
    fn writes_are_visible_at_once_but_only_synced_data_survives_a_crash() {
        let mut d = disk(DiskConfig::default());
        let mut rand = Lcg(1);
        d.write("f", 0, b"synced");
        d.sync("f", &mut rand).unwrap();
        d.write("f", 6, b"+unsynced");
        assert_eq!(d.read("f"), b"synced+unsynced");

        let report = d.crash(&mut rand);
        assert_eq!(report, CrashReport { survived: 0, torn: 0, lost: 1 });
        assert_eq!(d.read("f"), b"synced");
    }

    #[test]
    fn truncate_is_durable_only_after_sync() {
        let mut d = disk(DiskConfig::default());
        let mut rand = Lcg(1);
        d.write("f", 0, b"abcdef");
        d.sync("f", &mut rand).unwrap();
        d.truncate("f", 2);
        assert_eq!(d.read("f"), b"ab");
        d.crash(&mut rand);
        assert_eq!(d.read("f"), b"abcdef");
    }

    #[test]
    fn failed_sync_loses_data_even_if_a_retry_succeeds() {
        let mut d = disk(DiskConfig { sync_fail: 1.0, ..DiskConfig::default() });
        let mut rand = Lcg(1);
        d.write("f", 0, b"precious");
        assert_eq!(d.sync("f", &mut rand), Err(SyncError));
        assert_eq!(d.read("f"), b"precious", "still visible in the page cache");
        assert_eq!(d.sync("f", &mut rand), Ok(()), "the retry finds nothing to do");
        assert_eq!(d.take_sync_failures(), [SyncFailure { file: "f".into(), dropped: 1 }]);

        d.crash(&mut rand);
        assert_eq!(d.read("f"), b"");
    }

    #[test]
    fn unsynced_writes_can_survive_or_tear() {
        let mut d = disk(DiskConfig { unsynced_survive: 1.0, ..DiskConfig::default() });
        d.write("f", 0, b"lucky");
        assert_eq!(d.crash(&mut Lcg(1)).survived, 1);
        assert_eq!(d.read("f"), b"lucky");

        let mut d =
            disk(DiskConfig { unsynced_survive: 1.0, torn_write: 1.0, ..DiskConfig::default() });
        d.write("f", 0, b"0123456789");
        assert_eq!(d.crash(&mut Lcg(7)).torn, 1);
        assert!(b"0123456789".starts_with(d.read("f")), "a torn write leaves a prefix");
    }

    #[test]
    fn corruption_damages_one_spot_of_data_at_rest() {
        let original: Vec<u8> = (0..200).collect();
        let mut kinds = Vec::new();
        for seed in 0..50 {
            let mut d = disk(DiskConfig { corrupt: 1.0, ..DiskConfig::default() });
            let mut rand = Lcg(seed);
            d.write("f", 0, &original);
            d.sync("f", &mut rand).unwrap();
            let c = d.corrupt_at_rest(&mut rand).expect("p = 1 always corrupts");
            assert_eq!(c.file, "f");
            let changed: Vec<usize> = (0..200).filter(|&i| d.read("f")[i] != original[i]).collect();
            assert_eq!(d.read("f").len(), 200, "corruption never changes a file's size");
            if let Some(&first) = changed.first() {
                assert!(
                    first as u64 >= c.offset && changed.len() <= 64,
                    "{c:?} changed {changed:?}"
                );
            }
            kinds.push(matches!(c.kind, CorruptionKind::BitFlip));
        }
        assert!(kinds.contains(&true) && kinds.contains(&false), "both kinds happen");

        let mut d = disk(DiskConfig::default());
        d.write("f", 0, b"data");
        assert_eq!(d.corrupt_at_rest(&mut Lcg(1)), None, "p = 0 never corrupts");
    }

    #[test]
    fn surviving_writes_can_leave_holes() {
        // Two unsynced writes; if only the second survives, the first one's range reads as zeros.
        let config = DiskConfig { unsynced_survive: 0.5, ..DiskConfig::default() };
        let hole = (0..200u64).find(|&seed| {
            let mut d = disk(config.clone());
            d.write("f", 0, b"AAAA");
            d.write("f", 4, b"BBBB");
            d.crash(&mut Lcg(seed));
            d.read("f") == b"\0\0\0\0BBBB"
        });
        assert!(hole.is_some(), "expected some seed to lose only the first write");
    }
}
