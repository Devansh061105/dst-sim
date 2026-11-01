use std::fmt;
use std::hash::{Hash, Hasher};

use sim_api::{NodeId, Time, TimerId};

use sim_disk::CrashReport;

use crate::faults::Fault;

/// Record of a run: a running hash of every processed event, plus optional readable lines.
///
/// Two runs with the same hash processed the same events, in the same order, at the same virtual
/// times. Comparing hashes is how determinism is checked.
#[derive(Debug)]
pub struct Trace {
    hasher: Fnv1a,
    events: u64,
    lines: Option<Vec<String>>,
}

impl Trace {
    pub(crate) fn new(keep_lines: bool) -> Self {
        Trace { hasher: Fnv1a::default(), events: 0, lines: keep_lines.then(Vec::new) }
    }

    pub(crate) fn record<M: Hash + fmt::Debug, T: Hash + fmt::Debug>(
        &mut self,
        at: Time,
        entry: &Entry<'_, M, T>,
    ) {
        self.hasher.write_u64(at.as_nanos());
        entry.hash_into(&mut self.hasher);
        self.events += 1;
        if let Some(lines) = &mut self.lines {
            lines.push(format!("[{at}] {entry}"));
        }
    }

    /// Fingerprint of everything recorded so far.
    pub fn hash(&self) -> u64 {
        self.hasher.finish()
    }

    pub fn event_count(&self) -> u64 {
        self.events
    }

    /// One line per event, if the run was configured to keep them.
    pub fn lines(&self) -> Option<&[String]> {
        self.lines.as_deref()
    }
}

/// Why a message never reached its destination.
#[derive(Clone, Copy, Debug)]
pub(crate) enum DropReason {
    Lost,
    Partitioned,
    NodeDown,
}

/// One processed event, borrowed from the simulator just long enough to record it.
pub(crate) enum Entry<'a, M, T> {
    Start { node: NodeId },
    Deliver { from: NodeId, to: NodeId, msg: &'a M },
    Timer { node: NodeId, id: TimerId, timer: &'a T },
    Fault(&'a Fault),
    Drop { from: NodeId, to: NodeId, reason: DropReason, msg: &'a M },
    DiskCrash { node: NodeId, report: CrashReport },
    SyncFailed { node: NodeId, file: &'a str, dropped: u64 },
    Halt { node: NodeId },
}

impl<M: Hash, T: Hash> Entry<'_, M, T> {
    // Written by hand rather than derived so the byte layout is fixed by this code alone.
    fn hash_into(&self, h: &mut Fnv1a) {
        match self {
            Entry::Start { node } => {
                h.write_u8(0);
                h.write_u32(node.0);
            }
            Entry::Deliver { from, to, msg } => {
                h.write_u8(1);
                h.write_u32(from.0);
                h.write_u32(to.0);
                msg.hash(h);
            }
            Entry::Timer { node, id, timer } => {
                h.write_u8(2);
                h.write_u32(node.0);
                h.write_u64(id.0);
                timer.hash(h);
            }
            Entry::Fault(fault) => {
                h.write_u8(3);
                fault.hash_into(h);
            }
            Entry::Drop { from, to, reason, msg } => {
                h.write_u8(4);
                h.write_u32(from.0);
                h.write_u32(to.0);
                h.write_u8(*reason as u8);
                msg.hash(h);
            }
            Entry::DiskCrash { node, report } => {
                h.write_u8(5);
                h.write_u32(node.0);
                h.write_u64(report.survived);
                h.write_u64(report.torn);
                h.write_u64(report.lost);
            }
            Entry::SyncFailed { node, file, dropped } => {
                h.write_u8(6);
                h.write_u32(node.0);
                h.write(file.as_bytes());
                h.write_u64(*dropped);
            }
            Entry::Halt { node } => {
                h.write_u8(7);
                h.write_u32(node.0);
            }
        }
    }
}

impl<M: fmt::Debug, T: fmt::Debug> fmt::Display for Entry<'_, M, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Entry::Start { node } => write!(f, "{node} start"),
            Entry::Deliver { from, to, msg } => write!(f, "{to} <- {from} {msg:?}"),
            Entry::Timer { node, timer, .. } => write!(f, "{node} timer {timer:?}"),
            Entry::Fault(fault) => write!(f, "fault {fault}"),
            Entry::Drop { from, to, reason, msg } => {
                write!(f, "{to} <-x- {from} {reason:?} {msg:?}")
            }
            Entry::DiskCrash { node, report } => write!(
                f,
                "{node} disk crash: {} unsynced ops survived ({} torn), {} lost",
                report.survived, report.torn, report.lost
            ),
            Entry::SyncFailed { node, file, dropped } => {
                write!(f, "{node} disk sync failed on {file}, {dropped} ops dropped")
            }
            Entry::Halt { node } => write!(f, "{node} halted; restart scheduled"),
        }
    }
}

/// 64-bit FNV-1a.
///
/// Unlike std's `DefaultHasher`, whose algorithm may change between Rust releases, FNV-1a is a
/// fixed, published function. Integers are fed in little-endian order and `usize` as 64 bits, so
/// a trace hash is identical across operating systems, CPUs and compiler versions.
#[derive(Debug)]
pub(crate) struct Fnv1a(u64);

impl Default for Fnv1a {
    fn default() -> Self {
        Fnv1a(0xcbf2_9ce4_8422_2325)
    }
}

impl Hasher for Fnv1a {
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= u64::from(b);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn write_u16(&mut self, i: u16) {
        self.write(&i.to_le_bytes());
    }

    fn write_u32(&mut self, i: u32) {
        self.write(&i.to_le_bytes());
    }

    fn write_u64(&mut self, i: u64) {
        self.write(&i.to_le_bytes());
    }

    fn write_u128(&mut self, i: u128) {
        self.write(&i.to_le_bytes());
    }

    fn write_usize(&mut self, i: usize) {
        self.write_u64(i as u64);
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a_matches_reference_vectors() {
        let hash = |s: &str| {
            let mut h = Fnv1a::default();
            h.write(s.as_bytes());
            h.finish()
        };
        assert_eq!(hash(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(hash("a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(hash("foobar"), 0x8594_4171_f739_67e8);
    }
}
