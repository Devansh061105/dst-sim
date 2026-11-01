use std::fmt;
use std::hash::Hasher;
use std::ops::Range;
use std::str::FromStr;
use std::time::Duration;

use sim_api::{NodeId, Rand, Time};

/// Settings for the nemesis: the random fault schedule (the name comes from Jepsen).
#[derive(Clone, Debug)]
pub struct FaultConfig {
    /// Gap between nemesis actions.
    pub interval: Range<Duration>,
    /// Faults are injected only during this initial window. When it ends, the network heals and
    /// every crashed node restarts, and disks stop failing, so tests can check the system recovers.
    pub active_for: Duration,
    /// Inject partitions: symmetric splits and one-way link failures.
    pub partitions: bool,
    /// Crash and restart nodes.
    pub crashes: bool,
    /// Most nodes down at the same time.
    pub max_down: u32,
    /// Most nodes whose disks may be corrupted ([`DiskConfig::corrupt`](sim_disk::DiskConfig)) over
    /// a whole run, each at most once. This is the fault budget a correct system is tested
    /// within: lost data can only be recovered from peers that still have it, and a node needs
    /// one successful restart to repair damage before more damage could hit its other copy.
    pub max_corrupted: u32,
    /// How long the supervisor waits before restarting a node that halted itself.
    pub restart_delay: Range<Duration>,
    /// Replay exactly these faults instead of choosing them at random. Every run records the
    /// faults it injected ([`Simulation::script`](crate::Simulation::script)), so a failing run
    /// can be replayed with its schedule written out, then shrunk by deleting faults from it.
    pub script: Option<Script>,
}

/// A written-out fault schedule.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Script {
    /// Faults and when to inject them. Faults that no longer make sense when their turn comes
    /// (crashing a node that is already down, restarting one that is up) are skipped, so any
    /// subset of a recorded schedule is still a valid schedule.
    pub faults: Vec<(Time, Fault)>,
    /// When the faults end: the network heals, every crashed node restarts, disks stop failing.
    pub calm_at: Time,
}

impl FaultConfig {
    /// No faults at all.
    pub fn none() -> Self {
        FaultConfig {
            interval: Duration::from_millis(100)..Duration::from_secs(1),
            active_for: Duration::from_secs(10),
            partitions: false,
            crashes: false,
            max_down: 0,
            max_corrupted: 0,
            restart_delay: Duration::from_millis(100)..Duration::from_millis(500),
            script: None,
        }
    }

    /// Whether the random nemesis runs (a script replaces it).
    pub fn enabled(&self) -> bool {
        self.script.is_none() && (self.partitions || self.crashes)
    }
}

/// One fault. The nemesis picks these at random; tests can also inject them to script a scenario.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    /// Remove every partition.
    Heal,
    /// The listed nodes can't reach the rest, in either direction. Replaces any earlier partition.
    Split(Vec<NodeId>),
    /// Messages from `from` to `to` are lost; the reverse direction still works. Replaces any
    /// earlier partition.
    OneWay { from: NodeId, to: NodeId },
    /// The node stops. Its memory and pending timers are lost; messages sent to it are dropped.
    Crash(NodeId),
    /// The node boots again with fresh memory.
    Restart(NodeId),
}

impl Fault {
    // Hand-written so the byte layout is fixed by this code alone (see `Trace`).
    pub(crate) fn hash_into(&self, h: &mut impl Hasher) {
        match self {
            Fault::Heal => h.write_u8(0),
            Fault::Split(side) => {
                h.write_u8(1);
                h.write_u32(side.len() as u32);
                for node in side {
                    h.write_u32(node.0);
                }
            }
            Fault::OneWay { from, to } => {
                h.write_u8(2);
                h.write_u32(from.0);
                h.write_u32(to.0);
            }
            Fault::Crash(node) => {
                h.write_u8(3);
                h.write_u32(node.0);
            }
            Fault::Restart(node) => {
                h.write_u8(4);
                h.write_u32(node.0);
            }
        }
    }
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Fault::Heal => write!(f, "heal"),
            Fault::Split(side) => {
                write!(f, "split [")?;
                for (i, node) in side.iter().enumerate() {
                    write!(f, "{}{node}", if i == 0 { "" } else { " " })?;
                }
                write!(f, "] | rest")
            }
            Fault::OneWay { from, to } => write!(f, "one-way {from} -/-> {to}"),
            Fault::Crash(node) => write!(f, "crash {node}"),
            Fault::Restart(node) => write!(f, "restart {node}"),
        }
    }
}

/// Parses what `Display` prints, so schedules can be saved as text and read back.
impl FromStr for Fault {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let node = |word: &str| {
            word.strip_prefix('n')
                .and_then(|n| n.parse().ok())
                .map(NodeId)
                .ok_or_else(|| format!("expected a node like n3, got {word:?}"))
        };
        let words: Vec<&str> = s.split_whitespace().collect();
        match words.as_slice() {
            ["heal"] => Ok(Fault::Heal),
            ["crash", n] => Ok(Fault::Crash(node(n)?)),
            ["restart", n] => Ok(Fault::Restart(node(n)?)),
            ["one-way", from, "-/->", to] => Ok(Fault::OneWay { from: node(from)?, to: node(to)? }),
            ["split", ..] => {
                let side = s
                    .strip_prefix("split [")
                    .and_then(|r| r.strip_suffix("] | rest"))
                    .ok_or_else(|| format!("expected \"split [n0 n1] | rest\", got {s:?}"))?;
                let side = side.split_whitespace().map(node).collect::<Result<Vec<_>, _>>()?;
                if side.is_empty() {
                    return Err(format!("empty split in {s:?}"));
                }
                Ok(Fault::Split(side))
            }
            _ => Err(format!("unknown fault {s:?}")),
        }
    }
}

/// Picks the nemesis's next fault, or `None` if no fault is allowed right now.
pub(crate) fn choose(
    config: &FaultConfig,
    ids: &[NodeId],
    up: &[bool],
    rand: &mut impl Rand,
) -> Option<Fault> {
    #[derive(Clone, Copy)]
    enum Kind {
        Heal,
        Split,
        OneWay,
        Crash,
        Restart,
    }

    let n = ids.len() as u64;
    let running: Vec<NodeId> = ids.iter().copied().filter(|id| up[id.0 as usize]).collect();
    let down: Vec<NodeId> = ids.iter().copied().filter(|id| !up[id.0 as usize]).collect();

    let mut kinds = Vec::new();
    if config.partitions && n >= 2 {
        kinds.extend([Kind::Heal, Kind::Split, Kind::OneWay]);
    }
    if config.crashes {
        if (down.len() as u32) < config.max_down && !running.is_empty() {
            kinds.push(Kind::Crash);
        }
        if !down.is_empty() {
            kinds.push(Kind::Restart);
        }
    }
    if kinds.is_empty() {
        return None;
    }

    let fault = match kinds[rand.below(kinds.len() as u64) as usize] {
        Kind::Heal => Fault::Heal,
        Kind::Split => {
            let mut shuffled = ids.to_vec();
            shuffle(&mut shuffled, rand);
            // 1..n-1 nodes on the split side, so both sides are non-empty.
            let size = 1 + rand.below(n - 1) as usize;
            let mut side = shuffled[..size].to_vec();
            side.sort();
            Fault::Split(side)
        }
        Kind::OneWay => {
            let from = rand.below(n);
            let to = (from + 1 + rand.below(n - 1)) % n;
            Fault::OneWay { from: ids[from as usize], to: ids[to as usize] }
        }
        Kind::Crash => Fault::Crash(running[rand.below(running.len() as u64) as usize]),
        Kind::Restart => Fault::Restart(down[rand.below(down.len() as u64) as usize]),
    };
    Some(fault)
}

/// Fisher–Yates shuffle driven by the simulation's randomness.
fn shuffle<T>(items: &mut [T], rand: &mut impl Rand) {
    for i in (1..items.len()).rev() {
        let j = rand.below(i as u64 + 1) as usize;
        items.swap(i, j);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::{SimRng, Stream};

    fn ids(n: u32) -> Vec<NodeId> {
        (0..n).map(NodeId).collect()
    }

    #[test]
    fn partitions_only_never_crashes_and_splits_are_proper() {
        let config = FaultConfig { partitions: true, ..FaultConfig::none() };
        let ids = ids(5);
        let mut rng = SimRng::new(1, Stream::Faults);
        for _ in 0..1000 {
            match choose(&config, &ids, &[true; 5], &mut rng).expect("a fault is allowed") {
                Fault::Split(side) => assert!(!side.is_empty() && side.len() < 5),
                Fault::OneWay { from, to } => assert_ne!(from, to),
                Fault::Heal => {}
                other => panic!("partitions-only config produced {other:?}"),
            }
        }
    }

    #[test]
    fn crashes_respect_max_down() {
        let config = FaultConfig { crashes: true, max_down: 2, ..FaultConfig::none() };
        let ids = ids(5);
        let mut up = [true; 5];
        let mut rng = SimRng::new(2, Stream::Faults);
        for _ in 0..1000 {
            match choose(&config, &ids, &up, &mut rng).expect("a fault is allowed") {
                Fault::Crash(n) => up[n.0 as usize] = false,
                Fault::Restart(n) => up[n.0 as usize] = true,
                other => panic!("crashes-only config produced {other:?}"),
            }
            assert!(up.iter().filter(|u| !**u).count() <= 2);
        }
    }

    #[test]
    fn nothing_allowed_means_no_fault() {
        let mut rng = SimRng::new(3, Stream::Faults);
        assert_eq!(choose(&FaultConfig::none(), &ids(3), &[true; 3], &mut rng), None);
    }

    #[test]
    fn display_is_readable() {
        assert_eq!(Fault::Split(vec![NodeId(0), NodeId(3)]).to_string(), "split [n0 n3] | rest");
        assert_eq!(
            Fault::OneWay { from: NodeId(1), to: NodeId(2) }.to_string(),
            "one-way n1 -/-> n2"
        );
    }

    #[test]
    fn display_parses_back() {
        let faults = [
            Fault::Heal,
            Fault::Split(vec![NodeId(0), NodeId(3)]),
            Fault::OneWay { from: NodeId(4), to: NodeId(1) },
            Fault::Crash(NodeId(2)),
            Fault::Restart(NodeId(6)),
        ];
        for fault in faults {
            assert_eq!(fault.to_string().parse::<Fault>(), Ok(fault));
        }
        assert!("crash 3".parse::<Fault>().is_err());
        assert!("split [] | rest".parse::<Fault>().is_err());
        assert!("explode n1".parse::<Fault>().is_err());
    }
}
