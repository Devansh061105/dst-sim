//! A run written out in full: seed, fault preset, planted bug, workload, which fault types are
//! switched off, and the exact fault schedule. Saved as a small text file so a shrunk failure can
//! be replayed, read, and attached to a bug report.

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use sim_api::{NodeId, Time};
use sim_core::{Fault, Script, SimConfig};

use crate::{Bug, Mode, OPS, Outcome, SERVERS, run_cluster};

/// A kind of fault the shrinker can switch off for the whole run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Knob {
    Loss,
    Duplication,
    DelaySpikes,
    ClockDrift,
    ClockOffset,
    /// Unsynced writes that make it to disk before a crash anyway (off = all are lost).
    UnsyncedSurvival,
    TornWrites,
    FsyncFailures,
    CrashDuringFsync,
    Corruption,
    Buggify,
}

impl Knob {
    pub const ALL: [Knob; 11] = [
        Knob::Loss,
        Knob::Duplication,
        Knob::DelaySpikes,
        Knob::ClockDrift,
        Knob::ClockOffset,
        Knob::UnsyncedSurvival,
        Knob::TornWrites,
        Knob::FsyncFailures,
        Knob::CrashDuringFsync,
        Knob::Corruption,
        Knob::Buggify,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Knob::Loss => "loss",
            Knob::Duplication => "duplication",
            Knob::DelaySpikes => "delay-spikes",
            Knob::ClockDrift => "clock-drift",
            Knob::ClockOffset => "clock-offset",
            Knob::UnsyncedSurvival => "unsynced-survival",
            Knob::TornWrites => "torn-writes",
            Knob::FsyncFailures => "fsync-failures",
            Knob::CrashDuringFsync => "crash-during-fsync",
            Knob::Corruption => "corruption",
            Knob::Buggify => "buggify",
        }
    }

    /// Whether this fault can happen at all under `config`.
    pub fn is_on(self, config: &SimConfig) -> bool {
        match self {
            Knob::Loss => config.net.drop > 0.0,
            Knob::Duplication => config.net.duplicate > 0.0,
            Knob::DelaySpikes => config.net.spike > 0.0,
            Knob::ClockDrift => config.clocks.max_drift_ppm > 0,
            Knob::ClockOffset => config.clocks.max_offset > Duration::ZERO,
            Knob::UnsyncedSurvival => config.disk.unsynced_survive > 0.0,
            Knob::TornWrites => config.disk.torn_write > 0.0,
            Knob::FsyncFailures => config.disk.sync_fail > 0.0,
            Knob::CrashDuringFsync => config.disk.crash_during_sync > 0.0,
            Knob::Corruption => config.disk.corrupt > 0.0,
            Knob::Buggify => config.buggify > 0.0,
        }
    }

    fn switch_off(self, config: &mut SimConfig) {
        match self {
            Knob::Loss => config.net.drop = 0.0,
            Knob::Duplication => config.net.duplicate = 0.0,
            Knob::DelaySpikes => config.net.spike = 0.0,
            Knob::ClockDrift => config.clocks.max_drift_ppm = 0,
            Knob::ClockOffset => config.clocks.max_offset = Duration::ZERO,
            Knob::UnsyncedSurvival => config.disk.unsynced_survive = 0.0,
            Knob::TornWrites => config.disk.torn_write = 0.0,
            Knob::FsyncFailures => config.disk.sync_fail = 0.0,
            Knob::CrashDuringFsync => config.disk.crash_during_sync = 0.0,
            Knob::Corruption => config.disk.corrupt = 0.0,
            Knob::Buggify => config.buggify = 0.0,
        }
    }
}

impl FromStr for Knob {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        Knob::ALL
            .into_iter()
            .find(|k| k.name() == s)
            .ok_or_else(|| format!("unknown fault type {s:?}"))
    }
}

/// Everything that determines one run of the standard cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scenario {
    pub seed: u64,
    pub mode: Mode,
    pub bug: Option<Bug>,
    pub clients: u32,
    pub ops: u32,
    /// Fault types switched off on top of what `mode` sets.
    pub off: BTreeSet<Knob>,
    /// The fault schedule, or `None` for the random nemesis of `mode`.
    pub script: Option<Script>,
}

impl Scenario {
    /// The standard run for a seed: 5 servers, 2 clients doing 30 operations each, random faults.
    pub fn new(mode: Mode, seed: u64, bug: Option<Bug>) -> Self {
        Scenario { seed, mode, bug, clients: 2, ops: OPS, off: BTreeSet::new(), script: None }
    }

    pub fn nodes(&self) -> u32 {
        SERVERS + self.clients
    }

    pub fn config(&self) -> SimConfig {
        let mut config = self.mode.config_for(self.seed, self.nodes());
        for knob in &self.off {
            knob.switch_off(&mut config);
        }
        if let Some(script) = &self.script {
            // Drop faults about nodes this scenario doesn't have (e.g. a removed client).
            let exists = |n: &NodeId| n.0 < self.nodes();
            let faults = script.faults.iter().filter(|(_, f)| match f {
                Fault::Heal => true,
                Fault::Split(side) => side.iter().all(exists),
                Fault::OneWay { from, to } => exists(from) && exists(to),
                Fault::Crash(n) | Fault::Restart(n) => exists(n),
            });
            config.faults.script =
                Some(Script { faults: faults.cloned().collect(), calm_at: script.calm_at });
        }
        config
    }

    pub fn run(&self, keep_trace: bool) -> Outcome {
        let config = SimConfig { keep_trace_lines: keep_trace, ..self.config() };
        run_cluster(config, SERVERS, self.ops, self.bug.map(Bug::bugs).unwrap_or_default())
    }
}

/// The text format: one setting per line, `#` starts a comment. Times are in nanoseconds of
/// virtual time so a file replays exactly.
///
/// ```text
/// seed 1646
/// mode chaos
/// bug commit-beyond-match
/// clients 1
/// ops 4
/// off loss duplication torn-writes
/// calm 2345678901
/// fault 1203456789 crash n3
/// fault 1500000000 split [n0 n1] | rest
/// ```
impl fmt::Display for Scenario {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "# dst scenario: replay with `dst run --scenario <this file>`")?;
        writeln!(f, "seed {}", self.seed)?;
        writeln!(f, "mode {}", self.mode)?;
        if let Some(bug) = self.bug {
            writeln!(f, "bug {bug}")?;
        }
        writeln!(f, "clients {}", self.clients)?;
        writeln!(f, "ops {}", self.ops)?;
        if !self.off.is_empty() {
            let names: Vec<_> = self.off.iter().map(|k| k.name()).collect();
            writeln!(f, "off {}", names.join(" "))?;
        }
        if let Some(script) = &self.script {
            writeln!(f, "# faults end (network heals, every node restarts) at {}", script.calm_at)?;
            writeln!(f, "calm {}", script.calm_at.as_nanos())?;
            for (at, fault) in &script.faults {
                writeln!(f, "fault {} {fault}  # {at}", at.as_nanos())?;
            }
        }
        Ok(())
    }
}

impl FromStr for Scenario {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let mut seed = None;
        let mut mode = None;
        let mut scenario = Scenario::new(Mode::Chaos, 0, None);
        let mut calm = None;
        let mut faults = Vec::new();
        for (number, line) in text.lines().enumerate() {
            let line = line.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let at_line = |e: String| format!("line {}: {e}", number + 1);
            let (key, value) = line.split_once(' ').unwrap_or((line, ""));
            let number = |v: &str| v.parse::<u64>().map_err(|e| at_line(format!("{v:?}: {e}")));
            match key {
                "seed" => seed = Some(number(value)?),
                "mode" => mode = Some(value.parse().map_err(at_line)?),
                "bug" => scenario.bug = Some(value.parse().map_err(at_line)?),
                "clients" => scenario.clients = number(value)? as u32,
                "ops" => scenario.ops = number(value)? as u32,
                "off" => {
                    for name in value.split_whitespace() {
                        scenario.off.insert(name.parse().map_err(at_line)?);
                    }
                }
                "calm" => calm = Some(Time::from_nanos(number(value)?)),
                "fault" => {
                    let (at, fault) = value.split_once(' ').ok_or_else(|| {
                        at_line("expected `fault <nanoseconds> <fault>`".to_owned())
                    })?;
                    faults.push((Time::from_nanos(number(at)?), fault.parse().map_err(at_line)?));
                }
                _ => return Err(at_line(format!("unknown setting {key:?}"))),
            }
        }
        scenario.seed = seed.ok_or("missing `seed`")?;
        scenario.mode = mode.ok_or("missing `mode`")?;
        scenario.script = match calm {
            Some(calm_at) => Some(Script { faults, calm_at }),
            None if faults.is_empty() => None,
            None => return Err("faults listed without a `calm` time".to_owned()),
        };
        Ok(scenario)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_round_trips() {
        let mut scenario = Scenario::new(Mode::Swarm, 77, Some(Bug::LocalReads));
        scenario.clients = 1;
        scenario.ops = 4;
        scenario.off = [Knob::Loss, Knob::TornWrites].into();
        scenario.script = Some(Script {
            faults: vec![
                (Time::from_nanos(1_203_456_789), Fault::Crash(NodeId(3))),
                (Time::from_nanos(1_500_000_000), Fault::Split(vec![NodeId(0), NodeId(1)])),
                (Time::from_nanos(1_700_000_000), Fault::Heal),
            ],
            calm_at: Time::from_nanos(2_345_678_901),
        });
        assert_eq!(scenario.to_string().parse::<Scenario>(), Ok(scenario.clone()));

        let plain = Scenario::new(Mode::Chaos, 5, None);
        assert_eq!(plain.to_string().parse::<Scenario>(), Ok(plain));
    }

    #[test]
    fn bad_text_is_rejected_with_a_line_number() {
        let err = "seed 1\nmode chaos\nfault 10 explode n1\ncalm 20".parse::<Scenario>();
        assert!(err.unwrap_err().starts_with("line 3:"));
        assert!("mode chaos".parse::<Scenario>().is_err());
    }

    #[test]
    fn switching_a_knob_off_turns_that_fault_off() {
        let base = Mode::Chaos.config(1);
        for knob in Knob::ALL.into_iter().filter(|k| k.is_on(&base)) {
            let mut config = base.clone();
            knob.switch_off(&mut config);
            assert!(!knob.is_on(&config), "{}", knob.name());
        }
    }
}
