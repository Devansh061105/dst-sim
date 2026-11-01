//! `dst` command-line tool: run, replay, sweep and shrink simulations.

// This binary drives simulations rather than being one: real threads (rayon) and wall-clock
// timing (for seeds per second) are fine here, so the determinism lint is switched off.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::ops::Range;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

mod real;
mod viewer;

use clap::{Parser, Subcommand};
use std::path::Path;

use harness::{Bug, Mode, Scenario, run_seed};
use rayon::prelude::*;
use sim_core::Stats;

#[derive(Parser)]
#[command(name = "dst", about = "Deterministic simulation testing of a Raft key-value store")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run one seed. The same seed always replays the same run, so this is also how to debug a
    /// failure that a sweep found.
    Run {
        #[arg(long, required_unless_present = "scenario")]
        seed: Option<u64>,
        /// calm, chaos or swarm
        #[arg(long, default_value = "chaos")]
        mode: Mode,
        /// Plant a bug (see `dst bugs` for the list)
        #[arg(long)]
        bug: Option<Bug>,
        /// Replay a scenario file written by `dst shrink` instead of a seed
        #[arg(long, conflicts_with_all = ["seed", "bug"])]
        scenario: Option<PathBuf>,
        /// Print every event
        #[arg(long)]
        trace: bool,
        /// Write the run as a self-contained trace viewer page
        #[arg(long)]
        html: Option<PathBuf>,
        /// Write the run as JSON (open it in viewer/index.html)
        #[arg(long)]
        json: Option<PathBuf>,
    },
    /// Shrink a failing seed to a much smaller run that fails the same way, and save it as a
    /// scenario file.
    Shrink {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value = "chaos")]
        mode: Mode,
        #[arg(long)]
        bug: Option<Bug>,
        /// Where to save the shrunk scenario (default: shrunk-SEED.txt)
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Run a range of seeds in parallel and report any that fail.
    Sweep {
        /// Seeds to run, as START..END
        #[arg(long, default_value = "0..1000", value_parser = parse_range)]
        seeds: Range<u64>,
        #[arg(long, default_value = "chaos")]
        mode: Mode,
        #[arg(long)]
        bug: Option<Bug>,
        /// Worker threads (default: one per CPU core)
        #[arg(long)]
        jobs: Option<usize>,
        /// Where failing seeds are written, if there are any
        #[arg(long, default_value = "failures.txt")]
        failures: PathBuf,
    },
    /// For every planted bug, how many seeds it takes to catch it.
    Bugs {
        /// Seeds to try per bug
        #[arg(long, default_value_t = 1000)]
        seeds: u64,
        #[arg(long, default_value = "chaos")]
        mode: Mode,
        #[arg(long)]
        jobs: Option<usize>,
    },
    /// Run one server of a real cluster, over TCP and real files in DATA.
    Serve {
        /// This node's id: an index into --nodes
        #[arg(long)]
        id: u32,
        /// Every node's address, servers first: a comma-separated list, or a port range such as
        /// 7000-7003 for localhost
        #[arg(long, value_parser = real::parse_nodes)]
        nodes: real::Nodes,
        /// How many of the nodes are servers
        #[arg(long, default_value_t = 3)]
        servers: u32,
        #[arg(long)]
        data: PathBuf,
    },
    /// Run a client against a real cluster, then check what it saw for linearizability.
    Client {
        #[arg(long)]
        id: u32,
        #[arg(long, value_parser = real::parse_nodes)]
        nodes: real::Nodes,
        #[arg(long, default_value_t = 3)]
        servers: u32,
        #[arg(long, default_value_t = 200)]
        ops: u32,
        /// Give up after this many seconds
        #[arg(long, default_value_t = 60)]
        timeout: u64,
    },
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Run { seed, mode, bug, scenario, trace, html, json } => {
            let scenario = match (scenario, seed) {
                (Some(path), _) => match load(&path) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("{e}");
                        return ExitCode::FAILURE;
                    }
                },
                (None, Some(seed)) => Scenario::new(mode, seed, bug),
                (None, None) => unreachable!("clap requires --seed or --scenario"),
            };
            run(&scenario, trace, html.as_deref(), json.as_deref())
        }
        Command::Shrink { seed, mode, bug, out } => {
            let out = out.unwrap_or_else(|| PathBuf::from(format!("shrunk-{seed}.txt")));
            shrink_seed(&Scenario::new(mode, seed, bug), &out)
        }
        Command::Sweep { seeds, mode, bug, jobs, failures } => {
            pool(jobs).install(|| sweep(seeds, mode, bug, &failures))
        }
        Command::Bugs { seeds, mode, jobs } => pool(jobs).install(|| bugs(seeds, mode)),
        Command::Serve { id, nodes, servers, data } => real::serve(id, nodes, servers, data),
        Command::Client { id, nodes, servers, ops, timeout } => {
            real::client(id, nodes, servers, ops, timeout)
        }
    }
}

fn run(scenario: &Scenario, trace: bool, html: Option<&Path>, json: Option<&Path>) -> ExitCode {
    let outcome = scenario.run(trace || html.is_some() || json.is_some());
    if trace {
        for event in outcome.trace.iter().flatten() {
            println!("{event}");
        }
    }
    if let Some(path) = html {
        write_file(path, viewer::page(scenario, &outcome));
    }
    if let Some(path) = json {
        write_file(path, viewer::json(scenario, &outcome));
    }
    println!(
        "{}: {} operations, {} entries committed, max term {}, {} events, ended at {}, trace \
         hash {:#018x}",
        describe(scenario),
        outcome.operations,
        outcome.committed,
        outcome.max_term,
        outcome.events,
        outcome.end_time,
        outcome.trace_hash,
    );
    println!("{}", faults(&outcome.stats));
    match outcome.failure() {
        None => {
            println!("PASS");
            ExitCode::SUCCESS
        }
        Some(failure) => {
            println!("FAIL: {failure}");
            if scenario.script.is_none() {
                let (seed, mode, bug) = (scenario.seed, scenario.mode, scenario.bug);
                println!("replay: {}", dst_command("run", seed, mode, bug, " --trace"));
                println!("shrink: {}", dst_command("shrink", seed, mode, bug, ""));
            }
            ExitCode::FAILURE
        }
    }
}

fn shrink_seed(start: &Scenario, out: &Path) -> ExitCode {
    let started = Instant::now();
    let shrunk = match harness::shrink(start) {
        Ok(shrunk) => shrunk,
        Err(e) => {
            eprintln!("{}: {e}", describe(start));
            return ExitCode::FAILURE;
        }
    };
    let faults = |s: &Scenario| s.script.as_ref().map_or(0, |s| s.faults.len());
    println!("{}: fails with {}", describe(start), shrunk.kind);
    for step in &shrunk.steps {
        println!("  {step}");
    }
    // With fewer faults a failure can take longer to show, so the run may not get shorter.
    let size = if shrunk.ratio() > 1.0 {
        format!("{:.1}x fewer", shrunk.ratio())
    } else {
        "the failure now takes longer to show".to_owned()
    };
    println!(
        "shrunk in {:.1} s ({} simulations): {} faults -> {}, {} events -> {} ({size})",
        started.elapsed().as_secs_f64(),
        shrunk.runs,
        faults(&shrunk.original),
        faults(&shrunk.scenario),
        shrunk.original_events,
        shrunk.events,
    );
    if let Err(e) = std::fs::write(out, shrunk.scenario.to_string()) {
        eprintln!("could not write {}: {e}", out.display());
        return ExitCode::FAILURE;
    }
    println!(
        "saved to {}; replay with: dst run --scenario {} --trace",
        out.display(),
        out.display()
    );
    ExitCode::SUCCESS
}

fn write_file(path: &Path, contents: String) {
    match std::fs::write(path, contents) {
        Ok(()) => println!("wrote {}", path.display()),
        Err(e) => eprintln!("could not write {}: {e}", path.display()),
    }
}

fn load(path: &Path) -> Result<Scenario, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    text.parse().map_err(|e| format!("{}: {e}", path.display()))
}

fn describe(s: &Scenario) -> String {
    let bug = s.bug.map(|b| format!(", bug {b}")).unwrap_or_default();
    let scripted = if s.script.is_some() { ", scripted faults" } else { "" };
    format!("seed {} ({}{bug}{scripted})", s.seed, s.mode)
}

fn sweep(seeds: Range<u64>, mode: Mode, bug: Option<Bug>, failures_path: &PathBuf) -> ExitCode {
    let started = Instant::now();
    let results: Vec<(u64, Option<String>, Stats)> = seeds
        .clone()
        .into_par_iter()
        .map(|seed| {
            let outcome = run_seed(mode, seed, bug, false);
            (seed, outcome.failure(), outcome.stats)
        })
        .collect();
    let elapsed = started.elapsed().as_secs_f64();

    let mut total = Stats::default();
    for (_, _, stats) in &results {
        total += *stats;
    }
    let failures: Vec<_> =
        results.iter().filter_map(|(s, f, _)| f.as_ref().map(|f| (*s, f))).collect();
    let count = seeds.end.saturating_sub(seeds.start);
    println!(
        "{count} seeds ({mode}{}) in {elapsed:.1} s on {} threads: {:.0} seeds/s",
        bug.map(|b| format!(", bug {b}")).unwrap_or_default(),
        rayon::current_num_threads(),
        count as f64 / elapsed.max(1e-9),
    );
    println!("{}", faults(&total));

    if failures.is_empty() {
        println!("all seeds passed");
        return ExitCode::SUCCESS;
    }
    println!("{} seeds FAILED:", failures.len());
    for (seed, failure) in failures.iter().take(20) {
        println!("  seed {seed}: {failure}");
    }
    let report: String = failures.iter().map(|(seed, f)| format!("{seed}\t{f}\n")).collect();
    match std::fs::write(failures_path, report) {
        Ok(()) => println!("failing seeds written to {}", failures_path.display()),
        Err(e) => eprintln!("could not write {}: {e}", failures_path.display()),
    }
    println!("replay the first: {}", dst_command("run", failures[0].0, mode, bug, " --trace"));
    ExitCode::FAILURE
}

fn bugs(seeds: u64, mode: Mode) -> ExitCode {
    println!("Seeds to detect each planted bug ({mode} mode, {seeds} seeds each):\n");
    println!("| Bug | First failing seed | Caught in | Mean seeds to detect | Caught by |");
    println!("|---|---|---|---|---|");
    let mut all_caught = true;
    for bug in [None].into_iter().chain(Bug::ALL.map(Some)) {
        let failing: Vec<(u64, &'static str)> = (0..seeds)
            .into_par_iter()
            .filter_map(|seed| {
                let outcome = run_seed(mode, seed, bug, false);
                outcome.failure_kind().map(|kind| (seed, kind))
            })
            .collect();
        let name = bug.map_or_else(|| "(none: correct code)".to_owned(), |b| format!("`{b}`"));
        let caught = failing.len() as u64;
        let first = failing.first().map_or_else(|| "—".to_owned(), |(s, _)| s.to_string());
        let mean = if caught == 0 {
            "—".to_owned()
        } else {
            format!("{:.1}", seeds as f64 / caught as f64)
        };
        println!(
            "| {name} | {first} | {caught} / {seeds} | {mean} | {} |",
            most_common(failing.iter().map(|(_, k)| *k))
        );
        match bug {
            Some(_) if caught == 0 => all_caught = false,
            None if caught > 0 => all_caught = false,
            _ => {}
        }
    }
    if all_caught { ExitCode::SUCCESS } else { ExitCode::FAILURE }
}

fn most_common(kinds: impl Iterator<Item = &'static str>) -> &'static str {
    let mut counts = std::collections::BTreeMap::new();
    for k in kinds {
        *counts.entry(k).or_insert(0) += 1;
    }
    counts.into_iter().max_by_key(|&(_, n)| n).map_or("—", |(k, _)| k)
}

fn faults(s: &Stats) -> String {
    format!(
        "faults: {} crashes, {} halts, {} partitions, {} lost messages, {} duplicated, {} fsync \
         failures, {} crashes mid-fsync, {} torn writes, {} corrupted disks",
        s.crashes,
        s.halts,
        s.partitions,
        s.lost,
        s.duplicated,
        s.sync_failures,
        s.sync_crashes,
        s.torn_writes,
        s.corruptions
    )
}

/// A `dst` command line for one seed, runnable from the repository.
fn dst_command(command: &str, seed: u64, mode: Mode, bug: Option<Bug>, extra: &str) -> String {
    let bug = bug.map(|b| format!(" --bug {b}")).unwrap_or_default();
    format!("cargo run --release -p dst-cli -- {command} --seed {seed} --mode {mode}{bug}{extra}")
}

fn parse_range(s: &str) -> Result<Range<u64>, String> {
    let (start, end) = s.split_once("..").ok_or("expected START..END, e.g. 0..1000")?;
    let start: u64 = start.parse().map_err(|e| format!("bad start {start:?}: {e}"))?;
    let end: u64 = end.parse().map_err(|e| format!("bad end {end:?}: {e}"))?;
    if start >= end {
        return Err(format!("empty range {s}"));
    }
    Ok(start..end)
}

fn pool(jobs: Option<usize>) -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new().num_threads(jobs.unwrap_or(0)).build().expect("thread pool")
}
