//! `dst` command-line tool: run, replay, sweep and shrink simulations.

// This binary drives simulations rather than being one: real threads (rayon) and wall-clock
// timing (for seeds per second) are fine here, so the determinism lint is switched off.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::ops::Range;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use clap::{Parser, Subcommand};
use harness::{Bug, Mode, Outcome, run_seed};
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
        #[arg(long)]
        seed: u64,
        /// calm, chaos or swarm
        #[arg(long, default_value = "chaos")]
        mode: Mode,
        /// Plant a bug (see `dst bugs` for the list)
        #[arg(long)]
        bug: Option<Bug>,
        /// Print every event
        #[arg(long)]
        trace: bool,
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
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Run { seed, mode, bug, trace } => run(seed, mode, bug, trace),
        Command::Sweep { seeds, mode, bug, jobs, failures } => {
            pool(jobs).install(|| sweep(seeds, mode, bug, &failures))
        }
        Command::Bugs { seeds, mode, jobs } => pool(jobs).install(|| bugs(seeds, mode)),
    }
}

fn run(seed: u64, mode: Mode, bug: Option<Bug>, trace: bool) -> ExitCode {
    let outcome = run_seed(mode, seed, bug, trace);
    for line in outcome.trace.iter().flatten() {
        println!("{line}");
    }
    println!(
        "seed {seed} ({mode}{}): {} operations, {} entries committed, max term {}, ended at {}, \
         trace hash {:#018x}",
        bug.map(|b| format!(", bug {b}")).unwrap_or_default(),
        outcome.operations,
        outcome.committed,
        outcome.max_term,
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
            println!("replay: {}", replay_command(seed, mode, bug));
            ExitCode::FAILURE
        }
    }
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
    println!("replay the first: {}", replay_command(failures[0].0, mode, bug));
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
                outcome.failure().map(|_| (seed, kind(&outcome)))
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

/// Which check failed first.
fn kind(outcome: &Outcome) -> &'static str {
    if outcome.violation.is_some() {
        "Raft invariants"
    } else if !outcome.clients_done {
        "liveness"
    } else {
        "linearizability"
    }
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
         failures, {} crashes mid-fsync, {} torn writes",
        s.crashes,
        s.halts,
        s.partitions,
        s.lost,
        s.duplicated,
        s.sync_failures,
        s.sync_crashes,
        s.torn_writes
    )
}

fn replay_command(seed: u64, mode: Mode, bug: Option<Bug>) -> String {
    let bug = bug.map(|b| format!(" --bug {b}")).unwrap_or_default();
    format!("cargo run --release -p dst-cli -- run --seed {seed} --mode {mode}{bug} --trace")
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
