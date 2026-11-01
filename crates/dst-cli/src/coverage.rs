//! `dst coverage`: campaigns of swarm runs, measured by the abstract states they explore.
//!
//! Every run gets a fault mix (a swarm seed, which picks which faults are on and how hard they
//! hit, plus fault types switched off) and its own run seed for everything else. A plain campaign
//! draws a fresh mix for every run. A guided one keeps a corpus of the mixes whose runs reached
//! states no earlier run had, and spends most runs on those mixes, often with one fault type
//! switched off or back on: the fuzzer idea of mutating inputs that found something new.
//!
//! Runs go in batches on all cores; the corpus is updated between batches, in run order, so a
//! campaign is reproducible from its seed whatever the thread count.

use std::collections::BTreeSet;
use std::process::ExitCode;
use std::time::Instant;

use harness::{Bug, Knob, Mode, NODES, OPS, SERVERS, run_cluster_with_coverage};
use rayon::prelude::*;

/// Runs between corpus updates.
const BATCH: u64 = 500;
/// Share of guided runs (out of 10) spent on corpus entries rather than fresh mixes.
const EXPLOIT: u64 = 8;

/// A fault mix.
#[derive(Clone, Debug)]
struct Mix {
    swarm_seed: u64,
    off: BTreeSet<Knob>,
}

/// SplitMix64: the campaign's own small random stream.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

pub fn campaign(runs: u64, guided: bool, bug: Option<Bug>, seed: u64) -> ExitCode {
    let started = Instant::now();
    let mut rng = Rng(seed);
    let mut corpus: Vec<Mix> = Vec::new();
    let mut seen: BTreeSet<u64> = BTreeSet::new();
    let (mut caught, mut first_caught) = (0u64, None);
    let kind = if guided { "guided" } else { "plain swarm" };
    println!(
        "{runs} runs ({kind}{}), campaign seed {seed}",
        bug.map(|b| format!(", bug {b}")).unwrap_or_default()
    );
    println!("| Runs | Distinct states | Corpus |");
    println!("|---|---|---|");

    let mut done = 0;
    while done < runs {
        let batch = BATCH.min(runs - done);
        let mixes: Vec<Mix> = (0..batch).map(|_| choose(&mut rng, guided, &corpus)).collect();
        let results: Vec<(BTreeSet<u64>, bool)> = mixes
            .par_iter()
            .enumerate()
            .map(|(i, mix)| {
                let mut config = Mode::Swarm.config_for(mix.swarm_seed, NODES);
                // The mix came from the swarm seed; everything else follows the run's own seed.
                config.seed = (seed << 32) ^ (done + i as u64);
                for knob in &mix.off {
                    knob.switch_off(&mut config);
                }
                let bugs = bug.map(Bug::bugs).unwrap_or_default();
                let outcome = run_cluster_with_coverage(config, SERVERS, OPS, bugs);
                let failed = outcome.failure_kind().is_some();
                (outcome.states, failed)
            })
            .collect();
        for (i, (states, failed)) in results.into_iter().enumerate() {
            if failed {
                caught += 1;
                first_caught.get_or_insert(done + i as u64);
            }
            let before = seen.len();
            seen.extend(states);
            if seen.len() > before {
                corpus.push(mixes[i].clone());
            }
        }
        done += batch;
        if done % 2_000 == 0 || done == runs {
            println!("| {done} | {} | {} |", seen.len(), corpus.len());
        }
    }
    println!(
        "{} distinct states in {runs} runs ({:.1} s)",
        seen.len(),
        started.elapsed().as_secs_f64()
    );
    if bug.is_some() {
        match first_caught {
            Some(first) => println!("bug caught in {caught} runs, first at run {first}"),
            None => println!("bug not caught"),
        }
    }
    ExitCode::SUCCESS
}

/// The next run's fault mix.
fn choose(rng: &mut Rng, guided: bool, corpus: &[Mix]) -> Mix {
    if guided && !corpus.is_empty() && rng.below(10) < EXPLOIT {
        let mut mix = corpus[rng.below(corpus.len() as u64) as usize].clone();
        // Half the time, mutate: switch one fault type off, or back on.
        if rng.below(2) == 0 {
            let knob = Knob::ALL[rng.below(Knob::ALL.len() as u64) as usize];
            if !mix.off.remove(&knob) {
                mix.off.insert(knob);
            }
        }
        mix
    } else {
        Mix { swarm_seed: rng.next(), off: BTreeSet::new() }
    }
}
