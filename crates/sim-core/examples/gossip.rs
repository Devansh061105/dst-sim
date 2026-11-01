//! Runs the gossip toy protocol and prints its trace.
//!
//! ```text
//! cargo run -p sim-core --example gossip -- [seed] [nodes] [chaos]
//! ```
//! Run it twice with the same seed: the output is identical, line for line. Add `chaos` to turn
//! on message loss, duplication, delay spikes, partitions, crashes and clock drift.

use sim_core::toys::Gossip;
use sim_core::{SimConfig, Simulation};

fn main() {
    let mut args = std::env::args().skip(1);
    let seed = args.next().map_or(42, |s| s.parse().expect("seed must be a number"));
    let nodes = args.next().map_or(5, |s| s.parse().expect("nodes must be a number"));
    let chaos = args.next().is_some_and(|s| s == "chaos");

    let base = if chaos { SimConfig::chaos(seed, nodes) } else { SimConfig::new(seed, nodes) };
    let config = SimConfig { keep_trace_lines: true, ..base };
    let mut sim = Simulation::new(config, |_| Gossip::default());
    let summary = sim.run();

    for line in sim.trace().events().unwrap_or_default() {
        println!("{line}");
    }
    println!(
        "\nseed {seed}: {:?} after {} events at {}, trace hash {:#018x}\n{:?}",
        summary.stop, summary.steps, summary.end_time, summary.trace_hash, summary.stats
    );
}
