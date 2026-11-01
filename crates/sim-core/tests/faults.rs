//! Milestone M2: network faults, crashes and clock drift behave as specified, and a protocol that
//! should survive them (push-pull gossip) does.

use std::collections::BTreeSet;
use std::time::Duration;

use sim_api::{NodeId, Time};
use sim_core::toys::{Gossip, PingPong};
use sim_core::{Fault, NetConfig, SimConfig, Simulation, Stats, StopReason};

fn ids(n: u32) -> BTreeSet<NodeId> {
    (0..n).map(NodeId).collect()
}

fn set(nodes: &[u32]) -> BTreeSet<NodeId> {
    nodes.iter().copied().map(NodeId).collect()
}

fn trace_lines(sim: &Simulation<Gossip>) -> Vec<String> {
    sim.trace().events().expect("trace kept").iter().map(ToString::to_string).collect()
}

fn at(secs: u64) -> Time {
    Time::ZERO + Duration::from_secs(secs)
}

fn gossip_sim(config: SimConfig) -> Simulation<Gossip> {
    Simulation::new(config, |_| Gossip::default())
}

#[test]
fn certain_loss_stops_everything() {
    let config =
        SimConfig { net: NetConfig { drop: 1.0, ..NetConfig::default() }, ..SimConfig::new(1, 2) };
    let mut sim = Simulation::new(config, |_| PingPong::new(10));
    let summary = sim.run();
    assert_eq!(summary.stop, StopReason::Quiescent);
    assert_eq!(sim.node(NodeId(0)).completed(), 0);
    assert_eq!(summary.stats.lost, 1);
    assert_eq!(summary.stats.delivered, 0);
}

#[test]
fn duplicates_are_harmless_for_gossip() {
    let config = SimConfig {
        net: NetConfig { duplicate: 1.0, ..NetConfig::default() },
        ..SimConfig::new(2, 5)
    };
    let mut sim = gossip_sim(config);
    let summary = sim.run();
    assert_eq!(summary.stop, StopReason::Quiescent);
    assert!(summary.stats.duplicated > 0);
    assert!(sim.nodes().all(|(_, n)| n.known() == &ids(5)));
}

#[test]
fn split_blocks_until_healed() {
    let mut sim = gossip_sim(SimConfig::new(3, 5));
    sim.inject(Fault::Split(vec![NodeId(0), NodeId(1)]));

    // Nodes keep gossiping because nobody can complete their view.
    let summary = sim.run_until(at(5));
    assert_eq!(summary.stop, StopReason::TimeLimit);
    assert!(summary.stats.partitioned > 0);
    assert_eq!(sim.node(NodeId(0)).known(), &set(&[0, 1]));
    assert_eq!(sim.node(NodeId(4)).known(), &set(&[2, 3, 4]));

    sim.inject(Fault::Heal);
    assert_eq!(sim.run().stop, StopReason::Quiescent);
    assert!(sim.nodes().all(|(_, n)| n.known() == &ids(5)));
}

#[test]
fn one_way_partition_is_asymmetric() {
    let mut sim = gossip_sim(SimConfig::new(4, 2));
    sim.inject(Fault::OneWay { from: NodeId(0), to: NodeId(1) });
    sim.run_until(at(5));
    // n0 hears n1's pushes, but its replies and pushes to n1 are all lost.
    assert_eq!(sim.node(NodeId(0)).known(), &set(&[0, 1]));
    assert_eq!(sim.node(NodeId(1)).known(), &set(&[1]));
}

#[test]
fn crash_stops_node_and_restart_recovers() {
    let config = SimConfig { keep_trace_lines: true, ..SimConfig::new(5, 3) };
    let mut sim = gossip_sim(config);
    sim.inject(Fault::Crash(NodeId(2)));
    sim.run_until(at(2));
    assert_eq!(sim.node(NodeId(0)).known(), &set(&[0, 1]), "nobody can learn about crashed n2");

    sim.inject(Fault::Crash(NodeId(0)));
    sim.run_until(at(4));
    {
        let lines = trace_lines(&sim);
        let crash = lines.iter().position(|l| l.ends_with("fault crash n0")).unwrap();
        let after = &lines[crash + 1..];
        assert!(!after.iter().any(|l| l.contains("] n0 timer") || l.contains("] n0 <- ")));
        assert!(after.iter().any(|l| l.contains("n0 <-x- n1 NodeDown")));
        assert!(after.iter().any(|l| l.contains("] n1 timer")), "n1 keeps running");
    }

    sim.inject(Fault::Restart(NodeId(0)));
    sim.inject(Fault::Restart(NodeId(2)));
    assert_eq!(sim.run().stop, StopReason::Quiescent);
    assert!(sim.nodes().all(|(_, n)| n.known() == &ids(3)));

    let lines = trace_lines(&sim);
    let starts =
        |node: &str| lines.iter().filter(|l| l.ends_with(&format!("{node} start"))).count();
    assert_eq!(starts("n0"), 2, "boot + restart");
    assert_eq!(starts("n2"), 1, "crashed before its first boot, so only the restart");
}

/// The M2 "done when": gossip converges under full chaos on 1,000 seeds, and every kind of fault
/// actually fired somewhere in the sweep.
#[test]
fn gossip_survives_chaos_on_1000_seeds() {
    let mut total = Stats::default();
    for seed in 0..1000 {
        let mut sim = gossip_sim(SimConfig::chaos(seed, 7));
        let summary = sim.run();
        assert_eq!(summary.stop, StopReason::Quiescent, "seed {seed}");
        for (id, node) in sim.nodes() {
            assert!(sim.is_up(id), "seed {seed}: {id} still down");
            assert_eq!(node.known(), &ids(7), "seed {seed}: {id}");
        }
        let s = summary.stats;
        total.lost += s.lost;
        total.partitioned += s.partitioned;
        total.to_down_node += s.to_down_node;
        total.duplicated += s.duplicated;
        total.crashes += s.crashes;
        total.restarts += s.restarts;
        total.partitions += s.partitions;
    }
    assert!(total.lost > 0 && total.duplicated > 0, "{total:?}");
    assert!(total.partitions > 0 && total.partitioned > 0, "{total:?}");
    assert!(total.crashes > 0 && total.restarts == total.crashes, "{total:?}");
    assert!(total.to_down_node > 0, "{total:?}");
}
