//! Leader leases: reads skip their round of heartbeats while the lease holds, what clients see
//! stays linearizable under every fault and clock drift, and a lease that ignores drift is caught.

use harness::{Mode, NODES, OPS, SERVERS, run_cluster, run_cluster_with_lease};
use kv::Bugs;
use raft::LeaseConfig;
use sim_core::SimConfig;

/// The lease the cluster's clocks call for: sized against their drift bound.
fn lease_for(config: &SimConfig) -> LeaseConfig {
    LeaseConfig { max_drift_ppm: config.clocks.max_drift_ppm }
}

#[test]
fn leases_skip_read_rounds() {
    // Same seed, same workload: with leases most reads are answered without a round of
    // AppendEntries, so fewer messages go out.
    let config = Mode::Calm.config_for(1, NODES);
    let without = run_cluster(config.clone(), SERVERS, OPS, Bugs::default());
    let with =
        run_cluster_with_lease(config.clone(), SERVERS, OPS, Bugs::default(), lease_for(&config));
    without.assert_healthy();
    with.assert_healthy();
    assert!(
        with.stats.delivered < without.stats.delivered,
        "{} messages with leases, {} without",
        with.stats.delivered,
        without.stats.delivered
    );
}

#[test]
fn leases_stay_linearizable_under_chaos() {
    for seed in 0..200 {
        let config = Mode::Chaos.config_for(seed, NODES);
        let lease = lease_for(&config);
        run_cluster_with_lease(config, SERVERS, OPS, Bugs::default(), lease).assert_healthy();
    }
}

#[test]
fn leases_stay_linearizable_in_swarm() {
    for seed in 0..200 {
        let config = Mode::Swarm.config_for(seed, NODES);
        let lease = lease_for(&config);
        run_cluster_with_lease(config, SERVERS, OPS, Bugs::default(), lease).assert_healthy();
    }
}

/// The targeted lease scenario on a three-server cluster with fast-drifting clocks:
///
/// 1. measure each server's clock rate (its clock's advance over real time);
/// 2. steer leadership to the slowest clock, by cutting off any other leader alone until the
///    slow one wins (heal as soon as a new leader exists);
/// 3. cut the slow leader off right as its lease is renewed, so the followers last heard from
///    it at the very round the lease counts from, and heal a second later.
///
/// A lease sized against the drift bound ends before the faster-clocked followers may elect a
/// new leader; one that ignores drift is still running when they do.
fn lease_scenario(seed: u64, bugs: Bugs) -> harness::Outcome {
    use harness::run_scripted_with_lease;
    use raft::Role;
    use sim_api::{NodeId, Time};
    use sim_core::Fault;
    use std::time::Duration;

    const SERVERS: u32 = 3;
    let mut config = Mode::Calm.config_for(seed, SERVERS + 2);
    config.clocks.max_drift_ppm = 400_000;
    let lease = lease_for(&config);
    let servers: Vec<NodeId> = (0..SERVERS).map(NodeId).collect();
    let ms = |n| Time::ZERO + Duration::from_millis(n);
    let mut first: Option<Vec<Time>> = None;
    let mut slowest: Option<NodeId> = None;
    let mut isolated: Option<(NodeId, u64)> = None;
    let mut cut_at: Option<Time> = None;
    let mut last_until: Option<(NodeId, Time)> = None;
    let mut done = false;
    run_scripted_with_lease(config, SERVERS, OPS, bugs, lease, move |sim| {
        let now = sim.now();
        if done {
            return Vec::new();
        }
        let leader = servers.iter().copied().find(|&id| {
            sim.is_up(id)
                && sim.node(id).as_server().is_some_and(|s| s.raft().role() == Role::Leader)
        });
        // 1. Clock rates, from two readings 100 ms of real time apart.
        let Some(start) = &first else {
            first = Some(servers.iter().map(|&id| sim.local_time(id)).collect());
            return Vec::new();
        };
        if slowest.is_none() {
            if now < ms(100) {
                return Vec::new();
            }
            slowest =
                servers.iter().copied().min_by_key(|id| sim.local_time(*id) - start[id.0 as usize]);
        }
        let slow = slowest.expect("measured");
        if let Some(at) = cut_at {
            if now >= at + Duration::from_secs(1) {
                done = true;
                return vec![Fault::Heal];
            }
            return Vec::new();
        }
        // 2. Steer leadership to the slowest clock.
        if let Some((cut, term)) = isolated {
            let moved_on = servers.iter().any(|&id| {
                id != cut
                    && sim
                        .node(id)
                        .as_server()
                        .is_some_and(|s| s.raft().role() == Role::Leader && s.raft().term() > term)
            });
            if moved_on {
                isolated = None;
                return vec![Fault::Heal];
            }
            return Vec::new();
        }
        let Some(leader) = leader else { return Vec::new() };
        let raft = sim.node(leader).as_server().expect("a server").raft();
        if leader != slow {
            if now >= ms(150) {
                isolated = Some((leader, raft.term()));
                return vec![Fault::Split(vec![leader])];
            }
            return Vec::new();
        }
        // 3. Cut the slow leader off as its lease is renewed.
        let Some(until) = raft.lease_until() else { return Vec::new() };
        let renewed = last_until.is_some_and(|(was, before)| was == leader && until > before);
        last_until = Some((leader, until));
        if renewed && raft.holds_lease(sim.local_time(leader)) {
            cut_at = Some(now);
            return vec![Fault::Split(vec![leader])];
        }
        Vec::new()
    })
}

#[test]
fn correct_leases_pass_the_lease_scenario() {
    for seed in 0..20 {
        lease_scenario(seed, Bugs::default()).assert_healthy();
    }
}

#[test]
fn a_lease_that_ignores_drift_is_caught() {
    let mut bugs = Bugs::default();
    bugs.raft.lease_ignores_drift = true;
    let outcomes: Vec<_> = (0..20).map(|seed| lease_scenario(seed, bugs)).collect();
    let caught: Vec<_> = outcomes.iter().filter(|o| !o.passed()).collect();
    for outcome in &caught {
        assert_eq!(outcome.failure_kind(), Some("Raft invariants"), "seed {}", outcome.seed);
    }
    assert!(caught.len() >= 10, "only {} of 20 seeds caught the lease bug", caught.len());
}
