//! The async facade under the full fault model: the async echo protocol behaves exactly like
//! the callback version, finishes under chaos, and replays deterministically.

use std::time::Duration;

use sim_api::{NodeId, Time};
use sim_async::AsyncNode;
use sim_async::echo::{Echo, async_echo};
use sim_core::{RunSummary, SimConfig, Simulation, StopReason};

const NODES: u32 = 4;
const REQUESTS: u32 = 20;

fn callbacks(config: SimConfig) -> (RunSummary, Simulation<Echo>) {
    let mut sim = Simulation::new(config, |id| Echo::new(id, REQUESTS));
    (sim.run(), sim)
}

fn async_version(
    config: SimConfig,
) -> (RunSummary, Simulation<AsyncNode<sim_async::echo::EchoMsg>>) {
    let mut sim = Simulation::new(config, |id| async_echo(id, REQUESTS));
    (sim.run(), sim)
}

fn clients() -> impl Iterator<Item = NodeId> {
    (1..NODES).map(NodeId)
}

#[test]
fn both_versions_behave_identically_under_chaos() {
    for seed in 0..200 {
        let (a, _) = callbacks(SimConfig::chaos(seed, NODES));
        let (b, _) = async_version(SimConfig::chaos(seed, NODES));
        assert_eq!(
            (a.stop, a.steps, a.end_time, a.stats),
            (b.stop, b.steps, b.end_time, b.stats),
            "seed {seed}: the async version diverged from the callback version"
        );
    }
}

#[test]
fn every_client_finishes_under_chaos() {
    for seed in 0..200 {
        let (summary, sim) = async_version(SimConfig::chaos(seed, NODES));
        assert_eq!(summary.stop, StopReason::Quiescent, "seed {seed}");
        for id in clients() {
            assert!(sim.node(id).is_finished(), "seed {seed}: {id} did not finish");
        }
        let (_, sim) = callbacks(SimConfig::chaos(seed, NODES));
        for id in clients() {
            assert!(sim.node(id).is_done(), "seed {seed}: callback {id} did not finish");
        }
    }
}

#[test]
fn same_seed_same_trace() {
    for seed in 0..50 {
        let (a, _) = async_version(SimConfig::chaos(seed, NODES));
        let (b, _) = async_version(SimConfig::chaos(seed, NODES));
        assert_eq!(a.trace_hash, b.trace_hash, "seed {seed}");
    }
}

#[test]
fn sleeping_wakes_at_the_right_virtual_time() {
    use std::cell::RefCell;
    use std::rc::Rc;

    let woke: Rc<RefCell<Vec<Time>>> = Rc::default();
    let log = Rc::clone(&woke);
    let config = SimConfig::new(7, 1);
    let mut sim = Simulation::new(config, move |_| {
        let log = Rc::clone(&log);
        AsyncNode::<()>::new(move |io| {
            let log = Rc::clone(&log);
            async move {
                for ms in [10, 20, 30] {
                    io.sleep(Duration::from_millis(ms)).await;
                    log.borrow_mut().push(io.now());
                }
            }
        })
    });
    let summary = sim.run();
    assert_eq!(summary.stop, StopReason::Quiescent);
    let ms = |n: u64| Time::from_nanos(n * 1_000_000);
    assert_eq!(*woke.borrow(), vec![ms(10), ms(30), ms(60)]);
    assert!(sim.node(NodeId(0)).is_finished());
}
