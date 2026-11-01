//! `dst serve` and `dst client`: the key-value store as real processes, over TCP and real files.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use checker::Verdict;
use kv::{KvModel, KvMsg, Recorder};
use runtime_real::{Codec, Runtime, Stopped};
use sim_api::NodeId;

/// Search steps allowed per key when checking a real run's history.
const CHECK_BUDGET: u64 = 10_000_000;

/// Every node's address, servers first. An alias, so clap parses `--nodes` as one value (a
/// field written as `Vec<_>` would make it a repeated flag).
pub type Nodes = Vec<SocketAddr>;

fn codec() -> Codec<KvMsg> {
    Codec { encode: KvMsg::to_bytes, decode: KvMsg::from_bytes }
}

/// Runs server `id` until it halts (after a failed fsync; a supervisor should restart it).
pub fn serve(id: u32, nodes: Vec<SocketAddr>, servers: u32, data: PathBuf) -> ExitCode {
    let node = kv::cluster(servers, 0)(NodeId(id));
    let addr = nodes[id as usize];
    let mut runtime = match Runtime::start(NodeId(id), nodes, data.clone(), node, codec()) {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("n{id}: could not start on {addr}: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!("n{id}: serving on {addr}, data in {}", data.display());
    match runtime.run_until(|_| false, Duration::MAX) {
        Stopped::Halted => {
            eprintln!("n{id}: halted (a sync failed); restart me");
            ExitCode::FAILURE
        }
        Stopped::Done | Stopped::TimedOut => ExitCode::SUCCESS,
    }
}

/// Runs client `id` for `ops` operations, then checks what it saw for linearizability.
pub fn client(id: u32, nodes: Vec<SocketAddr>, servers: u32, ops: u32, timeout: u64) -> ExitCode {
    let node = kv::cluster(servers, ops)(NodeId(id));
    // A client keeps nothing on disk, but the runtime wants a directory.
    let data = std::env::temp_dir().join(format!("dst-client-{}", std::process::id()));
    let mut runtime = match Runtime::start(NodeId(id), nodes, data.clone(), node, codec()) {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("n{id}: could not start: {e}");
            return ExitCode::FAILURE;
        }
    };
    let started = Instant::now();
    let mut recorder = Recorder::default();
    let mut reported = 0;
    let stopped = runtime.run_until(
        |node| {
            let client = node.as_client_mut().expect("a client node");
            for event in client.drain_events() {
                recorder.record(event);
            }
            if client.completed() >= reported + 50 {
                reported = client.completed();
                println!("n{id}: {reported}/{ops} operations done");
            }
            client.is_done()
        },
        Duration::from_secs(timeout),
    );
    let _ = std::fs::remove_dir_all(&data);
    let done = runtime.node().as_client().map_or(0, kv::Client::completed);
    let elapsed = started.elapsed().as_secs_f64();
    println!("n{id}: {done}/{ops} operations in {elapsed:.1} s");
    if stopped != Stopped::Done {
        eprintln!("FAIL: the cluster did not serve every operation within {timeout} s");
        return ExitCode::FAILURE;
    }
    match checker::check(&KvModel, recorder.history(), CHECK_BUDGET) {
        Verdict::Linearizable => {
            println!(
                "PASS: the history of {} operations is linearizable",
                recorder.history().ops().len()
            );
            ExitCode::SUCCESS
        }
        verdict => {
            eprintln!("FAIL: {verdict:?}");
            ExitCode::FAILURE
        }
    }
}

/// A shorthand node list: `--nodes 7000-7003` means those ports on localhost.
pub fn parse_nodes(s: &str) -> Result<Vec<SocketAddr>, String> {
    if let Some((from, to)) = s.split_once('-')
        && let (Ok(from), Ok(to)) = (from.parse::<u16>(), to.parse::<u16>())
    {
        return Ok((from..=to).map(|port| SocketAddr::from(([127, 0, 0, 1], port))).collect());
    }
    s.split(',').map(|a| a.trim().parse().map_err(|e| format!("bad address {a:?}: {e}"))).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_lists_parse() {
        let range = parse_nodes("7000-7002").unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[2], "127.0.0.1:7002".parse().unwrap());
        let list = parse_nodes("10.0.0.1:1, 10.0.0.2:2").unwrap();
        assert_eq!(list[1], "10.0.0.2:2".parse().unwrap());
        assert!(parse_nodes("nonsense").is_err());
    }
}
