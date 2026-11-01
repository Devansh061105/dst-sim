//! The key-value store as real processes: three `dst serve` processes on localhost, killed
//! (`kill -9` on Linux, `TerminateProcess` on Windows) and restarted one after another while a
//! `dst client` process writes and reads. The client then checks everything it saw with the same
//! linearizability checker the simulator uses.

// Real processes need real time.
#![allow(clippy::disallowed_methods)]

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

const DST: &str = env!("CARGO_BIN_EXE_dst");

/// Ports the OS says are free right now.
fn free_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<TcpListener> =
        (0..n).map(|_| TcpListener::bind("127.0.0.1:0").expect("bind")).collect();
    listeners.iter().map(|l| l.local_addr().expect("addr").port()).collect()
}

fn serve(id: usize, nodes: &str, data: &Path) -> Child {
    Command::new(DST)
        .args(["serve", "--id", &id.to_string(), "--nodes", nodes, "--servers", "3", "--data"])
        .arg(data.join(format!("n{id}")))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start a server")
}

struct Cluster {
    servers: Vec<Child>,
    data: PathBuf,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for server in &mut self.servers {
            let _ = server.kill();
            let _ = server.wait();
        }
        let _ = std::fs::remove_dir_all(&self.data);
    }
}

#[test]
fn killed_servers_recover_and_clients_see_a_linearizable_history() {
    let ports = free_ports(4);
    let nodes = ports.iter().map(|p| format!("127.0.0.1:{p}")).collect::<Vec<_>>().join(",");
    let data = std::env::temp_dir().join(format!("dst-real-cluster-{}", std::process::id()));
    let mut cluster = Cluster { servers: (0..3).map(|i| serve(i, &nodes, &data)).collect(), data };

    let mut client = Command::new(DST)
        .args(["client", "--id", "3", "--nodes", &nodes, "--ops", "200", "--timeout", "90"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start the client");

    // Kill each server in turn mid-run (one at a time, so a majority stays up) and restart it on
    // its data directory: what it acknowledged must still be there.
    let started = Instant::now();
    let mut kills = 0;
    while client.try_wait().expect("poll the client").is_none() {
        assert!(started.elapsed() < Duration::from_secs(120), "the client never finished");
        sleep(Duration::from_millis(400));
        let victim = kills % 3;
        let server = &mut cluster.servers[victim];
        server.kill().expect("kill a server");
        server.wait().expect("reap it");
        kills += 1;
        sleep(Duration::from_millis(150));
        cluster.servers[victim] = serve(victim, &nodes, &cluster.data);
    }

    let output = client.wait_with_output().expect("client output");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(stdout.contains("PASS"), "{stdout}");
    assert!(kills >= 2, "only {kills} kills: the run was too short to test recovery");
    println!("{stdout}servers killed and restarted: {kills}");
}
