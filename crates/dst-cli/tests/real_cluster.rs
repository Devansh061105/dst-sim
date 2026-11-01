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

/// Kills that must land while a client is running. Each client does a fixed amount of work, and
/// how long that takes varies a lot between machines (Linux finishes in well under a second, a
/// Windows CI runner takes tens of seconds), so clients are started one after another until
/// enough kills have hit a running one. Each client gets a fresh cluster: the checker assumes
/// every key starts out absent, so a client must see the store from its very first write.
const KILLS: u32 = 3;

fn client(nodes: &str) -> Child {
    Command::new(DST)
        .args(["client", "--id", "3", "--nodes", nodes, "--ops", "200", "--timeout", "90"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start the client")
}

#[test]
fn killed_servers_recover_and_clients_see_a_linearizable_history() {
    let started = Instant::now();
    let (mut kills, mut clients) = (0, 0);
    while kills < KILLS {
        clients += 1;
        let ports = free_ports(4);
        let nodes = ports.iter().map(|p| format!("127.0.0.1:{p}")).collect::<Vec<_>>().join(",");
        let data =
            std::env::temp_dir().join(format!("dst-real-cluster-{}-{clients}", std::process::id()));
        let mut cluster =
            Cluster { servers: (0..3).map(|i| serve(i, &nodes, &data)).collect(), data };
        let mut running = client(&nodes);

        // Kill each server in turn mid-run (one at a time, so a majority stays up) and restart
        // it on its data directory: what it acknowledged must still be there.
        loop {
            assert!(started.elapsed() < Duration::from_secs(180), "the clients never finished");
            sleep(Duration::from_millis(400));
            if running.try_wait().expect("poll the client").is_some() {
                break;
            }
            let victim = (kills % 3) as usize;
            let server = &mut cluster.servers[victim];
            server.kill().expect("kill a server");
            server.wait().expect("reap it");
            kills += 1;
            sleep(Duration::from_millis(150));
            cluster.servers[victim] = serve(victim, &nodes, &cluster.data);
        }
        // Every client, including one that saw no kill, must see a linearizable history.
        let output = running.wait_with_output().expect("client output");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(stdout.contains("PASS"), "{stdout}");
        println!("client {clients}: {stdout}");
    }
    println!("{clients} clients, servers killed and restarted while one ran: {kills}");
}
