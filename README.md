# dst-sim

A **deterministic simulation testing** framework in Rust, inspired by FoundationDB and TigerBeetle's VOPR.

It runs a whole distributed cluster on one thread in virtual time, injects network partitions,
message loss, node crashes, clock drift and disk faults, and makes every run reproducible from a
single 64-bit seed. The system under test is a Raft-replicated key-value store. Every run is
checked step by step against Raft's safety properties and, at the end, for linearizability.

> **Status:** milestones M1–M6 complete (simulator, faults, Raft KV, linearizability checking,
> bug-hunting CLI). Next: failure shrinking and a trace viewer. See [docs/ROADMAP.md](docs/ROADMAP.md).

## Results

A 5-server Raft cluster with 2 clients, release build on 16 threads:

| | Chaos (fixed fault mix) | Swarm (random fault mix per seed) |
|---|---|---|
| Seeds | 100,000 | 100,000 |
| Result | **all pass**: no safety or linearizability violation, every client finished | **all pass** |
| Wall-clock time | 18 s (≈5,500 seeds/s) | 19 s (≈5,200 seeds/s) |
| Crashes / self-halts | 183,975 / 360,919 | 443,634 / 664,510 |
| Partitions | 425,013 | 551,661 |
| Lost / duplicated messages | 8.0 M / 3.0 M | 7.7 M / 7.3 M |
| `fsync` failures / crashes mid-`fsync` / torn writes | 180,486 / 180,433 / 30,992 | 329,914 / 334,596 / 85,998 |

Determinism: the same seed gives a bit-identical trace, verified on Linux and Windows in CI. Every
push runs a 10,000-seed chaos sweep and a 2,000-seed swarm sweep; a nightly job sweeps 200,000
swarm seeds it has never run before.

## Bugs it catches

**Planted bugs** (mutation testing). Each is a real mistake, behind a flag; the table shows the
mean number of seeds needed to catch it, measured over 10,000 seeds per bug with `dst bugs`.

| Bug | Chaos | Swarm | Caught by |
|---|---|---|---|
| Follower truncates its log even when entries match (stale messages erase entries) | 1.0 | 1.1 | Raft invariants |
| Appended entries acknowledged without `fsync` | 1.9 | 3.2 | Raft invariants |
| Votes granted without the "candidate's log is up to date" check | 2.2 | 3.4 | Raft invariants |
| Replicas answer reads from their own, possibly stale, copy | 8.5 | 5.5 | linearizability |
| Follower commits past the entries a message verified (a Figure 2 subtlety) | ~1,700 | ~1,100 | Raft invariants |
| Vote kept in memory but not persisted | not yet | not yet | — |
| Leader commits earlier-term entries by counting replicas (Figure 8) | not yet | not yet | — |
| Truncate and append share one `fsync` | not yet | not yet | — |

The last three need a precise multi-step sequence (for example Figure 8 of the Raft paper) that
random faults have not lined up in 10,000 seeds; targeted scenarios are future work. The
`commit-beyond-match` bug is only reachable when a message carries fewer entries than the
follower holds, which in practice takes BUGGIFY's one-entry batches.

**Durability bugs** in a toy write-ahead log: acknowledging before `fsync` (< 20 seeds) and
trusting a retried `fsync` after a failure ("fsyncgate", PostgreSQL 2018; < 300 seeds).

**Found while building it**:
- A restarted node could receive messages before it had recovered from disk (simulator bug,
  caught on the first chaos seed).
- One swarm seed in about 14,000 failed on correct code: a cut-off node won an *older* term through
  delayed votes after a newer term had committed an entry it lacked. Raft allows that; the
  invariant checker was too strict. The checker was fixed, and the seed is a regression test.

## How it works

```mermaid
flowchart LR
    seed(["seed (u64)"]) --> rng["Seeded RNG<br/>one stream per subsystem"]
    subgraph sim["Simulator: one thread, virtual time"]
        rng --> faults["Fault models<br/>network · disk · nemesis · clocks"]
        queue[("Event queue<br/>ordered by (time, seq)")] --> step["Step loop"]
        faults --> step
        step --> nodes["Nodes (actors)<br/>Raft KV servers + clients"]
        nodes -- "send / set_timer / storage" --> step
    end
    step --> trace["Trace + FNV-1a hash"]
    step --> inv["Raft invariant checker<br/>after every step"]
    nodes -- "client history" --> lin["Linearizability checker<br/>(WGL search)"]
```

**Determinism.** Nodes are event handlers that touch the world only through a `Context` trait:
send a message, set a timer, read the clock, use the disk. The simulator owns time, delivers
events in `(virtual time, sequence number)` order, and draws every random choice (latency, loss,
crashes, election timeouts) from ChaCha8 streams derived from the seed, one stream per subsystem
so that changing one doesn't reshuffle another. A clippy lint bans wall-clock time, threads and
`HashMap` in simulated code. Each run is fingerprinted by a portable FNV-1a hash of its events;
two golden-hash tests pin exact runs across operating systems.

**Fault model.**

| Layer | Faults |
|---|---|
| Network | loss, duplication, delay spikes (reordering), symmetric and one-way partitions |
| Nodes | crash and restart (memory and timers lost), supervisor restart after a self-halt |
| Clocks | per-node offset and drift (up to ±5%) |
| Disk | unsynced writes lost or partly kept on crash, torn writes, `fsync` failure (writes dropped, as on Linux), crash in the middle of `fsync` |

A Jepsen-style nemesis applies partitions and crashes for the first 10 s of virtual time, then
heals everything so the checks can confirm the cluster recovers.

**Swarm testing.** In swarm mode every seed also picks which faults are on and how hard they hit
(loss, duplication, delay spikes, partitions, crashes, clock drift, each disk fault, nemesis pace),
from its own random stream. A single fixed mix tends to hide bugs that need one fault to be absent
so another can reach a rare state.

**BUGGIFY.** Raft and the store contain FoundationDB-style hooks that, in simulation, occasionally
take rare but correct paths: one-entry batches, skipped heartbeats, minimum election timeouts,
ignoring a rejection's hint, delayed replication, re-proposing retried requests, impatient client
retries. Outside simulation they are always off.

**System under test.** Raft (election, replication, commit rules from Figure 2 of the paper) with
durable term, vote and log, plus a key-value store with `get` / `put` / `cas`. All operations go
through the log; client sessions with sequence numbers make retries exactly-once.

**Checking.** After every step, all five safety properties of Figure 3 of the Raft paper: election
safety, log matching (via prefix hashes of every `(index, term)` ever seen), leader completeness,
state machine safety, and committed entries never lost. After every run: the clients' recorded history must be
linearizable (Wing & Gong search with memoisation, partitioned per key). Operations whose outcome
is unknown (client timed out or crashed) may or may not have taken effect.

## Try it

```bash
# All tests (a few seconds)
cargo test --workspace

# Watch a gossip protocol under chaos; same seed, same output
cargo run -p sim-core --example gossip -- 42 5 chaos

# The dst tool (build once)
cargo build --release -p dst-cli

# Run one seed; on failure it prints the command that replays it
./target/release/dst run --seed 42 --mode chaos

# Plant a bug and watch it get caught; --trace prints every event
./target/release/dst run --seed 0 --mode chaos --bug local-reads --trace

# Sweep seeds in parallel on every core
./target/release/dst sweep --seeds 0..100000 --mode swarm

# The planted-bug table above
./target/release/dst bugs --seeds 10000 --mode chaos
```

Modes: `calm` (no faults), `chaos` (every fault, fixed intensity), `swarm` (random mix per seed).
Bugs: `local-reads`, `vote-without-log-check`, `forget-vote`, `truncate-without-conflict`,
`commit-beyond-match`, `skip-append-fsync`, `commit-old-terms`, `single-sync-truncate`.

## Layout

| Crate | Role |
|---|---|
| `sim-api` | What a system under test can use: `Node`, `Context`, `Storage`, virtual time, checksummed records |
| `sim-core` | Simulator: RNG streams, event queue, step loop, nemesis, clocks, trace |
| `sim-net` | Network fault model |
| `sim-disk` | Disk fault model with page-cache semantics |
| `raft` | Raft consensus, written only against `sim-api`, plus its invariant checker |
| `kv` | Replicated key-value store, retrying clients, sequential model for checking |
| `checker` | Generic linearizability checker |
| `harness` | Runs a checked cluster for one seed: invariants each step, liveness, linearizability |
| `dst-cli` | `dst` command-line tool: `run`, `sweep`, `bugs` |

## References

- D. Ongaro and J. Ousterhout, *In Search of an Understandable Consensus Algorithm* (Raft), 2014
- W. Wilson, *Testing Distributed Systems w/ Deterministic Simulation*, Strange Loop 2014
- TigerBeetle, *VOPR* simulation testing
- J. Wing and C. Gong, *Testing and Verifying Concurrent Objects*, 1993; A. Athalye, *Porcupine*
- A. Groce et al., *Swarm Testing*, ISSTA 2012
- K. Kingsbury, Jepsen analyses
