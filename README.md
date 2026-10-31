# dst-sim

A **deterministic simulation testing** framework in Rust, inspired by FoundationDB and TigerBeetle's VOPR.

It runs an entire distributed cluster on a single thread with virtual time, injects network
partitions, node crashes, clock drift and disk faults, and makes every run reproducible from one
64-bit seed. The system under test is a Raft-replicated key-value store, checked for
linearizability after every run.

> **Status: in development.** Phase 0 (workspace, lints, CI) is done. See
> [docs/ROADMAP.md](docs/ROADMAP.md) for progress and [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md)
> for the design.

## Planned features

- Discrete-event simulator with seeded RNG streams and a `(time, seq)` event queue
- Fault injection: drop / delay / duplicate / reorder, symmetric and asymmetric partitions,
  crash-restart, clock drift, torn writes, lost fsync, bit flips
- Raft KV store with persistence, snapshots and linearizable reads
- Linearizability checker (WGL / Porcupine algorithm)
- Parallel seed sweeps, swarm testing, BUGGIFY, a planted-bug suite and failure shrinking
- HTML space-time trace viewer

## Layout

| Crate | Role |
|---|---|
| `sim-api` | Traits and types the system under test uses to talk to the world |
| `sim-core` | Simulator: RNG, virtual clock, event queue, main loop, trace |
| `sim-net` | Simulated network with fault injection |
| `sim-disk` | Simulated disk with fault injection |
| `checker` | Linearizability checker |
| `raft` | Raft consensus, written only against `sim-api` |
| `kv` | Replicated key-value store and client |
| `dst-cli` | `dst` binary: run, replay, sweep, shrink |

## Building

```bash
cargo test --workspace
cargo run --bin dst
```
