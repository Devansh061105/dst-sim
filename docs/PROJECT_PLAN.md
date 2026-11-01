# dst-sim — Project Plan

> A deterministic simulation testing (DST) framework in Rust, inspired by FoundationDB and
> TigerBeetle's VOPR. It runs a whole distributed cluster on one thread, makes time, network and
> disk faults fully reproducible from a single `u64` seed, and finds real consistency bugs in a
> Raft-replicated key-value store it ships with.

---

## 1. Goals

| Goal | What "done" looks like |
|---|---|
| **Determinism** | Same seed → byte-identical event trace, on any machine, every time. Verified automatically. |
| **Fault injection** | Network (drop / delay / duplicate / reorder / partitions), node crashes, clock drift, disk faults (torn writes, lost fsync, bit flips). |
| **A real system under test** | Raft-based linearizable KV store with persistence, crash recovery, and snapshots. |
| **Correctness oracle** | Linearizability checker (Porcupine / WGL algorithm) plus per-step Raft safety invariants. |
| **Bug-finding evidence** | Planted bugs found automatically, with measured "seeds-to-detect", minimised reproductions, and a visual trace. |
| **Recruiter-ready packaging** | README with architecture diagram, metrics, flamegraph, a "bug museum", CI running seeds on every push. |

### Non-goals (deliberately out of scope)
- Byzantine (malicious) faults: crash and network faults are enough to find real bugs.
- Production-grade performance of the KV store. Correctness is the product.
- Simulating arbitrary existing async Rust code (that's what `madsim` does). We use our own node API.

---

## 2. Feature list

Features are tiered so there is **always a finished, showable project**: Core = MVP (week 8),
Advanced = what makes it stand out, Stretch = only if time remains.

### Tier 1 — Core (MVP)
1. **Seeded deterministic RNG.** One `ChaCha8Rng` root seed, forked into independent streams per
   subsystem (network, disk, clock, workload) so changing one subsystem doesn't reshuffle the others.
2. **Discrete-event simulator.** Virtual clock + priority queue of events ordered by `(time, seq)`.
   No real time, no threads, no OS I/O inside the simulation.
3. **Node API (actor style).** Each node implements `on_start`, `on_message`, `on_timer`,
   `on_client_request`; it talks to the world only through a `Context`
   (`send`, `set_timer`, `now`, `storage`, `rng`).
4. **Simulated network.** Configurable latency distribution, drop, duplicate and reorder
   probabilities; symmetric, asymmetric (A→B works, B→A doesn't) and isolate-one-node partitions.
5. **Node crash / restart.** A crash wipes volatile memory; only what was fsynced survives.
6. **Per-node clock drift.** Each node's local clock runs at a slightly different rate / offset.
7. **Simulated disk.** Write / read / fsync with separate volatile and durable layers. Faults:
   lost unsynced writes on crash, torn writes, fsync returning an error, latency spikes.
8. **Raft KV store.** Leader election, log replication, persistent term/vote/log, commit and
   apply, `get`/`put`/`cas` state machine, client retries with request IDs (exactly-once).
9. **Safety invariants checked every step.** Election safety, log matching, leader completeness,
   state-machine safety.
10. **Linearizability checker.** History of invoke / ok / fail / timeout events, WGL search with
    memoisation, partitioned by key.
11. **Event trace + determinism self-check.** Every run produces a trace hash. Running the same
    seed twice and comparing hashes catches accidental nondeterminism.

### Tier 2 — Advanced (what makes it stand out)
12. **CLI (`dst`).** `run --seed`, `replay --seed`, `sweep --seeds 0..100000 --jobs N` (parallel
    over CPU cores with `rayon`), `shrink --seed`.
13. **Planted-bug suite ("mutation testing").** About 6 known Raft bugs behind a runtime flag
    (for example: no term check on vote, committing entries from old terms, not persisting votes,
    stale reads from the leader, not truncating conflicting log, applying before fsync). Report
    the mean seeds-to-detect for each bug.
14. **Swarm testing.** Each seed also randomises *which* faults are enabled and how often
    (FoundationDB / TigerBeetle technique). Finds bugs that uniform fault rates hide.
15. **BUGGIFY points.** Code-level fault hooks inside the system under test (`buggify!()` randomly
    takes a rare path, such as a tiny batch size or an early timeout) during simulation only.
16. **Failure shrinking.** Delta-debugging over the fault schedule: turn a 50,000-event failing
    run into the smallest set of faults that still breaks linearizability.
17. **Trace viewer.** Static HTML/JS page that renders a JSON trace as a space-time (Lamport)
    diagram: nodes as lanes, messages as arrows, faults highlighted, the violating operations
    marked. This is the screenshot for the README.
18. **Disk corruption.** Random bit flips and misdirected writes. The KV store detects them with
    per-record checksums and recovers from peers.
19. **Snapshots and log compaction.** Raft `InstallSnapshot`, which is a classic source of bugs.
20. **Linearizable reads via ReadIndex.** Reads stay correct without writing to the log.
21. **Real-mode runtime.** The same Raft code runs over real TCP and real files (a second
    `Context` implementation). Demo: kill a node mid-write and show it recover with no data loss.
22. **Determinism lint.** `clippy.toml` `disallowed-methods` / `disallowed-types` ban
    `std::time::Instant`, `SystemTime`, `HashMap` with random hasher, `thread::spawn`,
    `rand::thread_rng` inside simulated crates.
23. **CI.** GitHub Actions on Linux and Windows: unit tests, clippy, a sweep of 10,000 seeds per
    push, a larger nightly sweep, and failing seeds uploaded as artifacts.

### Tier 3 — Stretch (only after Tier 2 is polished)
24. Raft membership changes (joint consensus or single-server changes).
25. A second system under test (chain replication or a deliberately broken two-phase commit) to
    prove the framework is generic.
26. State-coverage tracking: hash abstract Raft states and report distinct states explored per
    1,000 seeds; bias fault choices toward unseen states.
27. An async facade: a tiny deterministic `Future` executor so nodes can be written with
    `async/await`.
28. A trace viewer compiled to WASM that replays a seed in the browser.

---

## 3. Architecture

```
                    ┌─────────────────────────────────────────────┐
  dst CLI  ───────► │  Simulator (single thread)                   │
  (run/replay/      │                                              │
   sweep/shrink)    │  seed ─► RNG streams ─┬─► Network model       │
                    │                       ├─► Disk model          │
                    │                       ├─► Clock model         │
                    │                       └─► Workload / clients  │
                    │                                              │
                    │  Event queue: BinaryHeap<(time, seq, Event)>  │
                    │        │                                     │
                    │        ▼                                     │
                    │  ┌────────┐ ┌────────┐ ┌────────┐            │
                    │  │ Node 1 │ │ Node 2 │ │ Node 3 │  … Raft KV  │
                    │  └────────┘ └────────┘ └────────┘            │
                    │        │  (Context: send/timer/disk/now)      │
                    │        ▼                                     │
                    │  Trace recorder ─► invariants (every step)    │
                    │  History recorder ─► linearizability checker  │
                    └─────────────────────────────────────────────┘
                                   │
                                   ▼
                     trace.json ─► HTML trace viewer
```

### Workspace layout (Cargo workspace)

```
dst-sim/
├── Cargo.toml              # workspace
├── clippy.toml             # determinism lint (feature 22)
├── crates/
│   ├── sim-api/            # traits: Node, Context, Storage, message types
│   ├── sim-core/           # RNG, clock, event queue, simulator loop, trace
│   ├── sim-net/            # network fault model
│   ├── sim-disk/           # disk fault model
│   ├── checker/            # linearizability checker + history types
│   ├── raft/               # Raft library (depends only on sim-api)
│   ├── kv/                 # KV state machine + client on top of raft
│   ├── runtime-real/       # real TCP + file Context (feature 21)
│   └── dst-cli/            # `dst` binary
├── viewer/                 # static HTML/JS trace viewer
├── docs/                   # plans, design notes, bug museum
└── .github/workflows/      # CI
```

### Key design decisions

| Decision | Choice | Why |
|---|---|---|
| Concurrency model | **Event-driven actors, not async** | Fully deterministic by construction, far easier to debug, same approach as TigerBeetle's VOPR. Async is a stretch goal. |
| RNG | `rand_chacha::ChaCha8Rng` | Portable and stable across platforms and versions; fast. |
| Collections in sim | `BTreeMap` / `Vec`, never default `HashMap` | `HashMap` iteration order is randomised per process, which breaks determinism. |
| Event ordering | `(virtual_time, insertion_seq)` | Ties are broken deterministically. |
| Raft code | Depends only on `sim-api` traits | The same code runs in simulation and in real mode. |
| Checker | Own WGL implementation (Porcupine algorithm) | Core learning value; keep histories small by partitioning per key. |
| Serialisation | `serde` + `serde_json` for traces; hand-rolled binary + CRC32 for disk records | Traces must be human-readable; disk records need checksums for corruption detection. |

### Determinism rules (enforced by lint and review)
1. No wall-clock time inside the simulation; only `ctx.now()`.
2. No threads or real I/O inside simulated crates.
3. All randomness comes from the seeded RNG streams.
4. No iteration over randomised-hash collections.
5. Every run prints its seed; every failure prints a one-line replay command.

---

## 4. Testing strategy

- **Unit tests** for each model (network drop rates, disk crash semantics, checker on handcrafted
  linearizable and non-linearizable histories).
- **Determinism test**: N random seeds, each run twice, trace hashes must match.
- **Simulation sweeps**: thousands of seeds per CI run with random fault mixes.
- **Mutation testing**: every planted bug must be caught within a bounded number of seeds; CI
  fails if the detector gets weaker.
- **Real-mode crash test**: kill -9 style process kill during writes, then a restart and a
  consistency check.

---

## 5. Metrics to report in the README

| Metric | How measured |
|---|---|
| Simulated time per wall-clock second | Virtual time advanced ÷ real time, for a 5-node cluster |
| Seeds per second (single core and all cores) | `dst sweep` timing |
| Seeds-to-detect per planted bug | Mean / median over the bug suite |
| Shrink ratio | Events in the original failing trace ÷ events after shrinking |
| Simulated cluster-hours tested | Total virtual time across a nightly sweep |
| CPU flamegraph | `cargo flamegraph` on a sweep |

---

## 6. Environment constraints

- **OS:** Windows 11. Nothing in the core needs Linux. CI also runs on Linux.
- **Disk:** only about **4.8 GB free** on C: when planning started. Mitigations:
  - Install Rust with `rustup` using the **minimal profile** and the **GNU toolchain**
    (`x86_64-pc-windows-gnu`), which skips the 2–5 GB Visual Studio Build Tools, then add
    `clippy` and `rustfmt` components only.
  - In the workspace `Cargo.toml`, set `[profile.dev] debug = "line-tables-only"` to shrink
    `target/` substantially.
  - Keep dependencies few: `rand_chacha`, `rand_core`, `serde`, `serde_json`, `clap`, `rayon`,
    `crc32fast`. No `tokio` until the real-mode runtime phase (maybe not even then; std TCP is enough).
  - Run `cargo clean` regularly; save only failing seeds, never full traces of passing runs.
  - Freeing space to about 8–10 GB is recommended before Phase 7.

---

## 7. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Raft bugs eat weeks | Follow Figure 2 of the Raft paper exactly; use MIT 6.5840's "Students' Guide to Raft"; the simulator itself will find the bugs (that is the point). |
| Checker blows up exponentially | Partition histories by key, cap operations per seed, memoise visited states. |
| Scope creep | Hard MVP gate at the end of week 8. No Tier 2 work until Tier 1 passes 10,000 seeds. |
| Placement / exam season | Each phase ends in a working, committed state; the roadmap has buffer weeks. |
| Low disk space | See section 6. |

---

## 8. Resume bullets (real numbers, measured at the end of Phase 8)

- Built a deterministic simulation testing framework in Rust (10 crates, ~9k lines) that runs a
  Raft key-value cluster on one thread **~4,200× faster than real time**; any failure replays
  bit-for-bit from a single 64-bit seed, verified across Linux and Windows in CI.
- Injected partitions, message loss, duplication and reordering, crashes, clock drift and disk
  faults (torn writes, lost and failed `fsync`, crashes mid-`fsync`, bit flips, misplaced writes);
  checked all five Raft safety properties after every step and client histories with a custom
  linearizability checker (WGL search).
- Caught **9 of 11 planted consensus and storage bugs**, the common ones in a mean of **1–26
  seeds**; a delta-debugging shrinker cuts failing fault schedules **3× at the median**, viewed as
  space-time diagrams in a self-contained HTML trace viewer.
- Found and fixed **16 real bugs and test gaps** in my own Raft, storage, simulator and
  checkers, one as rare as 1 in 300,000 seeds; **874 simulated cluster-hours** (500,000 seeds)
  pass in under two minutes on 16 threads, and the same code survives repeated `kill -9` as real
  processes over TCP.

---

## 9. References

- Raft paper (Ongaro & Ousterhout, 2014), especially Figure 2.
- FoundationDB: "Testing Distributed Systems w/ Deterministic Simulation" (Will Wilson, Strange Loop 2014).
- TigerBeetle VOPR docs and blog posts on simulation testing.
- Porcupine linearizability checker (Anish Athalye) and the WGL algorithm paper.
- MIT 6.5840 Distributed Systems labs and "Students' Guide to Raft".
- Jepsen analyses and Kyle Kingsbury's talks.
