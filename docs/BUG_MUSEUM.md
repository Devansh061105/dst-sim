# Bug museum

Real bugs the simulator found in this repository's own code: the Raft implementation (including
its membership changes), its storage format, the two-phase commit system, the simulator, and the
checkers. None of them were planted. Each entry says how
the bug showed up, what caused it, how it was fixed, and what now keeps it from coming back.

Seeds name the run that first exposed a bug, on the code as it was then. They pass now, because
the bug is fixed, and they keep passing because the sweeps (10,000 chaos and 2,000 swarm seeds
on every push, 200,000 new swarm seeds every night) run them again. Where a planted bug
reproduces the same class of mistake, it is named: `dst run --bug ...` shows that class failing
today, and `dst shrink` plus `dst run --html` turn it into a small trace.

## Storage and recovery

### A misplaced write looked like a torn write

- **Symptom.** Chaos seed 101: a server lost a committed entry after a crash.
- **Cause.** The log first marked the end of each synced batch with a marker record, so
  recovery could tell a torn, never-acknowledged tail (safe to drop) from damage to synced data
  (not safe). A misplaced write, a span of bytes landing at the wrong offset, covered both the
  last entry and its marker. Recovery saw a clean end of log and silently dropped an entry the
  server had acknowledged.
- **Fix.** The synced log length lives in a separate file, `raft-log-end`, written and synced
  after every append. One corruption damages one file, so the two always disagree when synced
  data is lost.
- **Guard.** Unit tests `a_record_in_the_wrong_place_is_damage` and
  `damage_to_the_log_end_record_loses_nothing` in `raft/src/durable.rs`. Planted bug
  `trust-damaged-log` (trusts a damaged log and keeps voting) is caught in about 1 in 26 chaos
  seeds.

### A damaged snapshot record brought back an older, longer log

- **Symptom.** Swarm seed 1204: a server lost a committed entry.
- **Cause.** Corruption hit the newest snapshot record, so recovery fell back to the previous
  snapshot and its log file. That older log was *longer* than the recorded synced length, so the
  length check passed, but its tail held entries from a term that had since been overwritten.
- **Fix.** The log-end record also stores the term of the last synced entry (and later the
  snapshot index it belongs to). Recovery compares `(index, term)`, not just a length.
- **Guard.** Unit test `falling_back_to_a_stale_log_is_damage`.

### Recovery reported damage that wasn't there, and the cluster stopped electing leaders

- **Symptom.** Swarm seed 81625, a liveness failure: after the fault period ended, the cluster
  went through 299 terms in 50 seconds of quiet network without electing a leader. One run in
  100,000.
- **Cause.** Installing a snapshot that disagreed with the local log lowered the log-end record
  *before* writing the new log. A crash between the two left the old log next to the new log's
  end record. Recovery saw a term mismatch, decided the disk was damaged, and the server became a
  non-voter (correctly, given what it saw). Three of five servers did this, and two voters can't
  form a majority.
- **Fix.** The log-end record is tagged with the snapshot it belongs to. An end record from an
  older snapshot means a switch happened, which is not damage; a newer one means recovery fell
  back past a damaged snapshot record. The "lower it first" step is gone.
- **Guard.** Unit test `a_crash_right_after_a_snapshot_switch_is_not_damage`, and a new harness
  check (next entry).

### Writes after a halt: fsyncgate in our own code

- **Symptom.** 15 seeds in 100,000 swarm seeds, found by a check added because of the previous
  entry: any run where a server claims disk damage that the simulator never caused now fails.
- **Cause.** On a failed `fsync` the node halts, which means the process is gone. But the rest of
  the event handler kept running in memory and kept writing. After a failed snapshot `fsync` (whose
  writes Linux-style semantics drop for good), the handler appended another snapshot record at the
  page-cache offset, after the dropped one, and synced it. On disk: zeros, then a valid record
  nobody could reach, and an end record pointing past them. Recovery called that damage.
- **Fix.** After `halt`, storage calls are discarded: writes are ignored and `sync` fails, in the
  simulator and in the real runtime alike. The `Context::halt` documentation had always promised
  this; the implementation didn't do it.
- **Guard.** The harness false-alarm check runs on every seed.

### Damage right after a snapshot switch went unnoticed

- **Symptom.** Swarm seed 275843: `n4 lost committed entry 13`, one seed in 300,000.
- **Cause.** Right after switching to a new snapshot and log file, the only end record on disk
  described the *old* file, so recovery had nothing to check the new log against. A failed `fsync`
  followed by corruption of the new log's tail lost two acknowledged entries without a trace.
- **Fix.** The snapshot record itself carries the new log's end, `(index, term)`, so there is a
  synced end to compare against from the moment of the switch.
- **Guard.** Unit test `damage_right_after_a_snapshot_switch_is_still_damage`.

### An empty snapshot restored after a failed `fsync`

- **Symptom.** A panic while restoring the key-value store from a snapshot.
- **Cause.** `install_snapshot` updated its in-memory state before the switch was durable. When
  the `fsync` failed, the node halted, but the rest of the event still ran against in-memory state
  that pointed at a snapshot which never reached disk.
- **Fix.** Memory changes only after the snapshot record is synced.
- **Guard.** The sweeps (snapshots happen every 32 entries, and the chaos disk fails `fsync`
  often).

## The Raft protocol

### A million-event resend storm after a follower lost data

- **Symptom.** A run hit the simulator's one-million-event limit.
- **Cause.** A follower repairing a damaged disk had lost entries it had acknowledged before. Its
  rejections pointed below the leader's `match_index` for it, but the leader never lowers a match
  index, so it kept resending from past the end of the follower's log forever.
- **Fix.** On a rejection, the leader lowers `match_index` to just below the follower's hint.
  Lowering a match index can never commit anything, so this is always safe.
- **Guard.** The sweeps with corruption on (each run is capped, so a storm fails as liveness).

### A restarted node handled messages before it had recovered

- **Symptom.** Raft invariant violations right after restarts (Phase 4).
- **Cause.** Restart queued a `Start` event for the new process. Messages already due at the
  same instant were delivered first, to a node that hadn't read its term, vote and log from disk
  yet.
- **Fix.** Restart boots the node synchronously: recovery runs before any other event.
- **Guard.** Every Raft run restarts nodes many times.

## The fault model

### Torn-write recovery was never exercised

- **Symptom.** A 10,000-seed sweep passed, but its statistics said `torn writes: 0`.
- **Cause.** Crashes only happened between events, when Raft had already synced everything it
  wrote, so a crash never caught unsynced data. The torn-tail recovery code had never run.
- **Fix.** A new disk fault: the process dies in the middle of `fsync`, leaving pending writes
  partly applied and torn.
- **Guard.** The chaos tests assert that every kind of fault, torn writes included, actually
  happened.

### Two corruptions of one node, the second one unrecoverable

- **Symptom.** A server lost committed entries although recovery had done the right thing.
- **Cause.** Corruption hit the log; recovery noticed and halted on a failed `fsync` before it
  could record the damage; then a second corruption hit the log-end file. With both copies of
  the evidence gone, no recovery scheme can notice. This asked more of the system than any
  storage design can give.
- **Fix.** The fault budget follows what a replicated system can survive: at most a minority of
  servers are corrupted, each at most once per run, the same approach TigerBeetle's simulator
  takes.

### Disk faults never stopped

- **Symptom.** Part of swarm seed 81625: some seeds pick a disk where about 4% of `fsync` calls
  fail or crash the process.
- **Cause.** The nemesis heals the network and restarts every node after 10 seconds so the
  checks can confirm the cluster recovers. Disk faults kept going, so the "calm" period wasn't
  calm.
- **Fix.** The calm period also makes disks reliable.

## The checkers

### Leader Completeness was checked too strictly

- **Symptom.** Swarm seed 11644, about 1 in 14,000 seeds: "leader completeness" violation.
- **Cause.** A server cut off from the cluster won an *older* term, through delayed votes, after
  a newer term had committed an entry it lacked. Raft allows that: the property is about entries
  committed in *earlier* terms than the leader's. The checker, not Raft, was wrong.
- **Fix.** The checker records the term in which each entry was first seen committed, and only
  holds leaders of later terms to it.
- **Guard.** `stale_leader_of_older_term_is_not_a_violation` replays the case (now swarm seed
  9588, after a change to the random streams) and
  `stale_leader_of_older_term_may_lack_newer_commits` tests it directly.

### A repairing node's stale log confused the checker

- **Symptom.** Invariant violations blamed on a server that was correctly repairing a damaged
  disk.
- **Cause.** The checker forgave a repairing server for being *shorter* than before, but after
  falling back to an older log it can also *disagree* with the committed log.
- **Fix.** For a repairing server the checker walks back to the longest prefix that still agrees
  with the committed log.

## Membership changes

### A server just added couldn't vote for the real leader, and no one was ever elected

- **Symptom.** Membership runs, about 1 in 1,000 chaos seeds: liveness failures with terms in
  the hundreds, elections failing for the whole calm period.
- **Cause.** The first version ignored vote requests from servers outside the receiver's own
  configuration, so a removed server couldn't disrupt the cluster. But the receiver's view can be
  the stale one: a server just added, with an empty log, still believes the *initial*
  configuration. It refused the leader-to-be, which needed its vote under the newest
  configuration; another member was repairing a damaged disk and couldn't vote either.
- **Fix.** Ignore outsiders' vote requests only while a leader is known to be alive (heard from
  within the minimum election timeout), as in the Raft thesis (§4.2.3). That still protects a
  healthy cluster, and lets everyone vote when there is no leader.
- **Guard.** 300,000 membership runs (calm, chaos, swarm) pass.

### A server holding the newest entries refused to campaign

- **Symptom.** Chaos seed 1193 (membership): no leader for 50 seconds.
- **Cause.** Servers outside their own latest configuration never campaigned. Here the server
  with the most up-to-date log had an uncommitted configuration that removed it; the others'
  shorter logs failed its "at least as up to date" check, and one was repairing its disk. Nobody
  could win.
- **Fix.** A server outside its latest configuration still campaigns while that configuration is
  uncommitted (it may hold entries the others need, thesis §4.2.2), counting members' votes but
  not its own; once the change commits it steps down and stays out.

### Removals shrank the cluster to two servers

- **Symptom.** Swarm seed 522 (membership): no leader. Two members, one of them repairing a damaged
  disk, so no majority.
- **Cause.** The admin chose removals from its own, sometimes stale, view of the membership; its
  "at least three members" rule didn't hold.
- **Fix.** The server checks the rule against its own configuration when it receives the
  request.

### Stale hints sent the admin and clients in circles

- **Symptom.** 14 of 2,000 *calm* membership runs: an admin (and, in other seeds, a client) never
  finished although the cluster had a healthy leader.
- **Cause.** Removed servers keep stale views. One said "the leader is n1"; n1, also removed,
  said "I don't know" and the next server tried was the first one again, forever.
- **Fix.** Follow a leader hint at most a few times, then try servers in turn. For clients this
  is enabled only in clusters whose membership changes: runs without membership changes can meet
  a hint cycle too (chaos seed 93 loses about 3 seconds to one), but always escape it, and
  leaving them exactly as they were keeps every existing seed reproducible.

## Two-phase commit

### A restart during redelivery forgot an older decision

- **Symptom.** Chaos seed 104: liveness failure; one participant never learned the outcome of
  transaction 13.
- **Cause.** The coordinator's recovery resent only its *last* decision, assuming earlier ones had
  all been acknowledged. It restarted while still delivering transaction 13 (one participant was
  partitioned); the presumed abort it logged for the next transaction made 13 no longer last, and
  the next restart forgot it.
- **Fix.** Once every participant acknowledges a decision, the coordinator logs a *done* record
  (unsynced; losing it only means resending). Recovery resends every decision without one.
- **Guard.** `seeds_that_found_real_bugs_pass` in `crates/twopc/tests/sweep.rs`.

### An unsynced truncate brought back a commit nobody decided

- **Symptom.** 7 of 200,000 seeds, all "validity: transaction committed without a yes vote";
  swarm seed 2571 fails at 0.68 s.
- **Cause.** The coordinator wrote a commit decision together with an unsynced *done* record and
  crashed in the middle of the `fsync`: the decision reached the disk, the record before it didn't,
  leaving a gap. Recovery cut the log at the gap but didn't sync the cut before appending a
  presumed-abort record; the next crash kept the append and lost the cut. The new record was the
  same size as the lost one, filled the gap exactly, and made the stale commit readable again, at
  the position of the next transaction. The same class of mistake as the planted Raft bug
  `single-sync-truncate`.
- **Fix.** Recovery syncs the cut on its own before anything is appended.
- **Guard.** `seeds_that_found_real_bugs_pass` in `crates/twopc/tests/sweep.rs`.

## Smaller ones

- **Read confirmation panicked on a follower.** `confirmed_round` looked up per-peer state that
  only a leader has. Fixed to treat missing state as round 0.
- **A real-mode timer deadline overflowed.** `run_until` with no limit computed
  `Instant::now() + Duration::MAX`, which panics. Fixed with `checked_add`.
- **A planted bug crashed the harness instead of failing a check.** With `commit-beyond-match`
  the commit index can pass the end of the log, and answering reads indexed past it. The read
  path now checks the entry exists, so the invariant checker reports the bug instead.
