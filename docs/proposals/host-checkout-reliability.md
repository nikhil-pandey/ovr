# Proposal: host checkout reliability

Status: proposal.

Propose bounded retries for index-lock contention and snapshot metadata for diagnostics. GC contention and
submodule overhead need evidence before changing their behaviour. The proposed changes stay on the host and
require no wire change.

## 1. Retry index-lock contention without removing another writer's lock

**Problem.** A Git process killed with `SIGKILL` can leave `.git/index.lock` behind, preventing later resets until
the lock or checkout is recovered. A live Git command can also hold the lock briefly while a run applies. Lock
age alone cannot distinguish the two: `.git/ovr.lock` coordinates `ovr` applications, not a command's own Git
processes. Git uses the [lockfile as both mutual exclusion and the pending replacement file][git-locks].

**Change.** In `Checkout::apply`, while holding `.git/ovr.lock`:

1. If `reset --hard` fails to acquire `.git/index.lock`, retry with up to 2 seconds of waiting at 100 ms intervals.
   Other Git failures retain their original error.
2. If contention persists, return the Git error with guidance to wait for the active Git operation or confirm that
   no writer remains before recovering the lock. Never unlink the lock automatically.

**Provides.** Brief contention can resolve without failing the run, while active writers retain their locks.

**Implications.** An abandoned lock still needs manual recovery. A contended sync can delay other applications of
the same checkout by the retry interval. Automatic stale-lock recovery is deferred until ownership and liveness
can be established reliably; a longer timeout alone does not provide that evidence.

## 2. Investigate ref-lock contention during GC

**Hypothesis.** After a command exits, its [keeper][keeper] runs `git gc --auto` without `ovr.lock`. Ref packing and
a concurrent reset may contend for ref locks, but this proposal has no reproducer establishing that cause of a
failed `ovr` sync. Capture the failing operation and ref before attributing a generic "cannot lock ref" error to GC.

**Candidate change.** If the collision is reproduced, have `Checkout::collect_garbage` run
`git -c gc.packRefs=false gc --auto --quiet`. This removes the ref-packing step while preserving background object
maintenance and the existing pruning policy. Keep GC off the apply lock so lengthy repacks do not block syncs.

**Expected value.** Prevent the demonstrated ref-packing collision, if confirmed. A checkout has few refs, so
packing them offers little benefit. This is not a claim that every possible concurrent GC failure is eliminated.

**Decision.** Leave GC behaviour unchanged until there is evidence for this mitigation. The general concurrency
caveats in [Git's GC documentation][git-gc] do not by themselves establish an `ovr` defect or require a GC redesign.

## 3. Measure submodule overhead before changing synchronization

**Current behaviour.** A repository with `.gitmodules` runs `git submodule update --init --recursive` while
holding `ovr.lock` in [checkout application][checkout]. Git [fetches missing commits][git-submodule]; an unchanged,
initialized submodule need not make a network call. Process startup and recursive traversal still cost time,
and an update that needs a fetch holds the apply lock for longer.

**Decision.** Keep the existing update call, lock boundary, and warning on update failure. Measure unchanged
submodules separately from missing commits and first initialization before proposing an optimization. A
`submodule status --recursive` precheck also traverses submodules and is not assumed to be cheaper.

**Deferred tradeoff.** Moving updates outside the apply lock lets parent snapshots advance during a submodule
update. Shared changing checkouts are intentional in `ovr`, so this is not automatically a contract violation.
Any future change must define when a new run may start and how updates converge on the latest parent snapshot.
A separate submodule lock alone does not coordinate updates with parent resets.

## 4. Record the snapshot selected during sync

**Problem.** `docs/DESIGN.md` says a run directory holds the run's snapshot, but `Meta`
([run records][runs]) records none. A run that arrives late, with a lower `seq`, selects the newer snapshot
already applied, so recording only the requested snapshot would misdescribe its sync.

**Change.** `Checkout::apply` returns the selected commit alongside any submodule warning. `Meta` gains
`synced_snapshot: Option<String>` with that commit, defaulting to `None` when reading older run directories.
Include it with the run ID in host diagnostics when recording the run.

**Provides.** The stored record and host diagnostics identify the snapshot selected for a run's sync. This helps
correlate runs with delivery and checkout activity without extending the wire.

**Meaning.** The checkout can change after the apply lock is released, even before the keeper starts the command:

```text
apply A -> release lock -> apply B -> start A's command
```

The field is diagnostic context, not a guarantee of every file the command observed. It does not retain Git
objects for replay. `ovr ps` and `ovr logs` remain as they are; displaying the field there would require separate
protocol and presentation work.

**Implications.** Older run directories remain readable. When implemented, update "How the remote applies it"
and "Run directory" in `docs/DESIGN.md` with the retry policy and this field's precise meaning.

## Validation

- A lock released within the retry interval allows the sync to complete. Persistent and abandoned locks remain
  untouched, produce a useful error, and do not cause unrelated Git errors to be retried.
- A late snapshot records the newer commit selected by `apply`. Older metadata without the field still loads.
- Reproduce ref-packing contention before accepting the GC change; verify the specific failure disappears.
- Measure warm submodule update time and apply-lock waiting before changing the submodule path.

[checkout]: https://github.com/nikhil-pandey/ovr/blob/fef66f7d4d4805c31c8836dadc84d366d36616f5/crates/ovrd/src/agent/checkout.rs
[keeper]: https://github.com/nikhil-pandey/ovr/blob/fef66f7d4d4805c31c8836dadc84d366d36616f5/crates/ovrd/src/agent/keeper.rs
[runs]: https://github.com/nikhil-pandey/ovr/blob/fef66f7d4d4805c31c8836dadc84d366d36616f5/crates/ovrd/src/agent/runs.rs
[git-locks]: https://git-scm.com/docs/api-lockfile
[git-gc]: https://git-scm.com/docs/git-gc
[git-submodule]: https://git-scm.com/docs/git-submodule
