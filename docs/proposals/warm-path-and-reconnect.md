# Proposal: warm-path timing and faster reconnect

Status: proposal.

The design targets one network round trip, a few milliseconds, and the command for a warm run. Measure the
actual startup path, remove avoidable work, and reduce the retry delay after an initiator becomes reachable.
No latency saving or overall reconnect deadline is established by this proposal.

## 1. Time the warm path

**Problem.** The host logs how long receiving and applying a sync take, but nothing shows where the initiator's time
goes, so there is no way to tell whether the snapshot, the round trip, or the host dominates.

**Change.** Add structured `debug` events in the client library for:

- total elapsed time from entering the run path to receiving `ATTACHED`;
- opening the host stream, waiting for the snapshot lock, and building the snapshot;
- looking up the base, producing and sending the pack, and receiving `SYNCED`;
- recording the acknowledged base and waiting for `ATTACHED`;
- pack bytes, whether fallback was needed, and checkout/run identifiers for correlation with host logs.

Opening the host stream already overlaps snapshotting in [the client][client], and pack production overlaps
transfer. Report wall-clock total and phase durations separately; adding the phases does not give total latency.
Time until `SYNCED` includes host work and is not a pure network round-trip measurement.

**Collection.** The normal [CLI currently initializes no tracing subscriber][main]. Provide a small developer
example, `crates/ovr/examples/timing.rs`, that installs a DEBUG subscriber on stderr and calls the existing
`ovr::run` entry point. Run it with release optimizations for representative measurements:

```sh
cargo run --release -p ovr --example timing -- HOST -- CMD... 2> timings.log
```

This example is part of the proposed instrumentation; the command becomes available when it is implemented.
Its stderr includes both diagnostics and command stderr. Normal CLI output stays unchanged, and collecting
measurements requires no new runtime flag, configuration file, or environment variable.

**Provides.** A basis for the change below and for judging any future latency work.

**Implications.** Instrumentation and the example need tracing dependencies and the repository's normal build
integration. Use the same optimized measurement setup for comparisons and check instrumentation overhead before
making claims about small savings.

## 2. Trim the snapshot's critical path

**Problem.** A warm run starts five to seven `git` processes in sequence on the initiator (`rev-parse` to locate the
repository, `add -A`, `write-tree`, `commit-tree`, `rev-parse` for the base, `pack-objects` when anything changed,
`update-ref` after `SYNCED`). Their contribution depends on the repository and platform.

**Change.**

- Skip `update-ref` after `SYNCED` when the snapshot equals the base.
- If timing shows base lookup matters, overlap it with snapshot work once both repository location and initiator
  identity are available. Knowing the host name alone is insufficient: the checkout key uses both. Preserve the
  existing overlap between connection opening and snapshotting.

**Provides.** One fewer Git process for an unchanged acknowledged snapshot, and potentially less serial lookup
work. Report measured time to `ATTACHED` for unchanged and changed trees; no fixed millisecond saving is assumed.

**Implications.** No wire or behaviour change.

## 3. Reconnect dial-in hosts sooner

**Problem.** A dial-in host retries an unreachable initiator with backoff up to 10 seconds
([host connection loop][host]). Once a failed attempt ends, this delay can postpone noticing that the initiator
has become reachable. It is only one part of recovery: connection attempts have a 10-second timeout, an existing
connection can take 45 seconds of silence to be declared dead, and the CLI has its own reconnect backoff.

**Change.** Cap the backoff at 2 seconds for `host:port` addresses. Keep 10 seconds for tunnel addresses, where each
attempt restarts the provider's local forwarder.

**Provides.** Direct hosts wait at most 2 seconds between a failed attempt ending and the next attempt starting.
This reduces one avoidable delay; it does not guarantee attachment within 2 seconds of wake or network recovery.

**Implications.** Short failed attempts can produce up to five times as many retries and retry log entries at the
cap. Check idle retry cost and log volume. Keep connection timeouts, heartbeat detection, and the tunnel retry
policy unchanged in this proposal. When implemented, document the retry-delay cap under "Dial-in" in
`docs/DESIGN.md`.

## Validation

- Collect timings for unchanged trees, small edits, a full send, and fallback on a representative repository.
  Report repeated-run median and tail time to `ATTACHED`, along with bytes and the phases that explain changes.
- Verify normal command output and exit-code handling remain unchanged; diagnostics are accessible through the
  proposed example without changing the user-facing command surface.
- Measure recovery to a usable attachment after an initiator restart and a network interruption, distinguishing
  failure detection, connection attempt duration, retry waiting, and attachment.
- Verify direct retry waits cap at 2 seconds, tunnel retry waits retain their 10-second cap, and an unavailable
  initiator does not cause unacceptable background CPU use or log growth.

[client]: https://github.com/nikhil-pandey/ovr/blob/fef66f7d4d4805c31c8836dadc84d366d36616f5/crates/ovr/src/lib.rs
[main]: https://github.com/nikhil-pandey/ovr/blob/fef66f7d4d4805c31c8836dadc84d366d36616f5/crates/ovr/src/main.rs
[host]: https://github.com/nikhil-pandey/ovr/blob/fef66f7d4d4805c31c8836dadc84d366d36616f5/crates/ovrd/src/host.rs
