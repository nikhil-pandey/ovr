# Proposal: recover the delta base instead of resending the tree

Status: proposal.

## Problem

A warm run sends a thin pack against `base`, the snapshot the initiator last saw the host confirm. Two paths cause
an unnecessary full-tree transfer when a usable common snapshot still exists:

- **Base missing on the host.** The agent answers `WELCOME{need_full}` whenever it lacks the exact `base`
  ([agent session][agent-session]), even though the checkout may hold another useful snapshot recorded in
  `.git/ovr-applied`.
- **Failed sync.** An error handled before `SYNCED` calls `Delivery::forget` ([client session][client-session]),
  deleting `refs/ovr/hosts/<host>/<checkout>`. The next run sends everything. A failed concurrent delivery can also
  delete the base another run just recorded successfully.

On a large repository over a slow link, resending the full tree can dominate startup. The benefit depends on the
available common snapshot and the size of the delta; it needs to be measured in bytes and time saved.

## Change

### Preserve the confirmed base

Remove `Delivery::forget` from the failed-sync path. A failure leaves the host's state uncertain: it may have
applied the snapshot before the acknowledgement was lost. Keep the last confirmed base as a guess, and let the
host validate it on the next attempt. An error must not delete another delivery's successful acknowledgement.

This part works with the existing `WELCOME{need_full}` fallback and needs no protocol change. It can ship with
the recovery exchange below or independently.

### Wire

`WELCOME` names what the host has instead of only saying it lacks the base:

```rust
pub enum Welcome {
    /// The base is present; the speculative thin pack will be applied.
    Ok,
    /// The base is missing. The agent discards everything up to the first `EXEC`; the client follows it with a
    /// pack against `have`, or a full pack, and a second `EXEC`.
    Rebase { have: Option<String> },
}
```

`PROTOCOL_VERSION` is bumped.

### Host

When `base` is missing, the agent reads `.git/ovr-applied` and answers `Rebase { have }` with that commit if
`has_commit` confirms it. Missing or invalid applied state produces `Rebase { have: None }`. The speculative pack
and its `EXEC` are discarded as today. A request without a base continues to send a full pack immediately.

### Initiator

- On `Rebase { have: Some(c) }`, validate that `c` is an object ID and that `git cat-file -e c^{commit}` succeeds
  locally, then send a thin pack against it. Otherwise send a full pack. If `c` equals the requested snapshot,
  send the empty end-of-pack frame and `EXEC` without building a pack.
- On `SYNCED`, record the snapshot as the host's base, as today.
- Preserve the current explicit cleanup behaviour: `ovr clean HOST` forgets the host's bases in the current
  repository, including when some checkouts were kept because they were in use.

## What it provides

- Failed deliveries no longer discard a potentially usable confirmed base.
- A stale base costs one additional exchange and a replacement pack against the host's advertised snapshot when
  both sides still have it. The pack may be small or empty; otherwise the full-tree fallback remains.
- Successful speculative sends keep their existing round-trip budget.

## Implications

- The local `git gc` may already have pruned the host's `have` snapshot, since only the latest delivered snapshot
  per host is kept by a ref. That case falls back to a full pack, which is today's behaviour.
- Changing `WELCOME` requires a protocol bump: incompatible dial-in hosts are refused until upgraded, while SSH
  hosts get the new agent automatically. Preserving the base alone has no wire compatibility cost.
- When implemented, update "Speculative send", the wire sequence, and the cleanup fallback in `docs/DESIGN.md`.

## Validation

- A host missing the base but holding a snapshot available locally receives a pack against that snapshot.
- A sync that fails mid-pack leaves the base ref in place; the next run uses it if the host still has it.
- A lost `SYNCED` after application recovers whether the original base is still present or has been collected.
- A failed concurrent delivery preserves the base recorded by a successful delivery.
- Missing host state or a locally pruned advertised snapshot falls back to a full pack and completes the run.
- Compare transferred bytes and time to `ATTACHED` before and after an interrupted sync on a representative repo.

[client-session]: https://github.com/nikhil-pandey/ovr/blob/fef66f7d4d4805c31c8836dadc84d366d36616f5/crates/ovr/src/session.rs
[agent-session]: https://github.com/nikhil-pandey/ovr/blob/fef66f7d4d4805c31c8836dadc84d366d36616f5/crates/ovrd/src/agent/session.rs
