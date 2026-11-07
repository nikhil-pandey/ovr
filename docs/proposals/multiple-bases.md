# Proposal: several delta bases per host

Status: proposal; measure the benefit before implementation. Builds on
[base recovery](../DESIGN.md#how-a-run-is-sequenced).

## Problem

The initiator [keeps one base per host and checkout][snapshot]: the last snapshot delivered. Switching branches
`A -> B -> A` can resend objects from A on the return, even though the host received them one run earlier. The
same happens when stashing and unstashing, or alternating between working tree states while debugging.

Single-base thin packs already make small edits cheap. Multiple bases are most useful when returning to retained
states avoids substantial transfers. Compare bytes, pack-building time, and time to `ATTACHED` on a representative
branch-switching workload before accepting the additional retained state and protocol complexity.

## Change

### Initiator

- Keep the last `N = 4` distinct snapshots confirmed per host and checkout, ordered by acknowledgement recency.
  Reconfirming an existing snapshot moves it to the front without consuming another slot. Keep `N` a fixed default.
- Keep the existing latest-base ref at `refs/ovr/hosts/<host>/<checkout>`. Store up to three older distinct bases
  under the separate namespace `refs/ovr/bases/<host>/<checkout>/<slot>`, with slots `0`, `1`, and `2` in recency
  order. Existing installations start with their current base and accumulate history on subsequent deliveries.
- Do not create child refs beneath the existing latest-base ref: Git cannot store both `<checkout>` and
  `<checkout>/0`. The separate namespace avoids that migration conflict.
- Read and update the retained set under a short metadata lock per host and checkout. On `SYNCED`, reread the set
  and update the latest ref and history slots together with a Git ref transaction, so concurrent acknowledgements
  cannot overwrite each other's rotation. Release the lock before packing or network I/O.
- Build the pack from every base: `pack-objects --thin --shallow` with input `<snapshot>\n^<b0>\n^<b1>\n...`.
  Objects reachable from any base are left out, and deltas may use any of them.
- A snapshot equal to any base sends no pack, subject to the host accepting the advertised bases.

### Wire

`HELLO.base: Option<String>` becomes `HELLO.bases: Vec<String>`, containing at most four distinct IDs.
`PROTOCOL_VERSION` is bumped.

### Host

- Keep the last `N` distinct successfully synced snapshots reachable under `refs/ovr/kept/`, updating them under
  the apply lock before sending `SYNCED`. Retain the received snapshot even when its lower `seq` means a newer
  snapshot stays checked out. Repeated snapshots refresh recency without taking another slot.
- Retained refs protect their objects from normal garbage collection. Eviction must also preserve any bases
  being used by an admitted transfer until its pack is indexed or the transfer ends.
- The base check passes only when every listed base is present; otherwise the agent answers as in
  [base recovery](../DESIGN.md#how-a-run-is-sequenced), naming its newest applied snapshot.

The two retained sets need not match: acknowledgements can be lost or observed in a different order. A mismatch
causes recovery or a full send, rather than accepting a speculative pack whose bases are missing. Checkout
selection still follows `seq`, independently of base recency.

## What it provides

- Returning to an accepted retained snapshot sends no pack. Other runs omit objects reachable from any accepted
  base, which can reduce transfer size when switching between substantially different trees.
- Repeated unchanged runs do not evict other useful bases. Build caches still share the same changing checkout.

## Implications

- Both sides retain objects for up to four distinct base snapshots per host and checkout. The host also retains
  its current checkout and any bases needed by in-flight transfers; four snapshots is not a disk-size limit.
- Packing considers more excluded trees, and acknowledging a sync updates more refs. Retain the single-base
  design if the saved transfer time does not justify that work on representative workloads.
- `ovr clean HOST` forgets the host's refs in both initiator namespaces; host cleanup removes the checkout's refs.
- Incompatible dial-in hosts need upgrading. When implemented, update "What is sent", "Speculative send",
  cleanup, and the `HELLO` fields in `docs/DESIGN.md`.

## Validation

- `A -> B -> A` sends no pack on the third run when A is retained on both sides.
- Repeating B more than four times still retains A until four distinct newer states have displaced it.
- Existing single-base refs seed the new layout without a ref-name conflict or an unnecessary cold send.
- Concurrent acknowledgements leave a bounded, distinct set; lost acknowledgements and late arrivals recover.
- Missing kept refs trigger recovery, and retention changes do not invalidate a pack already being received.
- Compare the current design with four distinct bases for small edits, substantial branch changes, and unchanged
  runs. Record transferred bytes, pack time, time to `ATTACHED`, and retained disk usage; no latency saving is
  assumed in advance.

[snapshot]: https://github.com/nikhil-pandey/ovr/blob/fef66f7d4d4805c31c8836dadc84d366d36616f5/crates/ovr/src/snapshot.rs
