# ovr: run your builds over there

Status: design proposal.

```sh
ovr bigbox cargo test -p engine
ovr arm64 bazel test //services/...
ovr winbox dotnet build -c Release
ovr bigbox                    # interactive shell in the synced checkout
```

`ovr HOST CMD...` runs `CMD` on `HOST` against your **local, uncommitted** working tree, streams the output back as
if it ran locally, and returns the remote exit code. `HOST` is either something `ssh HOST` understands or a machine
that has dialed in to your laptop. There is nothing to configure.

## Problem

Large Bazel, Cargo, and .NET builds saturate a laptop. Most developers have a bigger machine somewhere (cloud VM,
dev box, Linux box under the desk, Windows workstation), but the only easy way to use it is to move the whole
workflow onto it. `ovr` keeps editing local and sends the heavy work elsewhere with one command.

Constraints that shape the design:

- **Hosts are reached one of two ways.** Over SSH when the host accepts it, or by the host dialing **out** to your
  laptop when it does not. Windows workstations, machines behind NAT, and locked-down boxes are the second kind.
  Overlay networks such as Tailscale make the laptop reachable from anywhere; `ovr` does not care which.
- **Bandwidth may be limited.** Sync sends deltas, never whole trees.
- **Mixed platforms.** macOS, Linux, and Windows on arm64 and x86-64, on either side. You pick a host whose OS and
  architecture match your target; `ovr` does not pick for you.

## Principles

- **No configuration.** No project file, no user file, no environment variables read by `ovr`. The only state is
  what `ssh` already has and the pairing records created by `ovrd pair`.
- **No environment forwarding.** The remote command sees the remote user's login environment, nothing from the
  laptop. Credentials never leave the machine by accident.
- **Git decides what is sent.** Tracked and untracked files are sent; `.gitignore`d files are never sent. Build
  outputs, `node_modules/`, `.venv/`, secrets in ignored files: none of it leaves the laptop.
- **No checks.** No doctor, no size limits, no policy. If the command fails remotely, you see the same error you
  would see locally.
- **Builds stay warm.** Each host keeps a persistent checkout per repository, so `target/`, `bazel-out`, `bin/`, and
  `obj/` survive between runs and incremental builds stay incremental.

## Non-goals

- Distributing one build across machines (Bazel remote execution does that).
- Cross-OS builds; pick a host whose OS matches.
- Host selection, pools, load balancing. You name the host.
- Two-way sync or remote editing. Use your editor's remote mode for that.
- Copying build outputs back. Run the thing remotely, or `scp` what you need.
- Provisioning machines, SSH keys, or network connectivity.

## Two binaries

| Binary | Where it runs | Job |
|---|---|---|
| `ovr` | laptop | The CLI. Snapshots the tree and hands the run to the local `ovrd` over a Unix socket (named pipe on Windows). Starts `ovrd` on demand if it is not running. |
| `ovrd` | laptop **and** every host | The server. On the laptop it owns connections to hosts and runs jobs through them. On a host it is the agent: spawned per run over SSH, or kept running and dialing out to the laptop. |

The CLI knows nothing about transports. The server knows nothing about Git working trees. Hosts of both kinds look
identical above the transport layer.

## Command surface

```
ovr HOST [CMD...]           run CMD on HOST (no CMD: login shell in the synced checkout)
ovr hosts                   list dialed-in hosts and their OS/arch

ovrd                        the laptop server; started automatically by ovr, or by launchd/systemd --user
ovrd pair                   print a one-shot command to run on a host that must dial in
ovrd connect ADDR CODE      on a host: pair with the laptop at ADDR and keep serving it (printed by `pair`)
```

- `HOST` is a dialed-in host's name (its hostname, as shown by `ovr hosts`) or an SSH destination: an alias from
  `~/.ssh/config`, `user@hostname`, or a hostname. Dialed-in names win if both match.
- `CMD...` is passed through verbatim, including flags (`ovr box cargo build --release`); nothing after `HOST` is
  parsed by `ovr`.
- The remote command runs in the same directory relative to the repository root as your current directory, so
  `cd crates/engine && ovr box cargo test` does what you expect.
- The remote command's exit code is returned unchanged. Failures in `ovr` itself exit with `125`, following the
  `docker run` convention.

## Architecture

```
 Laptop                                                   Hosts
 ┌──────────────────────────────────────────────┐
 │ ovr HOST cargo test                          │         ┌─────────────────────────────────┐
 │   snapshot tree ──► thin pack                │  ssh    │ linux-box                       │
 │   │ unix socket                              │ ───────►│   ssh HOST ovrd agent           │
 │   ▼                                          │         │   ~/ovr/<repo>/ (warm target/)  │
 │ ovrd                                         │         └─────────────────────────────────┘
 │   ├─ ssh hosts: spawn ssh, upload agent      │
 │   └─ dialed-in hosts: TLS listener :7420     │  TLS    ┌─────────────────────────────────┐
 │        paired keys, hostname ─► connection   │ ◄─dials─│ winbox                          │
 │                                              │   out   │   ovrd connect <laptop> CODE    │
 │ one agent protocol over either stream        │         │   C:\ovr\<repo>\ (warm obj/)    │
 └──────────────────────────────────────────────┘         └─────────────────────────────────┘
```

Every run is **one stream** to the host's agent: handshake, sync, execution, and terminal I/O are multiplexed over
it. For SSH hosts the stream is a session that `ovrd` keeps alive between runs; for dialed-in hosts it is a
channel over the host's long-lived TLS connection. Above that the two are indistinguishable.

## Transports

### SSH

`ovrd` runs `ssh HOST ~/.cache/ovr/bin/ovrd-<version> agent`, inheriting everything in `~/.ssh/config`:
jump hosts, `ProxyCommand`, keys, certificates, Tailscale SSH. If the remote command exits without a `HELLO`
(binary missing or wrong version), `ovrd` uploads the matching `ovrd` for the remote OS and architecture over
SFTP and retries. Nothing is installed by hand, and version skew is impossible: both ends are always the same build.

`ovrd` keeps one SSH master connection per host open for ten minutes after the last run (`ControlMaster` on Unix;
on a Windows laptop, where OpenSSH has no multiplexing, each run pays one connection).

### Dial-in

For hosts that cannot accept SSH, including every Windows workstation:

```
 laptop$ ovrd pair
 run this on the host within 10 minutes:
   ovrd connect 100.64.0.10:7420 7KQX-M4RT

 winbox> ovrd connect 100.64.0.10:7420 7KQX-M4RT
 paired with laptop; serving as winbox
```

- `ovrd` on the laptop listens on `:7420`. The host runs `ovrd connect ADDR CODE` once; the two exchange
  Ed25519 identities, after which every connection is TLS 1.3 with **both keys pinned**. Codes are single-use,
  expire, and wrong guesses are rate-limited. Pairing state lives in `~/.local/state/ovr/` and
  `%LOCALAPPDATA%\ovr\`; it is a record, like `known_hosts`, not configuration.
- After pairing, `ovrd connect` keeps serving: it reconnects with backoff when the laptop sleeps or moves
  networks, and only ever accepts jobs from the laptop it paired with. It runs as the logged-in user (Scheduled
  Task at logon on Windows, `launchd`/`systemd --user` elsewhere), never as SYSTEM or root, and only inside
  `~/ovr` / `C:\ovr`.
- The host is addressable by its hostname: `ovr winbox dotnet test`. `ovr hosts` lists what is connected.
- Reachability is the network's job. On Tailscale, `100.x.y.z` is the laptop's stable address and the WireGuard
  layer already authenticates peers; the pinned keys are defense in depth and also make the design work over any
  other overlay or a plain LAN.
- Dial-in works for any host, not just Windows. A Linux box behind NAT with no SSH server runs the same command.

Nothing about the laptop-side `ovrd` is specific to the laptop; the same server can run on an always-on VM later
(see Milestones). It runs on the laptop first because that needs no additional machine.

## Efficiency

The budget for a warm run is: **one network round trip + a few milliseconds + the command itself**. Nothing else is
allowed on the critical path. Every decision below exists to hit that.

### What is sent

- **Trees, not history.** The remote never needs your Git history; it needs the files. A snapshot is a
  **parentless commit** over the working tree, so the first sync sends exactly the working tree (compressed, ~size of
  a source tarball), never the repository's history. A ten-year monorepo costs the same as a fresh one.
- **Thin packs, not bundles.** New objects go out as `git pack-objects --thin --stdout --revs` with input
  `<snapshot>\n^<previous snapshot>`. Thin packs may delta against objects the receiver already has, so editing one
  line of a 1 MB file sends a few bytes of delta, not a recompressed blob. The remote runs
  `git index-pack --stdin --fix-thin`, which writes a pack directly; no loose objects, no `git fetch`, no refs
  negotiation.
- **Nothing when nothing changed.** If the snapshot tree equals the tree last sent to this host, no pack is built
  and zero bytes of sync cross the wire; the exec request goes immediately.
- **Already compressed.** Pack data is zlib-compressed by Git, so SSH compression is disabled and the TLS transport
  does no compression either.
- **`.gitignore` is the filter.** `git add -A` skips ignored files, so `target/`, `bazel-*`, `bin/`, `obj/`,
  `node_modules/` never get hashed, packed, or sent.
- **No pre-packing.** There is nothing to prepare in the background. On a warm run the only new objects are the
  blobs you just edited, and `pack-objects --thin` reuses the deltas and compressed blobs already in your local
  packs for everything else; `git gc --auto` has effectively pre-packed the repository for us. The one large
  transfer, the first sync to a host, is bounded by the working tree itself and cannot be shrunk without seeding
  from `origin`, which would reintroduce remote credentials and configuration. It happens once per host and is
  accepted.

### How the snapshot is built

1. `ovr` keeps its **own index** at `.git/ovr/index`, seeded by copying `.git/index` on first use and kept
   between runs. Your real index is never read again and never written.
2. With `GIT_INDEX_FILE` pointing at it and `core.untrackedCache=true`, run `git add -A`. The index carries stat
   data and the untracked-directory cache from the previous run, so an unchanged file costs one `lstat` and an
   unchanged directory costs one `stat`. Only files whose stat changed are rehashed. Where Git's builtin fsmonitor
   is available (macOS, Windows) and enabled in the repository, `git add` uses it and the walk disappears entirely.
3. `git write-tree`, then `git commit-tree <tree>` with no parent. Nothing touches your branches, `HEAD`, stash, or
   reflog; snapshots are reachable only from `refs/ovr/`, and all but the latest per host are collected by your
   normal `git gc`.

### How a run is sequenced

```
 t0  ovr: connect to ovrd socket; ovrd: open/reuse host stream ────────┐  (overlaps the snapshot)
 t0  ovr: snapshot: add -A, write-tree, commit-tree                    │
 t1  HELLO{repo, version, base, snapshot} ────────────────────────────►┼─► agent: HELLO{ok | need_full}
 t1  PACK frames (speculative, vs base) ──────────────────────────────►┼─► agent: index-pack as frames arrive
 t2  EXEC{argv, cwd, cols, rows} ─────────────────────────────────────►┼─► agent: read-tree, lock, spawn
     ◄── OUT frames ... ◄── EXIT{code} ────────────────────────────────┘
```

- **Speculative send.** `ovr` records, per host, the last snapshot it delivered as a ref, `refs/ovr/hosts/<host>`,
  so your local `git gc` cannot prune the delta base out from under the next run (losing it would silently degrade
  to a full-tree send). It assumes the host still has it and streams the thin pack immediately after `HELLO`
  without waiting for the reply. The agent answers `HELLO` as soon as it reads it; on `need_full` (checkout
  deleted, first contact) the client aborts the stream and sends a pack with no base. The common case pays zero
  extra round trips; the rare case pays one.
- **Pipelined.** `EXEC` follows the pack on the same stream, so the agent starts the command the moment checkout
  finishes without another exchange.
- **Connections outlive runs.** SSH masters stay up for ten minutes; dial-in connections stay up indefinitely. A
  warm run never pays a TCP or TLS handshake, let alone a jump host.
- **Overlapped.** Opening the host stream and building the snapshot run concurrently.

### How the remote applies it

1. `git index-pack --stdin --fix-thin` streams the pack straight into `.git/objects/pack/`.
2. `git read-tree -m -u <previous> <snapshot>` is a two-tree merge: it touches only paths that differ between the
   two snapshots, so a one-file edit rewrites one file and its mtime, and everything else keeps its timestamps.
   Cargo, Bazel, and MSBuild see exactly the changed inputs. Deleted files are removed; `.gitignore`d outputs are
   never touched because they are not in either tree.
3. `refs/ovr/head` is updated to the snapshot; the previous one becomes unreachable. After `EXIT` is sent, the
   agent runs `git gc --auto` in the background so pack accumulation never lands on a run's critical path.

The remote never runs `git clean`. The two-tree update already removes files that left the snapshot, and anything
the build creates that is not ignored stays, exactly as it would locally.

### The wire

- Frames are `type:u8, len:u32le, payload`. Control messages (`HELLO`, `EXEC`, `RESIZE`, `EXIT`) are small JSON and
  occur a handful of times per run. Data frames (`PACK`, `OUT`, `IN`) carry raw bytes, up to 64 KiB each, and are
  what the stream is made of.
- The same frames flow `ovr → ovrd` over the local socket and `ovrd → agent` over SSH or TLS; `ovrd`
  forwards them without parsing the data frames.
- PTY output is read in large chunks and forwarded as-is; no line buffering, no re-encoding. Build output
  throughput is bounded by the transport, which comfortably exceeds any terminal.
- Backpressure is the stream's: the agent stops reading the PTY when the client is slow, so `yes | head` behaves.

### Checkout location

The checkout lives at `~/ovr/<repo>` on Unix and `C:\ovr\<repo>` on Windows, where `<repo>` is the basename of
the local repository root. The path is stable and short, so incremental state (`target/`, Bazel's output base,
`obj/`) and path-keyed caches such as sccache stay valid across runs. Submodules and Git LFS content are fetched by
the remote from `origin` with the remote user's credentials.

## Agent

The agent is `ovrd` on the host, whether spawned per run over SSH or resident via `ovrd connect`.

- **Exec:** argument vector, working directory relative to the checkout, terminal size. No shell string, so no
  quoting problems on any OS. Environment is the host user's login environment plus `TERM`.
- **Terminal:** the agent creates the pseudo-terminal (`openpty` on Unix, ConPTY on Windows) and streams output.
  Input, resize, and interrupt frames flow the other way. Colors, progress bars, and interactive prompts work.
- **Lifetime:** the command runs in its own process group on Unix and in a Job Object with kill-on-close on Windows.
  Ctrl-C sends `SIGINT` or `CTRL_BREAK`, then kills the group after a grace period. If the stream drops, the run
  is killed and the whole process tree goes with it.
- **Concurrency:** a per-checkout file lock serialises runs against the same checkout on the same host.

## Windows

Windows works as a host (via dial-in) and as the laptop.

- **Host:** `ovrd connect` installed as a Scheduled Task at logon. Job Objects, ConPTY, `C:\ovr\<repo>`,
  `core.longpaths=true` set on the checkout. `cargo`, `bazel`, and `dotnet` locate their toolchains themselves;
  `ovr` runs no `vcvars` or setup scripts.
- **Laptop:** `ovrd` listens the same way and the CLI talks to it over a named pipe. The snapshot applies
  `.gitattributes` normalisation, so a Linux host receives the same content a real commit would. The console uses
  raw mode and VT output, and a control handler turns Ctrl-C into an interrupt frame instead of killing `ovr`.

## Build caches

`ovr` does not configure caching, but stable checkout paths and persistent ignored directories make the standard
tools effective: Cargo's `target/`, Bazel's output base, and MSBuild's `obj/` all stay warm on the host. Shared
caches such as sccache or bazel-remote are a per-host concern and work unchanged.

## Security

- Hosts come only from the command line and from explicit pairing; `ovrd` never discovers or scans anything.
- SSH hosts: authentication and host verification are OpenSSH's, using your existing configuration.
- Dial-in hosts: mutual TLS 1.3 with keys pinned at pairing. A host serves only the laptop it paired with; the
  laptop accepts only hosts it paired. Pairing codes are one-shot, expiring, and rate-limited.
- Dial-in agents run as a normal user, inside `~/ovr` / `C:\ovr` only.
- No environment is forwarded, so local tokens and keys cannot leak into the remote process.
- `.gitignore`d files never leave the laptop, and neither does Git history: hosts hold only snapshots.
- Commands travel as argument vectors and the SSH agent binary is uploaded over SFTP, so there is no shell
  injection surface.
- Checkouts live under one root per host and can be deleted at any time; the next run recreates them.

## Implementation

- Rust. Two binaries from one workspace: `ovr` (CLI) and `ovrd` (server, agent, dial-in client).
- Crates: `clap`, `miette`, `tokio`, `serde_json`, `rustls` + `ed25519-dalek`, `portable-pty`, `crossterm`,
  `windows-sys` (Job Objects, console control, named pipes), `fd-lock`.
- Uses the system `ssh` and `git`; reimplements neither.
- Prebuilt binaries for macOS, Linux, and Windows on arm64 and x86-64.

## Milestones

1. **Prototype.** A shell script doing snapshot commit, thin pack, `index-pack` + `read-tree` over `ssh -tt` to one
   Linux host, to measure bytes and latency per run.
2. **Core.** `ovr` + `ovrd` on the laptop, agent protocol over SSH with SFTP bootstrap, delta sync, PTY and
   exit-code passthrough on Linux and macOS.
3. **Dial-in.** `ovrd pair` / `ovrd connect`, pairing, mutual TLS, reconnect, `ovr hosts`.
4. **Windows.** Windows agent (Job Objects, ConPTY, paths, Scheduled Task install) and Windows laptop.
5. **Later, only if needed.** `ovrd` on an always-on VM shared by several laptops, copying outputs back, a
   remembered default host, detached runs that survive the laptop sleeping.

## Prior art

- **mainframer, cargo-remote.** Same sync-run pattern, but full-tree `rsync` per run, SSH only, and a Unix remote
  shell. `ovr` differs in Git-native delta sync, `.gitignore`-driven exclusion, dial-in hosts, and Windows.
- **Bazel remote execution, sccache-dist.** True distributed builds that need a worker mesh; complementary.
- **Nix remote builders, `docker buildx` SSH builders.** Same idea, scoped to one build system.
- **CI runners.** Outbound-only agents are the right shape, but not interactive; live output and Ctrl-C are the
  point of `ovr`.
- **Editor remote modes.** Best when working entirely on the remote; `ovr` keeps code and editor local.

## Open questions

- `<repo>` collisions when two local repositories share a basename: accept, or append a short hash of the origin URL.
- Tools that derive versions from Git history (Nerdbank.GitVersioning, MinVer, Bazel stamping with `git describe`)
  see a single parentless commit on the remote. Accept, or let them opt in to history by sending `HEAD` as the
  snapshot's parent when the remote already has it.
- Whether `ovrd` should also accept a Tailscale `whois` check on dial-in as an additional gate, or whether pinned
  keys alone are enough.
