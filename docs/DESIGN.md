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
| `ovrd` | laptop **and** every host | The server. On the laptop it owns connections to hosts and runs jobs through them. On a host it is the agent: kept running over an SSH session, or dialing out to the laptop. |

The CLI knows nothing about transports. The server knows nothing about Git working trees. Hosts of both kinds look
identical above the transport layer.

## Command surface

```
ovr HOST [CMD...]           run CMD on HOST (no CMD: login shell in the synced checkout)
ovr hosts                   list dialed-in hosts and their OS/arch

ovrd                        the laptop server; started automatically by ovr, or by launchd/systemd --user
ovrd pair                   print a one-shot command to run on a host that must dial in
ovrd connect ADDR CODE      on a host: pair with the laptop at ADDR and keep serving it (printed by `pair`)
ovrd connect                on a host: keep serving the laptop it already paired with
```

- `HOST` is a dialed-in host's name (its hostname, as shown by `ovr hosts`) or an SSH destination: an alias from
  `~/.ssh/config`, `user@hostname`, or a hostname. Dialed-in names win if both match.
- `CMD...` is passed through verbatim, including flags (`ovr box cargo build --release`); nothing after `HOST` is
  parsed by `ovr`.
- The remote command runs in the same directory relative to the repository root as your current directory, so
  `cd crates/engine && ovr box cargo test` does what you expect.
- The remote command's exit code is returned unchanged; a command killed by a signal reports `128 + signal`.
  Failures in `ovr` itself exit with `125`, following the `docker run` convention.

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

Every run is **one stream** to the host's agent: handshake, sync, execution, and terminal I/O travel over it.
Each host has one long-lived connection to its agent, multiplexed into a channel per run: an SSH session that
`ovrd` keeps alive between runs, or the host's own TLS connection for dialed-in hosts. Above that the two are
indistinguishable.

## Transports

### SSH

`ovrd` runs `ssh -T HOST .cache/ovr/bin/ovrd-<version> agent`, inheriting everything in `~/.ssh/config`: jump
hosts, `ProxyCommand`, keys, certificates, Tailscale SSH. The agent answers with a `BANNER` naming its version and
platform. If the binary is missing, `ovrd` runs `uname -sm`, uploads the matching `ovrd` over SFTP (its own
executable when the platforms match, otherwise the release archive for that target, downloaded once and cached in
the laptop's cache directory), and retries. Nothing is installed by hand, and version skew is impossible: both ends are
always the same build. Output before the banner is reported as a noisy shell startup file.

The agent process is the connection: `ovrd` multiplexes every run to that host over its stdio, and closes it ten
minutes after the last run. This needs no `ControlMaster`, so a Windows laptop gets the same warm path. SSH's
`ServerAliveInterval` probes notice a vanished host within a minute.

### Dial-in

For hosts that cannot accept SSH, including every Windows workstation:

```
 laptop$ ovrd pair
 run this on the host within 10 minutes:
   ovrd connect 100.64.0.10:7420 7KQX-M4RT

 winbox> ovrd connect 100.64.0.10:7420 7KQX-M4RT
 paired with laptop; serving as winbox
```

- `ovrd` on the laptop listens on `:7420`. Each side has a self-signed Ed25519 certificate; every connection is
  TLS 1.3 and each side checks the other's certificate fingerprint, so **both keys are pinned**. The host runs
  `ovrd connect ADDR CODE` once: it sends its name with an HMAC of both fingerprints keyed by the code, and the
  laptop answers with its own HMAC, so neither side can be impersonated by someone without the code. Codes are
  single-use and expire after ten minutes, and five wrong guesses revoke every outstanding code. The identity and
  pairing records live in the data directory (see [Files](#files)); they are a record, like `known_hosts`, not
  configuration. TCP keepalive notices a vanished peer within a minute.
- After pairing, `ovrd connect` keeps serving: it reconnects with backoff when the laptop sleeps or moves
  networks, and only ever accepts jobs from the laptop it paired with. Pairing installs it to start at login as
  the logged-in user (Scheduled Task on Windows, a `launchd` agent on macOS, a `systemd --user` unit on Linux),
  never as SYSTEM or root, and it works only inside `~/ovr` / `C:\ovr`. Where no service manager is available it
  keeps serving in the foreground. A Linux host that should serve without a login session needs
  `loginctl enable-linger`.
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
 t1  HELLO{repo, base, snapshot} ──────────────────────────────────────►┼─► agent: WELCOME{ok | need_full}
 t1  PACK frames (speculative, vs base) ──────────────────────────────►┼─► agent: index-pack as frames arrive
 t2  EXEC{argv, cwd, tty, cols, rows} ────────────────────────────────►┼─► agent: lock, reset, SYNCED, spawn
     ◄── OUT/ERR frames ... ◄── EXIT{code} ────────────────────────────┘
```

- **Speculative send.** `ovr` records, per host, the last snapshot it delivered as a ref,
  `refs/ovr/hosts/<host>/<repo>`,
  so your local `git gc` cannot prune the delta base out from under the next run (losing it would silently degrade
  to a full-tree send). It assumes the host still has it and streams the thin pack immediately after `HELLO`
  without waiting for the reply. The agent answers `WELCOME` as soon as it reads `HELLO`; on `need_full`
  (checkout deleted, first contact) it discards the speculative pack and the client sends one with no base. The common case pays zero
  extra round trips; the rare case pays one.
- **Pipelined.** `EXEC` follows the pack on the same stream, so the agent starts the command the moment checkout
  finishes without another exchange.
- **Connections outlive runs.** SSH agents stay up for ten minutes; dial-in connections stay up indefinitely. A
  warm run never pays a TCP or TLS handshake, let alone a jump host.
- **Overlapped.** Opening the host stream and building the snapshot run concurrently.

### How the remote applies it

1. `git index-pack --stdin --fix-thin` streams the pack straight into `.git/objects/pack/`.
2. `git reset --hard <snapshot>` makes the snapshot `HEAD`. Git rewrites only files whose index entry or stat data
   differ, so a one-file edit rewrites one file and its mtime, and everything else keeps its timestamps. Cargo,
   Bazel, and MSBuild see exactly the changed inputs. Deleted files are removed; `.gitignore`d outputs are never
   touched because they are not in either tree. Tracked files the host changed, for example a build rewriting a
   lockfile, are restored even when the snapshot itself is unchanged: the laptop is the source of truth.
3. Reflogs are off in the checkout, so the previous snapshot becomes unreachable at once. After `EXIT` is sent, the
   agent runs `git gc --auto` in the background so pack accumulation never lands on a run's critical path.

The remote never runs `git clean`. The reset already removes files that left the snapshot, and anything the build
creates that is not tracked stays, exactly as it would locally.

### The wire

- Frames are `len:u32le, kind:u8, payload`, where `len` covers the kind and payload, read and written with
  `tokio-util`'s length-delimited codec. Control messages (`HELLO`, `WELCOME`, `EXEC`, `SYNCED`, `RESIZE`,
  `EXIT`, and `BANNER`/`ACCEPT`/`PAIR` on connection setup) are small JSON and occur a handful of times per run.
  Data frames (`PACK`, `OUT`, `ERR`, `IN`) carry raw bytes, up to 64 KiB each, and are what the stream is made of.
- One connection to a host carries many runs as independent [yamux](https://github.com/libp2p/rust-yamux) streams
  with per-stream flow control, so a slow terminal on one run never stalls another.
- The same frames flow `ovr → ovrd` over the local socket and `ovrd → agent` over SSH or TLS; `ovrd`
  forwards them without parsing the data frames.
- PTY output is read in large chunks and forwarded as-is; no line buffering, no re-encoding. Build output
  throughput is bounded by the transport, which comfortably exceeds any terminal.
- Backpressure is the stream's: the agent stops reading the PTY when the client is slow, so `yes | head` behaves.
  After the command exits, output drains until the terminal or pipes have been idle for a second, so a slow client
  gets everything while a leftover build server holding them open does not hang the run.
- Input the command has not read yet is held by the agent up to 1 MiB while it keeps reading frames, so Ctrl-C and
  a vanished client are noticed even when the command ignores its input.

### Checkout location

The checkout lives at `~/ovr/<repo>` on Unix and `C:\ovr\<repo>` on Windows, where `<repo>` is the basename of
the local repository root. The path is stable and short, so incremental state (`target/`, Bazel's output base,
`obj/`) and path-keyed caches such as sccache stay valid across runs. Submodules are fetched by the host from their
URLs with the host user's credentials; the checkout has no `origin`, so relative submodule URLs do not resolve, and
a failed update is shown as a warning before the command runs. Git LFS content is not sent.

## Agent

The agent is `ovrd` on the host, whether spawned per run over SSH or resident via `ovrd connect`.

- **Exec:** argument vector, working directory relative to the checkout, terminal size. No shell string, so no
  quoting problems on any OS. On Unix the argument vector is run through the user's login shell
  (`$SHELL -l -c 'exec "$@"'`) so the host's `PATH` and toolchains are the ones the user sees. Nothing from the
  laptop's environment is forwarded; terminal runs get `TERM=xterm-256color`.
- **Terminal:** when `ovr` runs in a terminal, the agent creates a pseudo-terminal (`openpty` on Unix, ConPTY on
  Windows); otherwise stdout and stderr stay separate pipes, so `ovr box cargo metadata | jq` works. Input, resize,
  and interrupt frames flow the other way. Colors, progress bars, and interactive prompts work.
- **Lifetime:** the command runs in its own process group on Unix and in a Job Object on Windows. Ctrl-C is
  forwarded as an interrupt: `^C` on the terminal, `SIGINT` to the process group, or, for a Windows command without
  a terminal, which has no equivalent signal, terminating its Job Object. A second Ctrl-C abandons the
  run and exits `130`. If the stream drops or the run is abandoned, the whole process tree is killed; processes a
  command leaves behind after exiting normally, such as build servers, are left alone.
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

## Files

`ovr` and `ovrd` follow each platform's conventions on the laptop, and on hosts that dial in:

| | Linux | macOS | Windows |
|---|---|---|---|
| Identity, pairings, locks | `~/.local/share/ovr` | `~/Library/Application Support/ovr` | `%LOCALAPPDATA%\ovr\data` |
| Downloaded agents | `~/.cache/ovr` | `~/Library/Caches/ovr` | `%LOCALAPPDATA%\ovr\cache` |
| Logs | `~/.local/state/ovr` | `~/Library/Logs/ovr` | `%LOCALAPPDATA%\ovr\data` |
| Socket | `$XDG_RUNTIME_DIR/ovr`, else logs | `~/Library/Application Support/ovr` | named pipe |

XDG variables are honoured on Linux. SSH hosts keep the agent at `~/.cache/ovr/bin/`, resolved by the host's own
shell. Checkouts live in `~/ovr` and `C:\ovr`, deliberately short and visible.

## Implementation

- Rust. Two binaries from one workspace: `ovr` (CLI) and `ovrd` (server, agent, dial-in client).
- Crates: `clap`, `miette`, `tokio` + `tokio-util` (framing), `yamux` (multiplexing), `serde_json`, `rustls` +
  `rcgen` + `ring`, `process-wrap` (process groups, Job Objects), `portable-pty`, `crossterm`, `directories`, `nix`
  (signals), `windows-sys` (ConPTY Job Objects, console control). File locks are `std::fs::File::lock`.
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
