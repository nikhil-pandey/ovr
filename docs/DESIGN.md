# ovr: run your builds over there

Status: design proposal.

```sh
ovr bigbox -- cargo test -p engine
ovr arm64 -- bazel test //services/...
ovr winbox -- dotnet build -c Release
ovr bigbox                       # interactive shell in the synced checkout
ovr -d bigbox -- cargo bench     # start in the background; attach, follow, or kill it later
```

`ovr HOST -- CMD...` runs `CMD` on `HOST` against your **local, uncommitted** working tree, streams the output
back as if it ran locally, and returns the remote exit code. `HOST` is either something `ssh HOST` understands or a
machine that has dialed in to your initiator. There is nothing to configure.

The remote checkout behaves like the local one. Run as many commands against it at once as you would in local
terminals; each new run brings the checkout up to date with your latest edits, under whatever is already running.
A run belongs to the host, not to the connection: when the network, the initiator, or an overloaded host drops the
connection, the command keeps running and `ovr` picks it up again where it left off.

## Problem

Large Bazel, Cargo, and .NET builds saturate a laptop. Most developers have a bigger machine somewhere (cloud VM,
dev box, Linux box under the desk, Windows workstation), but the only easy way to use it is to move the whole
workflow onto it. `ovr` keeps editing local and sends the heavy work elsewhere with one command.

Constraints that shape the design:

- **Hosts are reached one of two ways.** Over SSH when the host accepts it, or by the host dialing **out** to your
  initiator when it does not. Windows workstations, machines behind NAT, locked-down boxes, and CI runners are the
  second kind. Overlay networks such as Tailscale make the initiator reachable from anywhere, and where none reaches,
  `ovrd` can open a tunnel; `ovr` does not care which.
- **Bandwidth may be limited.** Sync sends deltas, never whole trees.
- **Mixed platforms.** macOS, Linux, and Windows on arm64 and x86-64, on either side. You pick a host whose OS and
  architecture match your target; `ovr` does not pick for you.

## Principles

- **No configuration.** No project file and no user file. The only state is what `ssh` already has and the pairing
  and tunnel records created by `ovrd pair`. A few environment variables move where files live (see
  [Files](#files)); none is needed.
- **No environment forwarding.** The remote command sees the remote user's login environment, nothing from the
  initiator. Credentials never leave the machine by accident.
- **Git decides what is sent.** Tracked and untracked files are sent; `.gitignore`d files are never sent. Build
  outputs, `node_modules/`, `.venv/`, secrets in ignored files: none of it leaves the initiator.
- **No checks.** No doctor, no size limits, no policy. If the command fails remotely, you see the same error you
  would see locally.
- **Builds stay warm.** Each host keeps a persistent checkout per repository, so `target/`, `bazel-out`, `bin/`, and
  `obj/` survive between runs and incremental builds stay incremental.
- **Like local.** Concurrent runs share one checkout, edits land under running commands, and a run ends when its
  command does or when you stop it, never because a connection dropped.

## Non-goals

- Distributing one build across machines (Bazel remote execution does that).
- Cross-OS builds; pick a host whose OS matches.
- Host selection, pools, load balancing. You name the host.
- Two-way sync or remote editing. Use your editor's remote mode for that.
- Copying build outputs back. Run the thing remotely, or `scp` what you need.
- Provisioning machines, SSH keys, networks, or tunnel accounts. Tunnels use a provider CLI you already have.
- Keeping CI jobs alive. A tunnel reaches a runner for as long as its job runs, no longer.

## Two binaries

The **initiator** is the machine you run `ovr` on, usually your laptop; **hosts** are the machines that run the
commands.

| Binary | Where it runs | Job |
|---|---|---|
| `ovr` | initiator | The CLI. Snapshots the tree and hands the run to the local `ovrd` over a Unix socket (named pipe on Windows). Starts `ovrd` on demand if it is not running. |
| `ovrd` | initiator **and** every host | The server. On the initiator it owns connections to hosts and runs jobs through them. On a host it is the agent: kept running over an SSH session, or dialing out to the initiator. |

The CLI knows nothing about transports. The server knows nothing about Git working trees. Hosts of both kinds look
identical above the transport layer.

## Command surface

```
ovr [-d] HOST [-- CMD...]   run CMD on HOST (no CMD: login shell in the synced checkout); -d: in the background
ovr hosts                   list dialed-in hosts and their OS/arch
ovr ps [HOST]               list runs: running, and finished in the last 7 days
ovr attach HOST RUN         reconnect a terminal to a run, as if it had been started here
ovr logs HOST RUN           print a run's output, following it until the run ends
ovr kill HOST RUN           kill a run's whole process tree

ovrd                        the initiator server; started automatically by ovr, or by launchd/systemd --user
ovrd pair [PROVIDER]        print a one-shot command for a host that must dial in; PROVIDER opens a tunnel first
ovrd tunnel                 list open tunnels and the command that points a paired host at each
ovrd tunnel close PROVIDER  close a tunnel that `ovrd pair` opened
ovrd connect ADDR CODE      on a host: pair with the initiator at ADDR and keep serving it (printed by `pair`)
ovrd connect [ADDR]         on a host: keep serving the initiator it paired with, at ADDR from now on if given
```

- `HOST` is a dialed-in host's name (its hostname, as shown by `ovr hosts`) or an SSH destination: an alias from
  `~/.ssh/config`, `user@hostname`, or a hostname. Dialed-in names win if both match. Subcommand names win over host
  names; reach a host called `ps` as `user@ps`.
- `--` is required before `CMD`, so it is always clear where `ovr`'s arguments end and the command begins.
  Everything after it is passed through verbatim, including flags (`ovr box -- cargo build --release`). Anything
  else after `HOST` is an error that suggests `--`.
- The remote command runs in the same directory relative to the repository root as your current directory, so
  `cd crates/engine && ovr box -- cargo test` does what you expect.
- The remote command's exit code is returned unchanged; a command killed by a signal reports `128 + signal`.
  Failures in `ovr` itself exit with `125`, following the `docker run` convention.
- `RUN` is the number the host gives each run, shown by `ovr ps`. `ovr -d` prints it and exits `0` once the command
  has started. `attach` and `logs` exit with the run's code once it ends, so `ovr logs box 12 && deploy` works.
- Runs belong to the initiator that started them: `ovr ps` lists only this initiator's runs. Without `HOST` it asks
  every dialed-in host and every SSH host with an open connection.
- `ADDR` is `host:port`, or a tunnel: `cloudflare:HOSTNAME` or `devtunnel:ID`. `PROVIDER` is `cloudflare` or
  `devtunnel`. `ovrd connect ADDR CODE --foreground` pairs and serves without installing anything at login, for CI
  runners and other short-lived hosts.

## Architecture

```
 Initiator                                                Hosts
 ┌──────────────────────────────────────────────┐
 │ ovr HOST -- cargo test                       │         ┌─────────────────────────────────┐
 │   snapshot tree ──► thin pack                │  ssh    │ linux-box                       │
 │   │ unix socket                              │ ───────►│   ssh HOST ovrd agent           │
 │   ▼                                          │         │   <checkouts>/<repo>-<key>/     │
 │ ovrd                                         │         └─────────────────────────────────┘
 │   ├─ ssh hosts: spawn ssh, upload agent      │
 │   └─ dialed-in hosts: TLS listener :7420     │  TLS    ┌─────────────────────────────────┐
 │        paired keys, hostname ─► connection   │ ◄─dials─│ winbox                          │
 │                                              │   out   │   ovrd connect <initiator> CODE │
 │ one agent protocol over either stream        │         │   <checkouts>\<repo>-<key>\     │
 └──────────────────────────────────────────────┘         └─────────────────────────────────┘
```

Every run is **one stream** to the host's agent: handshake, sync, execution, and terminal I/O travel over it.
Each host has one long-lived connection to its agent, multiplexed into a channel per run: an SSH session that
`ovrd` keeps alive between runs, or the host's own TLS connection for dialed-in hosts. Above that the two are
indistinguishable.

The agent does not own the commands it starts. Each run has its own small **run keeper** process on the host that
holds the command, its terminal, and its output log (see [Runs](#runs)). Streams, connections, and agents can all
come and go; a new agent finds the keepers already running and serves them again.

## Transports

### SSH

`ovrd` runs `ssh -T HOST .cache/ovr/bin/ovrd-<version> agent`, inheriting everything in `~/.ssh/config`: jump
hosts, `ProxyCommand`, keys, certificates, Tailscale SSH. The agent answers with a `BANNER` naming its version and
platform. If the binary is missing, `ovrd` runs `uname -sm`, uploads the matching `ovrd` over SFTP (its own
executable when the platforms match, otherwise the release archive for that target, downloaded once and cached in
the initiator's cache directory), and retries. Nothing is installed by hand, and version skew is impossible: both
ends are always the same build. Output before the banner is reported as a noisy shell startup file.

The agent process is the connection: `ovrd` multiplexes every run to that host over its stdio, and closes it ten
minutes after the last attached run ends or detaches. Closing it, or losing it, does not touch running commands;
their keepers carry on, and the next connection's agent finds them. This needs no `ControlMaster`, so a Windows
initiator gets the same warm path. SSH's `ServerAliveInterval` probes notice a vanished host within a minute.

### Dial-in

For hosts that cannot accept SSH, including every Windows workstation:

```
 initiator$ ovrd pair
 run this on the host within 10 minutes:
   ovrd connect 100.64.0.10:7420 7KQX-M4RT

 winbox> ovrd connect 100.64.0.10:7420 7KQX-M4RT
 paired with 100.64.0.10:7420; serving as winbox
```

- `ovrd` on the initiator listens on `:7420`. Each side has a self-signed Ed25519 certificate; every connection is
  TLS 1.3 and each side checks the other's certificate fingerprint, so **both keys are pinned**. The host runs
  `ovrd connect ADDR CODE` once: it sends its name with an HMAC of both fingerprints keyed by the code, and the
  initiator answers with its own HMAC, so neither side can be impersonated by someone without the code. Codes are
  single-use and expire after ten minutes, and five wrong guesses revoke every outstanding code. The identity and
  pairing records live in the data directory (see [Files](#files)); they are a record, like `known_hosts`, not
  configuration.
- Both ends send a heartbeat on the connection every 15 seconds and close it after 45 seconds without one, so a
  vanished peer is noticed within a minute even through a tunnel or proxy, where TCP keepalive only probes the
  nearest hop, which stays up.
- After pairing, `ovrd connect` keeps serving: it reconnects with backoff when the initiator sleeps or moves
  networks, and only ever accepts jobs from the initiator it paired with. Pairing installs it to start at login as
  the logged-in user (Scheduled Task on Windows, a `launchd` agent on macOS, a `systemd --user` unit on Linux),
  never as SYSTEM or root, and it works only inside its checkouts directory. Restarting the service leaves run
  keepers alone (`KillMode=process` on Linux). The service keeps the `PATH` and `OVR_*` directory variables it was
  installed from, so it finds tunnel CLIs wherever the user installed them and its files where the user moved them;
  a Scheduled Task reads the user's persistent environment instead. With `--foreground`, or where no service
  manager is available, it serves in the foreground instead and installs nothing. A Linux host that should serve without a login session needs
  `loginctl enable-linger`.
- `ovrd connect ADDR` without a code moves a paired host to a new address of the same initiator. It needs no code
  because the pinned key, not the address, identifies the initiator: the host accepts the new address only once the
  initiator there presents that key.
- The host is addressable by its hostname: `ovr winbox -- dotnet test`. `ovr hosts` lists what is connected.
- Reachability is the network's job. On Tailscale, `100.x.y.z` is the initiator's stable address and the WireGuard
  layer already authenticates peers; the pinned keys are defense in depth and also make the design work over any
  other overlay, a plain LAN, or a tunnel.
- Dial-in works for any host, not just Windows. A Linux box behind NAT with no SSH server runs the same command.

### Tunnels

When no network path reaches the initiator, as for a CI runner or a machine on another network without an overlay,
`ovrd` opens one with a tunnel provider's CLI: `cloudflared` for Cloudflare quick tunnels, or `devtunnel` for
Microsoft Dev Tunnels.

```
 initiator$ ovrd pair cloudflare
 run this on the host within 10 minutes:
   ovrd connect cloudflare:quiet-river-delta.trycloudflare.com 7KQX-M4RT

 runner$ ovrd connect cloudflare:quiet-river-delta.trycloudflare.com 7KQX-M4RT --foreground
 paired with cloudflare:quiet-river-delta.trycloudflare.com; serving as runner
```

- **A tunnel only carries bytes.** Pairing and pinned mutual TLS run end to end through it unchanged, so the
  provider sees ciphertext and connection metadata, and whoever finds the public address can do no more than a
  stranger on the same LAN. Tunnels are therefore opened for anonymous access: no account, token, or other
  credential appears in any printed command or log, and `ovr` never handles a provider login. The initiator needs
  the provider's CLI installed, and for Dev Tunnels logged in; the host needs the CLI installed.
- **The initiator's `ovrd` owns the tunnel.** `ovrd pair PROVIDER` opens a tunnel to `:7420` unless one for that
  provider is already open, then prints the host's command with the tunnel's address. From then on `ovrd` restarts
  the provider's CLI when it exits and reopens the tunnel whenever `ovrd` starts, so the tunnel outlives the
  terminal that asked for it. Only `ovrd tunnel close PROVIDER` closes it, which also deletes what `ovrd` created at
  the provider; if that deletion fails, the tunnel stays open. Open tunnels are recorded in the data directory as
  soon as they are created at the provider, so none is forgotten.
- **The host's `ovrd connect` owns the forwarder.** Given a tunnel address, it starts the provider's local forwarder
  on a free port, dials through it, and restarts it on every reconnect. The forwarder lives and dies with
  `ovrd connect`, so the one printed command is all a host needs, whether it serves at login or in a CI job.
- **Addresses.** A Dev Tunnel keeps its ID across restarts, so paired hosts reconnect on their own; it suits hosts
  that stay. Should the provider lose it, as when it expires unused, `ovrd` creates a new one in its place. A Cloudflare quick tunnel gets a new hostname whenever it restarts, for example when the initiator
  reboots; it suits short-lived hosts such as CI runners. `ovrd tunnel` shows each tunnel's current address with the
  `ovrd connect ADDR` command that moves a paired host to it.
- **CI runners.** `--foreground` serves for as long as the job runs and installs nothing. When the job ends, its
  runs end with it.

Nothing about the initiator-side `ovrd` is tied to a laptop; the same server can run on an always-on VM later
(see Milestones). It usually runs on your laptop because that needs no additional machine.

## Efficiency

The budget for a warm run is: **one network round trip + a few milliseconds + the command itself**. Nothing else is
allowed on the critical path. Every decision below exists to hit that.

### What is sent

- **Trees, not history.** The remote never needs your Git history; it needs the files. A snapshot is a
  **parentless commit** over the working tree, so the first sync sends exactly the working tree (compressed, ~size of
  a source tarball), never the repository's history. A ten-year monorepo costs the same as a fresh one.
- **Thin packs, not bundles.** New objects go out as `git pack-objects --thin --shallow --stdout --revs` with input
  `<snapshot>\n^<previous snapshot>`. Snapshots have no parents, so `--shallow` is what makes the previous
  snapshot's trees and blobs count as already sent; without it every changed sync would resend the whole tree. Thin
  packs may delta against objects the receiver already has, so editing one line of a 1 MB file sends a few bytes of
  delta, not a recompressed blob. The remote runs `git index-pack --stdin --fix-thin`, which writes a pack directly;
  no loose objects, no `git fetch`, no refs negotiation. The host logs each sync with its checkout, the bytes
  received, and how long receiving and applying took.
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

Concurrent `ovr` invocations in one repository take snapshots one at a time under a lock on `.git/ovr/lock`, which
also numbers them: each snapshot gets the next value of a counter kept beside the index. The number travels with
the snapshot as `seq`, so the host can tell which of two snapshots reflects your later edits however the network
orders them.

### How a run is sequenced

```
 t0  ovr: connect to ovrd socket; ovrd: open/reuse host stream ────────┐  (overlaps the snapshot)
 t0  ovr: snapshot: add -A, write-tree, commit-tree                    │
 t1  HELLO{checkout, seq, base, snapshot} ─────────────────────────────►┼─► agent: WELCOME{ok | need_full}
 t1  PACK frames (speculative, vs base) ──────────────────────────────►┼─► agent: index-pack as frames arrive
 t2  EXEC{argv, cwd, tty, cols, rows} ────────────────────────────────►┼─► agent: apply, SYNCED, start keeper, ATTACHED{run}
     ◄── OUT/ERR frames ... ◄── EXIT{code} ────────────────────────────┘
```

- **Speculative send.** `ovr` records, per host, the last snapshot it delivered as a ref,
  `refs/ovr/hosts/<host>/<checkout>`, so your local `git gc` cannot prune the delta base out from under the next
  run (losing it would silently degrade to a full-tree send). It assumes the host still has it and streams the thin
  pack immediately after `HELLO` without waiting for the reply. The agent answers `WELCOME` as soon as it reads
  `HELLO`; on `need_full` (checkout deleted, first contact) it discards the speculative pack and the client sends
  one with no base. The common case pays zero extra round trips; the rare case pays one.
- **Pipelined.** `EXEC` follows the pack on the same stream, so the agent starts the command the moment checkout
  finishes without another exchange.
- **Connections outlive runs.** SSH agents stay up for ten minutes; dial-in connections stay up indefinitely. A
  warm run never pays a TCP or TLS handshake, let alone a jump host. Runs, in turn, outlive connections; see
  [Runs](#runs).
- **Overlapped.** Opening the host stream and building the snapshot run concurrently.
- **Failures mid-sync are heard.** An agent that fails while the pack is still arriving, say on a full disk,
  answers `ERROR` and then reads and discards the rest of the stream, and the client listens to the agent while
  it sends. Either side alone keeps a pack larger than the stream's window from stalling forever on a host that
  stopped reading.

### How the remote applies it

Applying holds a per-checkout lock for as long as the two steps below take, typically milliseconds, and never
while a command runs. Commands already running in the checkout keep running while it changes under them, exactly
as they would when you save files locally mid-build.

1. `git index-pack --stdin --fix-thin` streams the pack straight into `.git/objects/pack/`.
2. `git reset --hard` to the newest snapshot the checkout has seen: this run's, or a higher `seq` already applied
   by a concurrent run, so a run that arrives late never rolls your edits back. Git rewrites only files whose index
   entry or stat data differ, so a one-file edit rewrites one file and its mtime, and everything else keeps its
   timestamps. Cargo, Bazel, and MSBuild see exactly the changed inputs. Deleted files are removed; `.gitignore`d
   outputs are never touched because they are not in either tree. Tracked files the host changed, for example a
   build rewriting a lockfile, are restored even when the snapshot itself is unchanged: the initiator is the source
   of truth.
3. Reflogs are off in the checkout, so the previous snapshot becomes unreachable at once. After a command exits,
   its keeper runs `git gc --auto` so pack accumulation never lands on a run's critical path.

The remote never runs `git clean`. The reset already removes files that left the snapshot, and anything the build
creates that is not tracked stays, exactly as it would locally.

### The wire

- Frames are `len:u32le, kind:u8, payload`, where `len` covers the kind and payload, read and written with
  `tokio-util`'s length-delimited codec. Control messages (`HELLO`, `WELCOME`, `EXEC`, `SYNCED`, `RESIZE`,
  `CONSUMED`, `EXIT`, `ATTACH`, `ATTACHED`, `KILL`, `RUNS`, `RUN_LIST`, and `BANNER`/`ACCEPT`/`PAIR` on connection
  setup) are small JSON.
  Data frames (`PACK`, `OUT`, `ERR`, `IN`) carry raw bytes, up to 64 KiB each, and are what the stream is made of.
- One connection to a host carries many runs as independent [yamux](https://github.com/libp2p/rust-yamux) streams
  with per-stream flow control, so a slow terminal on one run never stalls another. On a dial-in connection the
  host opens the first stream for heartbeats, before any run.
- The same frames flow `ovr → ovrd` over the local socket and `ovrd → agent` over SSH or TLS; `ovrd`
  forwards them without parsing the data frames.
- PTY output is read in large chunks and forwarded as-is; no line buffering, no re-encoding. Build output
  throughput is bounded by the transport, which comfortably exceeds any terminal.
- Backpressure never reaches the command: its keeper always drains output into the run's log. An attached client is
  fed live and, when it falls behind, catches up from the log under the stream's flow control. After the command
  exits, output drains until the terminal or pipes have been idle for a second, so a client gets everything while a
  leftover build server holding them open does not hang the run. When `ovr`'s own stdout closes, as in
  `ovr box -- yes | head`, it kills the run, as `SIGPIPE` would locally.
- Input has its own window: the client sends stdin only while less than 1 MiB is unconsumed, and the agent reports
  with `CONSUMED` as the command takes it. The agent therefore always keeps reading frames, and both sides relay
  input and output concurrently, so a command that ignores its input and a terminal that stops reading output never
  hold up Ctrl-C or each other.

### Checkout location

The checkout lives at `<checkouts>/<repo>-<key>` (see [Files](#files)), where `<repo>` is the
basename of the local repository root and `<key>` is a short hash of the initiator's identity and that root's absolute
path. Every run from the same local checkout shares one remote checkout; another initiator, another clone, or another
Git worktree gets its own, so they never overwrite each other. Moving the local repository starts a fresh, cold
checkout. The path is stable, so incremental state (`target/`, Bazel's output base, `obj/`) and
path-keyed caches such as sccache stay valid across runs. Submodules are fetched by the host from their
URLs with the host user's credentials; the checkout has no `origin`, so relative submodule URLs do not resolve, and
a failed update is shown as a warning before the command runs. Git LFS content is not sent.

## Agent

The agent is `ovrd` on the host, whether spawned over SSH or resident via `ovrd connect`. It syncs checkouts, starts
runs, and relays their I/O; the commands themselves belong to [run keepers](#runs).

- **Exec:** argument vector, working directory relative to the checkout, terminal size. No shell string, so no
  quoting problems on any OS. On Unix the argument vector is run through the user's login shell
  (`$SHELL -l -c 'exec "$@"'`) so the host's `PATH` and toolchains are the ones the user sees. Nothing from the
  initiator's environment is forwarded; terminal runs get `TERM=xterm-256color`.
- **Terminal:** when `ovr` runs in a terminal, the keeper creates a pseudo-terminal (`openpty` on Unix, ConPTY on
  Windows); otherwise stdout and stderr stay separate pipes, so `ovr box -- cargo metadata | jq` works. Input, resize,
  and interrupt frames flow the other way. Colors, progress bars, and interactive prompts work.
- **Lifetime:** the command runs in its own process group on Unix and in a Job Object on Windows. Ctrl-C, read as
  a key in raw mode or as a signal otherwise, is forwarded as an interrupt: `^C` on the terminal, ahead of pending
  input, `SIGINT` to the process group, or, for a Windows command without a terminal, which has no equivalent
  signal, terminating its Job Object. A second Ctrl-C stops the run: its whole process tree is killed at once and
  `ovr` exits `130`. `ovr kill` does the same from anywhere. Processes a command leaves behind after exiting
  normally, such as build servers, are left alone. ConPTY cannot start a command suspended, so on Windows a
  terminal command is assigned to its job just after starting.
- **Concurrency:** runs in the same checkout run side by side, as in several local terminals. Only applying a
  snapshot is serialised, briefly (see [How the remote applies it](#how-the-remote-applies-it)).

## Runs

A run is owned by its keeper on the host, never by a stream, a connection, or an agent. Losing any of those is a
disconnect, not a stop.

- **Keeper.** Each command is started through a keeper: `ovrd` itself in an internal mode, detached from the agent
  (its own session on Unix, outside the agent's Job Object on Windows where that job allows it), so killing the
  agent or its SSH session leaves it running. The keeper holds the command's process group or Job Object, its
  pseudo-terminal, and the run directory, and serves the run's frames on a socket in that directory (a named pipe
  on Windows). A new agent finds running keepers through their directories. Keepers are a few megabytes, so under
  memory pressure the kernel's OOM killer picks the build over its keeper.
- **Run directory.** `runs/<n>/` in the host's state directory (see [Files](#files)) holds the exec request, the
  checkout and snapshot, the initiator identity that started it, the start time, the output log (`out`, plus `err`
  for runs without a terminal), and, once the command ends, its exit code. Logs keep the last 32 to 64 MiB of
  each stream; offsets count from the start of the run, so a client always knows what it missed. Finished runs are
  removed after seven days.
- **Reconnect.** When a run's stream breaks, `ovr` says so on stderr, reconnects with backoff, and sends
  `ATTACH{run, out, err}` with the byte offsets it already has; the keeper replays the rest and continues live. A
  terminal run gets the current window size. Input typed while disconnected is dropped rather than replayed late.
  Ctrl-C while reconnecting leaves the run alone and exits `125` after printing the `ovr attach` command to resume
  it. The same holds when the initiator sleeps or `ovr` itself is killed: `SIGHUP` and `SIGTERM` disconnect, they do
  not stop, and `ovr ps` finds the run later.
- **Background.** `ovr -d` syncs and starts a run exactly as a foreground run does, prints its number, and exits.
  It gets a pseudo-terminal when `ovr` is in a terminal, so attaching later is interactive; without one, its stdin
  is empty. There is no detach key: start a run with `-d`, or close the terminal running it.
- **Attach and logs.** `ovr attach` resumes a run as if it had been started there: output from where the log
  begins, input, resize, Ctrl-C, and the exit code. Any number of clients may attach; all see the output, input is
  interleaved, and the most recent to attach or resize sets the terminal size. `ovr logs` prints the log and
  follows it until the run ends, without input; Ctrl-C stops following and leaves the run alone.
- **State.** `ovr ps` shows each run's host, number, checkout, command, age, and state: `running`, `exited CODE`,
  or `lost`. A run is lost when its keeper dies without recording an exit; the agent then kills the run's process
  group, and on Windows the Job Object has already ended it.
- **Upgrades.** Run directories are plain files, so `ps` and `logs` work across `ovrd` versions. Attaching needs a
  keeper of the same build; older keepers run to completion and are then listed like any other finished run.

## Windows

Windows works as a host (via dial-in) and as the initiator.

- **Host:** `ovrd connect` installed as a Scheduled Task at logon. Job Objects, ConPTY,
  `core.longpaths=true` set on the checkout. `cargo`, `bazel`, and `dotnet` locate their toolchains themselves;
  `ovr` runs no `vcvars` or setup scripts.
- **Initiator:** `ovrd` listens the same way and the CLI talks to it over a named pipe. The snapshot applies
  `.gitattributes` normalisation, so a Linux host receives the same content a real commit would. The console uses
  raw mode and VT output, and a control handler turns Ctrl-C into an interrupt frame instead of killing `ovr`.

## Build caches

`ovr` does not configure caching, but stable checkout paths and persistent ignored directories make the standard
tools effective: Cargo's `target/`, Bazel's output base, and MSBuild's `obj/` all stay warm on the host. Shared
caches such as sccache or bazel-remote are a per-host concern and work unchanged.

## Security

- Hosts come only from the command line and from explicit pairing; `ovrd` never discovers or scans anything.
- Tunnels are transport only and anonymous by design: authentication is the pinned mutual TLS inside them, so a
  tunnel's public address is not a secret, and no provider credential is ever printed or logged.
- SSH hosts: authentication and host verification are OpenSSH's, using your existing configuration.
- Dial-in hosts: mutual TLS 1.3 with keys pinned at pairing. A host serves only the initiator it paired with; the
  initiator accepts only hosts it paired. Pairing codes are one-shot, expiring, and rate-limited.
- Dial-in agents run as a normal user, inside their checkouts directory only.
- No environment is forwarded, so local tokens and keys cannot leak into the remote process.
- `.gitignore`d files never leave the initiator, and neither does Git history: hosts hold only snapshots.
- Commands travel as argument vectors and the SSH agent binary is uploaded over SFTP, so there is no shell
  injection surface.
- Checkouts live under one root per host and can be deleted at any time; the next run recreates them.
- Run directories, logs included, and keeper sockets are private to the host user. `ovr ps` shows only the
  current initiator's runs, but that is a convenience, not a boundary: every run belongs to the host user.

## Files

`ovr` and `ovrd` follow each platform's conventions on the initiator and on hosts:

| | Linux | macOS | Windows |
|---|---|---|---|
| Identity, pairings, tunnels, locks | `~/.local/share/ovr` | `~/Library/Application Support/ovr` | `%LOCALAPPDATA%\ovr\data` |
| Downloaded agents | `~/.cache/ovr` | `~/Library/Caches/ovr` | `%LOCALAPPDATA%\ovr\cache` |
| Logs | `~/.local/state/ovr` | `~/Library/Logs/ovr` | `%LOCALAPPDATA%\ovr\data` |
| Runs (hosts) | `~/.local/state/ovr/runs` | `~/Library/Application Support/ovr/runs` | `%LOCALAPPDATA%\ovr\data\runs` |
| Checkouts (hosts) | `~/.local/share/ovr/checkouts` | `~/Library/Application Support/ovr/checkouts` | `%LOCALAPPDATA%\ovr\data\checkouts` |
| Socket | `$XDG_RUNTIME_DIR/ovr`, else logs | `~/Library/Application Support/ovr` | named pipe |

XDG variables are honoured on Linux. On every platform, absolute paths in these variables move a directory; empty
or relative values are ignored:

| Variable | Moves |
|---|---|
| `OVR_DATA_DIR` | identity, pairings, tunnels, locks; checkouts and the macOS and Windows socket and runs follow |
| `OVR_CACHE_DIR` | downloaded agents |
| `OVR_STATE_DIR` | logs, and runs at `runs/` beneath it |
| `OVR_CHECKOUTS` | checkouts, for example onto a fast NVMe disk: `OVR_CHECKOUTS=/nvme/ovr` |

They are read where the files are: on a host by its `ovrd`. Set them where that `ovrd` starts: in the shell that runs
`ovrd connect`, which records them in the login service; as user environment variables on Windows; and for SSH hosts
where a non-interactive `ssh HOST env` shows them. `ovr` and the local `ovrd` must see the same values.

Checkouts can be deleted at any time; the next run sends everything again. Some build tools reject paths with spaces,
which macOS's default has, and MSBuild still trips on Windows paths beyond 260 characters; `OVR_CHECKOUTS` moves
checkouts to a short path for those. SSH hosts keep the agent at `~/.cache/ovr/bin/`, resolved by the host's own shell
before `ovrd` runs.

## Implementation

- Rust. Two binaries from one workspace: `ovr` (CLI) and `ovrd` (server, agent, dial-in client).
- Crates: `clap`, `miette`, `tokio` + `tokio-util` (framing), `yamux` (multiplexing), `serde_json`, `rustls` +
  `rcgen` + `ring`, `process-wrap` (process groups, Job Objects), `portable-pty`, `crossterm`, `directories`, `nix`
  (signals), `windows-sys` (ConPTY Job Objects, console control). File locks are `std::fs::File::lock`.
- Uses the system `ssh`, `git`, and tunnel CLIs (`cloudflared`, `devtunnel`); reimplements none of them.
- Prebuilt binaries for macOS, Linux, and Windows on arm64 and x86-64.

## Milestones

1. **Prototype.** A shell script doing snapshot commit, thin pack, `index-pack` + `read-tree` over `ssh -tt` to one
   Linux host, to measure bytes and latency per run.
2. **Core.** `ovr` + `ovrd` on the initiator, agent protocol over SSH with SFTP bootstrap, delta sync, PTY and
   exit-code passthrough on Linux and macOS.
3. **Dial-in.** `ovrd pair` / `ovrd connect`, pairing, mutual TLS, reconnect, `ovr hosts`.
4. **Windows.** Windows agent (Job Objects, ConPTY, paths, Scheduled Task install) and Windows initiator.
5. **Tunnels.** `ovrd pair PROVIDER` with Cloudflare quick tunnels and Dev Tunnels, tunnel addresses and
   `--foreground` for `ovrd connect`, and heartbeats on dial-in connections.
6. **Later, only if needed.** `ovrd` on an always-on VM shared by several people's machines, copying outputs back, a
   remembered default host.

## Prior art

- **mainframer, cargo-remote.** Same sync-run pattern, but full-tree `rsync` per run, SSH only, and a Unix remote
  shell. `ovr` differs in Git-native delta sync, `.gitignore`-driven exclusion, dial-in hosts, and Windows.
- **Bazel remote execution, sccache-dist.** True distributed builds that need a worker mesh; complementary.
- **Nix remote builders, `docker buildx` SSH builders.** Same idea, scoped to one build system.
- **CI runners.** Outbound-only agents are the right shape, but not interactive; live output and Ctrl-C are the
  point of `ovr`.
- **Editor remote modes.** Best when working entirely on the remote; `ovr` keeps code and editor local.

## Open questions

- Tools that derive versions from Git history (Nerdbank.GitVersioning, MinVer, Bazel stamping with `git describe`)
  see a single parentless commit on the remote. Accept, or let them opt in to history by sending `HEAD` as the
  snapshot's parent when the remote already has it.
- Whether `ovrd` should also accept a Tailscale `whois` check on dial-in as an additional gate, or whether pinned
  keys alone are enough.
