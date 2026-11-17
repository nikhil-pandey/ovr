# AGENTS.md

`ovr` runs a command on another machine against the local working tree. [docs/DESIGN.md](docs/DESIGN.md) is the
source of truth for behaviour; read it before changing anything user-visible.

## Layout

- `crates/ovr`: the CLI. Snapshots the tree and hands the run to the local `ovrd`.
- `crates/ovrd`: the server. Initiator-side host connections, and the agent that runs on hosts.
- `crates/ovr-proto`: what both binaries share: the wire format, running `git`, where files live, and finding and
  starting the local `ovrd`.
- `action.yml`: the GitHub action that connects a job to an initiator through `ovrd connect ADDR github:FP`. It stays
  best effort: every failure is a warning.

## Principles

- Think holistically. Before changing anything, understand how it fits the whole design, and shape the change to
  leave the system coherent: the protocol, both binaries, the docs, and the tests. A narrow patch that only makes the
  immediate case work is not wanted. If the right fix is a restructuring, do the restructuring.
- There are no users yet. Breaking changes to the protocol, the CLI, or on-disk state are fine, and so are large
  refactors, when they move the design in the right direction. Do not add compatibility shims, migrations, or
  fallbacks for old behaviour.
- Keep it minimal. No configuration files, no flags that a fixed default can replace, and no environment variables
  beyond the `OVR_*` directory overrides in `crates/ovr-proto/src/paths.rs` and those a CI provider sets for its
  jobs, such as GitHub Actions' token request variables.
- Keep the README short: what it is, install, one example per command, a link to the design. Details belong in
  `docs/DESIGN.md`, not the README.
- Update `docs/DESIGN.md` in the same change when behaviour changes.

## Rust

- Clean, idiomatic Rust. Small modules, plain types, no clever abstractions before a second use exists.
- Versions, lints, and dependency versions live in the root `Cargo.toml`; members use `workspace = true` and pick
  features locally. Bazel reads the same manifests, `Cargo.lock`, and lint levels through rules_rs; never pin a crate
  in Bazel files.
- Cargo and Bazel both build everything, Bazel with hermetic Rust and LLVM toolchains. A new crate needs a
  `BUILD.bazel` modelled on an existing one.
- Errors are `thiserror` enums rendered with `miette`. No `unwrap`, `expect`, or `panic!` outside tests.
- `main.rs` only parses arguments, renders errors, and picks the exit code. Behaviour lives in the library.
- `ovr` exits with the remote command's code, or `125` for its own failures.

## Validate

```sh
bazel test //...            # every test, plus clippy (-D warnings) and rustfmt on every Rust target
bazel run @rules_rust//:rustfmt  # format every Rust target
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The client tests in `crates/ovr/src/session.rs` run a real `ovrd` agent in-process against temporary repositories
and need `git` on `PATH`. `crates/ovr/tests/e2e.rs` runs the real `ovr` and `ovrd` binaries on Unix, with a stand-in
`ssh` that serves a second home directory as the host, so runs are kept by real `ovrd keep` processes; under Cargo
it builds `ovrd` itself, under Bazel it gets both binaries as data. Its ignored tests dial in on the fixed port
`7420` through real Cloudflare quick tunnels and Dev Tunnels; after changing dial-in or tunnels, run
`cargo test -p ovr --test e2e -- --ignored` with `cloudflared` installed and `devtunnel` logged in. They take a few
minutes, mostly waiting for a new quick tunnel to resolve. Platform-specific code (Windows Job Objects,
ConPTY, named pipes) is only compiled on its platform. CI cross-builds and lints every released OS and architecture
with `bazel build --config=lint --platforms=@rules_rs//rs/platforms:<triple> //... //:rust-test-bundle`, and runs the
bundled tests on that OS.

Across runners, `.github/workflows/tunnel.yml` (driven by `.github/e2e/e2e.py`) has Linux, macOS, and Windows hosts
dial an initiator through a quick tunnel, either paired or as trusted jobs with an OIDC token. `e2e.yml` checks
syncs to both kinds on every pull request; `bench.yml` times real repositories on paired hosts nightly or on dispatch.

Checks gate pull requests only. A merge to `main` runs just the release workflow; nightly runs of `ci.yml` and
`bench.yml` on `main` refresh the caches pull requests restore from, since GitHub shares no other branch's caches.

## Release

Use [Conventional Commits](https://www.conventionalcommits.org); release-plz turns them into a release PR that bumps
the version and `CHANGELOG.md`. Merging that PR tags `v<version>`, and the release workflow publishes
`bazel build --config=release //packaging:dist`: every target cross-built from Linux, as archives and `.deb`/`.rpm`
packages, with the install scripts and `SHA256SUMS`. CI runs the same build and installs each artifact on its OS.
Never bump versions by hand. The install scripts always fetch the latest release; keep them free of fallbacks and
options.

## Dependencies

Renovate (`renovate.json`) updates every pinned version: crates, Bazel and its modules, the Rust toolchain, and
GitHub Actions. `tools/multitool.lock.json` pins nfpm; update its URLs and checksums together. Do not pin versions
anywhere else it cannot see.
