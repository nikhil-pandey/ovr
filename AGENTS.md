# AGENTS.md

`ovr` runs a command on another machine against the local working tree. [docs/DESIGN.md](docs/DESIGN.md) is the
source of truth for behaviour; read it before changing anything user-visible.

## Layout

- `crates/ovr`: the CLI. Snapshots the tree and hands the run to the local `ovrd`.
- `crates/ovrd`: the server. Initiator-side host connections, and the agent that runs on hosts.
- `crates/ovr-proto`: what both binaries share: the wire format, running `git`, where files live, and finding and
  starting the local `ovrd`.

## Principles

- Keep it minimal. No configuration files, no environment variables, no flags that a fixed default can replace.
- Keep the README short: what it is, one example, a link to the design. Details belong in `docs/DESIGN.md`, not the
  README.
- Update `docs/DESIGN.md` in the same change when behaviour changes.

## Rust

- Clean, idiomatic Rust. Small modules, plain types, no clever abstractions before a second use exists.
- Versions, lints, and dependency versions live in the root `Cargo.toml`; members use `workspace = true` and pick
  features locally. Bazel reads the same manifests and `Cargo.lock` through `crate_universe`; never pin a crate in
  Bazel files.
- Cargo and Bazel both build everything. A new crate needs a `BUILD.bazel` modelled on an existing one.
- Errors are `thiserror` enums rendered with `miette`. No `unwrap`, `expect`, or `panic!` outside tests.
- `main.rs` only parses arguments, renders errors, and picks the exit code. Behaviour lives in the library.
- `ovr` exits with the remote command's code, or `125` for its own failures.

## Validate

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
bazel test //...
```

The client tests in `crates/ovr/src/session.rs` run a real `ovrd` agent in-process against temporary repositories
and need `git` on `PATH`. `crates/ovr/tests/e2e.rs` runs the real `ovr` and `ovrd` binaries on Unix, with a stand-in
`ssh` that serves a second home directory as the host, so runs are kept by real `ovrd keep` processes; under Cargo
it builds `ovrd` itself, under Bazel it gets both binaries as data. Platform-specific code (Windows Job Objects,
ConPTY, named pipes) is only compiled on its platform; CI checks every released OS.

## Release

Use [Conventional Commits](https://www.conventionalcommits.org); release-plz turns them into a release PR that bumps
the version and `CHANGELOG.md`. Merging that PR tags `v<version>`, and GoReleaser (`.goreleaser.yaml`) cross-compiles
every target with cargo-zigbuild and publishes archives, `.deb`/`.rpm` packages, and the install scripts. CI runs the
same build as a snapshot and installs each artifact on its OS. Never bump versions by hand. The install scripts
always fetch the latest release; keep them free of fallbacks and options.

## Dependencies

Renovate (`renovate.json`) updates every pinned version: crates, Bazel, `rules_rust`, the Rust toolchain, and
GitHub Actions. Do not pin versions anywhere it cannot see.
