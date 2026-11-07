#!/usr/bin/env bash
# Syncs each repository in REPOS to HOST through `ovr` and builds it there, appending a Markdown table row per step
# to OUT. Exits non-zero if any step failed.
#
#   builds.sh HOST REPOS OUT
set -uo pipefail
host=$1 repos=$2 out=$3
status=0

step() {
  local label=$1 dir=$2
  shift 2
  local start=$SECONDS code=0
  echo "::group::$label"
  (cd "$repos/$dir" && ovr "$host" -- "$@" </dev/null) || code=$?
  echo "::endgroup::"
  echo "| $label | $((SECONDS - start)) s | $code |" >>"$out"
  ((code == 0)) || status=1
}

# An untracked file proves the working tree is sent, not just what is committed.
echo "$GITHUB_RUN_ID" >"$repos/ripgrep/ovr-e2e-untracked"
step "ripgrep: untracked file arrives" ripgrep git hash-object ovr-e2e-untracked
step "ripgrep: cargo build" ripgrep cargo build
step "cargo: cargo build" cargo cargo build
step "tokio: cargo build" tokio cargo build
step "bazel examples: bazel build" examples/cpp-tutorial/stage3 bazel build //main:hello-world
# Nothing changed, so this measures a warm sync and an up-to-date build.
step "ripgrep: cargo build again" ripgrep cargo build
exit $status
