#!/usr/bin/env bash
# Installs cloudflared, at the version in CLOUDFLARED_VERSION, onto this runner's PATH.
set -euo pipefail
bin="$RUNNER_TEMP/bin"
base="https://github.com/cloudflare/cloudflared/releases/download/$CLOUDFLARED_VERSION"
mkdir -p "$bin"
case $RUNNER_OS in
  Linux) curl -fsSL --retry 5 -o "$bin/cloudflared" "$base/cloudflared-linux-amd64" && chmod +x "$bin/cloudflared" ;;
  macOS) curl -fsSL --retry 5 "$base/cloudflared-darwin-arm64.tgz" | tar -xz -C "$bin" ;;
  Windows) curl -fsSL --retry 5 -o "$bin/cloudflared.exe" "$base/cloudflared-windows-amd64.exe" ;;
esac
echo "$bin" >>"$GITHUB_PATH"
"$bin/cloudflared" --version
