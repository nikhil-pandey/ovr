#!/usr/bin/env bash
# Installs cloudflared, at the version in CLOUDFLARED_VERSION, onto this runner's PATH.
set -euo pipefail
bin="$RUNNER_TEMP/bin"
mkdir -p "$bin"
case $RUNNER_OS in
  Linux) asset=cloudflared-linux-amd64 ;;
  macOS) asset=cloudflared-darwin-arm64.tgz ;;
  Windows) asset=cloudflared-windows-amd64.exe ;;
esac
# Release downloads sometimes fail for minutes at a time; the API is a second way to the same asset.
curl -fsSL --retry 10 --retry-all-errors --retry-max-time 300 -o "$bin/$asset" \
  "https://github.com/cloudflare/cloudflared/releases/download/$CLOUDFLARED_VERSION/$asset" ||
  gh release download "$CLOUDFLARED_VERSION" --repo cloudflare/cloudflared --pattern "$asset" --dir "$bin" --clobber
case $asset in
  *.tgz) tar -xzf "$bin/$asset" -C "$bin" ;;
  *.exe) mv "$bin/$asset" "$bin/cloudflared.exe" ;;
  *) mv "$bin/$asset" "$bin/cloudflared" && chmod +x "$bin/cloudflared" ;;
esac
echo "$bin" >>"$GITHUB_PATH"
"$bin/cloudflared" --version
