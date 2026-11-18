#!/usr/bin/env bash
# Install this runner's release archive, from the CI build directory when supplied.
set -euo pipefail
bin=$1
case "$RUNNER_OS-$RUNNER_ARCH" in
  Linux-X64) target=x86_64-unknown-linux-musl ;;
  Linux-ARM64) target=aarch64-unknown-linux-musl ;;
  macOS-X64) target=x86_64-apple-darwin ;;
  macOS-ARM64) target=aarch64-apple-darwin ;;
  Windows-X64) target=x86_64-pc-windows-gnullvm ;;
  Windows-ARM64) target=aarch64-pc-windows-gnullvm ;;
  *) echo "no ovr build for $RUNNER_OS-$RUNNER_ARCH" >&2; exit 1 ;;
esac
ext=tar.gz
if [[ $RUNNER_OS == Windows ]]; then ext=zip; fi
name="ovr-$target.$ext"
mkdir -p "$bin"
if [[ $# == 2 ]]; then
  archive="$2/$name"
else
  release=https://github.com/nikhil-pandey/ovr/releases
  if [[ ${ACTION_REF:-} =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    release+=/download/$ACTION_REF
  else
    release+=/latest/download
  fi
  archive="$RUNNER_TEMP/$name"
  curl -fsSL --retry 3 -o "$archive" "$release/$name"
fi
if [[ $ext == zip ]]; then
  unzip -oq "$archive" -d "$bin"
else
  tar -xzf "$archive" -C "$bin"
fi
