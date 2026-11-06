#!/bin/sh
# Installs ovr and ovrd from the latest release into ~/.local/bin.
#
#   curl -fsSL https://raw.githubusercontent.com/nikhil-pandey/ovr/main/install.sh | sh
set -eu

case "$(uname -s)" in
Darwin) os=apple-darwin ;;
Linux) os=unknown-linux-musl ;;
*) echo "ovr: unsupported OS $(uname -s); on Windows use install.ps1" >&2 && exit 1 ;;
esac

case "$(uname -m)" in
x86_64 | amd64) arch=x86_64 ;;
arm64 | aarch64) arch=aarch64 ;;
*) echo "ovr: unsupported architecture $(uname -m)" >&2 && exit 1 ;;
esac

bin_dir="$HOME/.local/bin"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

curl -fsSL "https://github.com/nikhil-pandey/ovr/releases/latest/download/ovr-$arch-$os.tar.gz" | tar -xz -C "$tmp"
mkdir -p "$bin_dir"
install -m 755 "$tmp/ovr" "$tmp/ovrd" "$bin_dir/"
echo "installed ovr and ovrd to $bin_dir"
