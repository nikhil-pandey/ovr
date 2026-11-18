#!/usr/bin/env bash
# Install one provider CLI into the given directory on a GitHub runner.
set -euo pipefail
provider=$1
bin=$2
case "$provider" in
  cloudflare) cli=cloudflared ;;
  tailcat|devtunnel) cli=$provider ;;
  *) echo "unknown provider: $provider" >&2; exit 1 ;;
esac
if command -v "$cli" >/dev/null; then exit 0; fi
mkdir -p "$bin"
if [[ -x "$bin/$cli" || -x "$bin/$cli.exe" ]]; then exit 0; fi
case "$RUNNER_ARCH" in
  X64) arch=amd64 ;;
  ARM64) arch=arm64 ;;
  *) echo "unsupported runner architecture: $RUNNER_ARCH" >&2; exit 1 ;;
esac
case "$provider" in
  tailcat)
    if [[ $RUNNER_OS == macOS ]]; then exec brew install tailcat; fi
    # renovate: datasource=github-releases depName=tailscale/tailcat
    version=v0.7.0
    repo=tailscale/tailcat
    case "$RUNNER_OS" in
      Linux) asset="tailcat_${version#v}_linux_$arch.tar.gz" ;;
      Windows) asset="tailcat_${version#v}_windows_$arch.zip" ;;
      *) exit 1 ;;
    esac ;;
  cloudflare)
    # renovate: datasource=github-releases depName=cloudflare/cloudflared
    version=2026.10.7
    repo=cloudflare/cloudflared
    case "$RUNNER_OS" in
      Linux) asset="cloudflared-linux-$arch" ;;
      macOS) asset="cloudflared-darwin-$arch.tgz" ;;
      Windows) asset=cloudflared-windows-amd64.exe ;;
      *) exit 1 ;;
    esac ;;
  devtunnel)
    base=https://aka.ms/TunnelsCliDownload
    case "$RUNNER_OS" in
      Linux) url="$base/linux-${arch/amd64/x64}"; asset=devtunnel ;;
      macOS) url="$base/osx-${arch/amd64/x64}-zip"; asset=devtunnel.zip ;;
      Windows) url="$base/win-x64"; asset=devtunnel.exe ;;
      *) exit 1 ;;
    esac ;;
esac

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
if [[ $provider == devtunnel ]]; then
  curl -fsSL --retry 3 -o "$tmp/$asset" "$url"
else
  curl -fsSL --retry 3 -o "$tmp/$asset" "https://github.com/$repo/releases/download/$version/$asset" ||
    gh release download "$version" --repo "$repo" --pattern "$asset" --dir "$tmp" --clobber
fi
dst=$cli
if [[ $RUNNER_OS == Windows ]]; then dst+=.exe; fi
case "$asset" in
  *.tgz|*.tar.gz) tar -xzf "$tmp/$asset" -C "$tmp"; file=$dst ;;
  *.zip) unzip -oq "$tmp/$asset" -d "$tmp"; file=$dst ;;
  *) file=$asset ;;
esac
chmod +x "$tmp/$file"
mv "$tmp/$file" "$bin/$dst"
