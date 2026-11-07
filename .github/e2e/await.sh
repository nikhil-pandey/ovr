#!/usr/bin/env bash
# Waits for another job in this workflow run to upload artifact NAME, then downloads it into DIR.
# Gives up after SECONDS, or as soon as process PID, if given, has exited.
#
#   await.sh NAME DIR SECONDS [PID]
set -euo pipefail
name=$1 dir=$2 seconds=$3 pid=${4:-}
deadline=$((SECONDS + seconds))
until gh run download "$GITHUB_RUN_ID" --repo "$GITHUB_REPOSITORY" --name "$name" --dir "$dir" 2>/dev/null; do
  if [[ -n $pid ]] && ! kill -0 "$pid" 2>/dev/null; then
    echo "process $pid exited while waiting for artifact $name" >&2
    exit 1
  fi
  if ((SECONDS >= deadline)); then
    echo "artifact $name did not appear within ${seconds}s" >&2
    exit 1
  fi
  sleep 10
done
