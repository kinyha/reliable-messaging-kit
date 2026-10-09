#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
exec python3 "$PERF_ROOT/perf-harness/scripts/profile.py" "$@"
