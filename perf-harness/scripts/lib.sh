#!/usr/bin/env bash
set -euo pipefail
PERF_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
perf_compose() { docker compose -p rmk-perf -f "$PERF_ROOT/demo-stand/docker-compose.yml" -f "$PERF_ROOT/perf-harness/compose.perf.yml" "$@"; }
