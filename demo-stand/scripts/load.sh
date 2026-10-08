#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
rps=${1:?Usage: load.sh RPS SECONDS}; duration=${2:?Usage: load.sh RPS SECONDS}
[[ "$rps" =~ ^[1-9][0-9]*$ && "$duration" =~ ^[1-9][0-9]*$ ]] || exit 2
# kill -9 scenarios deliberately produce HTTP failures during restart; retain counts in the report.
args=()
if [[ "${RMK_EXPECT_HTTP_FAILURES:-false}" == true ]]; then args+=(--no-thresholds); fi
docker run --rm --add-host=host.docker.internal:host-gateway \
  -e "RPS=$rps" -e "DURATION=$duration" \
  -v "$RMK_ROOT/demo-stand/scripts/load.js:/scripts/load.js:ro" \
  grafana/k6:1.3.0 run ${args[@]+"${args[@]}"} /scripts/load.js
