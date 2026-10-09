#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
run_started=$(date -u +%Y%m%dT%H%M%SZ)
python3 "$PERF_ROOT/perf-harness/scripts/benchmark.py" --suite ci --runs 1 --name "$run_started" "$@"
measurement=$(python3 - "$PERF_ROOT/perf-harness/results" "$run_started" <<'PY'
import json,sys
from pathlib import Path
files=[p for p in Path(sys.argv[1]).glob('*.measurement.json') if sys.argv[2] in p.name and json.loads(p.read_text()).get('suite')=='ci']
if len(files)!=1:raise SystemExit('Expected exactly one fresh CI measurement')
print(files[0])
PY
)
python3 "$PERF_ROOT/perf-harness/scripts/check-gate.py" "$measurement"
