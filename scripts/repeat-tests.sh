#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
count=${1:-10}
if ! [[ "$count" =~ ^[1-9][0-9]*$ ]]; then echo "Usage: $0 N (positive integer)" >&2; exit 2; fi
logs=outbox-spring-boot-starter/build/repeat-tests
mkdir -p "$logs"
for ((run=1;run<=count;run++)); do
  echo "[$run/$count] Full starter test suite, including PostgreSQL and Kafka"
  if ./gradlew :outbox-spring-boot-starter:test --rerun-tasks > "$logs/run-$run.log" 2>&1; then
    echo "[$run/$count] PASS (${SECONDS}s elapsed)"
  else
    cat "$logs/run-$run.log"
    echo "FAIL on run $run; logs: $logs" >&2
    exit 1
  fi
done
echo "PASS: $count consecutive runs; ${SECONDS}s; logs: $logs"
