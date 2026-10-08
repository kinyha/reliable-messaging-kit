#!/usr/bin/env bash
set -euo pipefail
RMK_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
RMK_RUN="$RMK_ROOT/demo-stand/.run"
mkdir -p "$RMK_RUN"
compose() { docker compose -f "$RMK_ROOT/demo-stand/docker-compose.yml" "$@"; }
psql_order() { compose exec -T order-postgres psql -X -qAt -v ON_ERROR_STOP=1 -U orders -d orders -c "$1"; }
psql_payment() { compose exec -T payment-postgres psql -X -qAt -v ON_ERROR_STOP=1 -U payments -d payments -c "$1"; }
kafka_tool() { compose exec -T kafka "/opt/kafka/bin/$1" "${@:2}"; }
build_jars() { (cd "$RMK_ROOT" && ./gradlew :demo-stand:order-service:bootJar :demo-stand:payment-service:bootJar) > "$RMK_RUN/build.log" 2>&1 || { cat "$RMK_RUN/build.log"; return 1; }; }
managed_pid() {
  local pid=$1 command
  command=$(ps -p "$pid" -o command= 2>/dev/null || true)
  [[ "$command" == *"$RMK_ROOT/demo-stand/"*"/build/libs/"*".jar"* ]]
}
kill9() { if managed_pid "$1"; then kill -KILL "$1"; fi; }
stop_all() {
  local file pid deadline
  for file in "$RMK_RUN"/*.pid; do
    [[ -f "$file" ]] || continue
    pid=$(cat "$file")
    if managed_pid "$pid"; then
      kill -TERM "$pid" 2>/dev/null || true
      deadline=$((SECONDS+30))
      while managed_pid "$pid" && ((SECONDS<deadline)); do sleep 1; done
      if managed_pid "$pid"; then kill9 "$pid"; fi
    fi
    rm -f "$file"
  done
}
wait_health() {
  local port=$1 pid=$2 log=$3 deadline=$((SECONDS+60))
  while ((SECONDS<deadline)); do
    if curl -fsS --max-time 1 "http://localhost:$port/actuator/health" >/dev/null 2>&1; then return; fi
    if ! kill -0 "$pid" 2>/dev/null; then cat "$log" >&2; return 1; fi
    sleep 1
  done
  tail -60 "$log" >&2
  echo "Application on port $port did not become healthy" >&2
  return 1
}
start_order() {
  local port=$1; shift
  local log="$RMK_RUN/order-$port.log"
  nohup java -Xms128m -Xmx512m -jar "$RMK_ROOT/demo-stand/order-service/build/libs/order-service-0.1.0-SNAPSHOT.jar" \
    "--server.port=$port" "$@" > "$log" 2>&1 &
  local pid=$!
  echo "$pid" > "$RMK_RUN/order-$port.pid"
  wait_health "$port" "$pid" "$log"
  echo "order-service :$port PID=$pid" >&2
}
start_payment() {
  local log="$RMK_RUN/payment.log"
  nohup java -Xms128m -Xmx512m -jar "$RMK_ROOT/demo-stand/payment-service/build/libs/payment-service-0.1.0-SNAPSHOT.jar" \
    --server.port=8082 "$@" > "$log" 2>&1 &
  local pid=$!
  echo "$pid" > "$RMK_RUN/payment.pid"
  wait_health 8082 "$pid" "$log"
  echo "payment-service :8082 PID=$pid" >&2
}
wait_drained() {
  local timeout=${1:-300} deadline=$((SECONDS+${1:-300})) stable=0 previous=-1 pending payments orders dead
  while ((SECONDS<deadline)); do
    pending=$(psql_order "select count(*) from outbox_message where status in ('NEW','FAILED','IN_FLIGHT')")
    dead=$(psql_order "select count(*) from outbox_message where status='DEAD'")
    payments=$(psql_payment "select count(*) from payments")
    orders=$(psql_order "select count(*) from orders")
    if ((dead>0)); then echo "Outbox has $dead DEAD messages" >&2; return 1; fi
    if ((pending==0 && payments==previous)); then stable=$((stable+1)); else stable=0; fi
    # Outbox mode also waits for all business effects, avoiding a false drain during Kafka rebalancing.
    if ((stable>=5)) && { [[ "${RMK_ALLOW_LOSS:-false}" == true ]] || ((payments>=orders)); }; then return; fi
    previous=$payments
    sleep 1
  done
  echo "Drain timed out after ${timeout}s: pending=$pending orders=$orders payments=$payments" >&2
  return 1
}
