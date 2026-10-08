#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
minutes=${1:-5}
[[ "$minutes" =~ ^[1-9][0-9]*$ ]] || exit 2
"$(dirname "$0")/reset.sh"
cleanup() { compose start kafka >/dev/null; stop_all; }
trap cleanup EXIT
# Enough attempts for a five minute outage even at the earliest possible equal-jitter delays.
start_order 8081 --outbox.relay.max-attempts=30
start_payment
"$(dirname "$0")/load.sh" 100 "$((minutes*60+30))" > "$RMK_RUN/load-kafka-down.log" 2>&1 & load_pid=$!
sleep 10
compose stop kafka
end=$((SECONDS+minutes*60))
while ((SECONDS<end)); do
  psql_order "select 'pending=' || count(*) || ' oldest_seconds=' || coalesce(extract(epoch from now()-min(created_at)),0) from outbox_message where status in ('NEW','FAILED')"
  sleep 10
done
compose start kafka
wait "$load_pid"
wait_drained 600
"$(dirname "$0")/reconcile.sh"
