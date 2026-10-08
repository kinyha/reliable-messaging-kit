#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
"$(dirname "$0")/reset.sh"
trap stop_all EXIT
start_payment
for port in 8081 8083 8085; do
  start_order "$port" --demo.chaos.publish-delay=3s --outbox.relay.lease-duration=2s \
    --outbox.relay.send-timeout=1s --outbox.relay.reaper-interval=200ms
done
"$(dirname "$0")/load.sh" 100 15 > "$RMK_RUN/load-three-relays.log" 2>&1
wait_drained 300
"$(dirname "$0")/reconcile.sh"
deliveries=$(psql_payment 'select count(*) from delivery_log')
unique=$(psql_payment 'select count(distinct message_id) from delivery_log')
((deliveries>unique)) || { echo 'No duplicate delivery observed.' >&2; exit 1; }
