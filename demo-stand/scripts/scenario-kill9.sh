#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
mode=${1:-outbox}
[[ "$mode" == outbox || "$mode" == naive ]] || exit 2
"$(dirname "$0")/reset.sh"
trap stop_all EXIT
args=("--demo.delivery-mode=$mode")
if [[ "$mode" == naive ]]; then args+=(--outbox.relay.enabled=false); export RMK_ALLOW_LOSS=true; fi
start_order 8081 "${args[@]}"
start_payment
RMK_EXPECT_HTTP_FAILURES=true "$(dirname "$0")/load.sh" 500 30 > "$RMK_RUN/load-kill9-$mode.log" 2>&1 & load_pid=$!
sleep 10
kill9 "$(cat "$RMK_RUN/order-8081.pid")"
start_order 8081 "${args[@]}"
wait "$load_pid"
wait_drained 300
if "$(dirname "$0")/reconcile.sh"; then
  [[ "$mode" == outbox ]] || { echo 'Naive run did not expose a loss; repeat the scenario.' >&2; exit 1; }
else
  [[ "$mode" == naive ]] || exit 1
  orders=$(psql_order 'select count(*) from orders')
  paid=$(psql_payment 'select count(distinct order_id) from payments')
  ((orders>paid)) || { echo 'Expected missing naive deliveries.' >&2; exit 1; }
  echo 'Expected naive dual-write loss reproduced.'
fi
