#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
"$(dirname "$0")/reset.sh"
trap stop_all EXIT
start_order 8081
start_payment
for ((i=0;i<100;i++)); do
  code=$(curl -sS --max-time 10 -o /dev/null -w '%{http_code}' -X POST 'http://localhost:8081/orders?fail=true' \
    -H 'content-type: application/json' -d '{"customerId":"rollback","total":10}')
  [[ "$code" == 500 ]] || { echo "Expected rollback HTTP 500, got $code" >&2; exit 1; }
done
[[ $(psql_order 'select count(*) from orders') == 0 ]]
[[ $(psql_order 'select count(*) from outbox_message') == 0 ]]
wait_drained 60
"$(dirname "$0")/reconcile.sh"
