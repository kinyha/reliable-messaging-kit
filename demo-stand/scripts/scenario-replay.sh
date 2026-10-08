#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
# Replay preserves the data from the preceding scenario.
stop_all
build_jars
before=$(psql_payment 'select count(*) from payments')
deliveries_before=$(psql_payment 'select count(*) from delivery_log')
((before>0)) || { echo 'Run a successful scenario before replay.' >&2; exit 1; }
kafka_tool kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group payment-service \
  --topic orders.v1 --reset-offsets --to-earliest --execute
trap stop_all EXIT
start_order 8081
start_payment
wait_drained 300
[[ $(psql_payment 'select count(*) from payments') == "$before" ]]
(( $(psql_payment 'select count(*) from delivery_log') > deliveries_before ))
"$(dirname "$0")/reconcile.sh"
