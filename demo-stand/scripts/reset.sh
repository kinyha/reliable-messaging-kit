#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
stop_all
compose up -d --wait
build_jars
# Initialize schemas even on the very first run, with relays and listeners stopped.
start_order 8081 --outbox.relay.enabled=false --spring.kafka.admin.auto-create=false
start_payment --spring.kafka.listener.auto-startup=false --spring.kafka.admin.auto-create=false
stop_all
psql_order 'truncate orders,outbox_message,inbox_message restart identity'
psql_payment 'truncate payments,delivery_log,outbox_message,inbox_message restart identity'
kafka_tool kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group payment-service >/dev/null 2>&1 || true
for topic in orders.v1 orders.v1.DLT; do
  kafka_tool kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$topic" --if-exists >/dev/null
  for ((attempt=0;attempt<20;attempt++)); do
    topics=$(kafka_tool kafka-topics.sh --bootstrap-server localhost:9092 --list)
    if ! printf '%s\n' "$topics" | awk -v topic="$topic" '$0==topic {found=1} END {exit !found}'; then break; fi
    sleep 1
  done
  kafka_tool kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$topic" --partitions 6 --replication-factor 1 >/dev/null
done
echo 'Demo data and consumer offsets reset.'
