#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
psql_order 'select id from orders order by id' > "$RMK_RUN/orders.ids"
psql_payment 'select distinct order_id from payments order by order_id' > "$RMK_RUN/payments.ids"
missing=$(comm -23 "$RMK_RUN/orders.ids" "$RMK_RUN/payments.ids" | awk 'END {print NR}')
unexpected=$(comm -13 "$RMK_RUN/orders.ids" "$RMK_RUN/payments.ids" | awk 'END {print NR}')
orders=$(psql_order 'select count(*) from orders')
payments=$(psql_payment 'select count(*) from payments')
unique_payments=$(psql_payment 'select count(distinct order_id) from payments')
deliveries=$(psql_payment 'select count(*) from delivery_log')
unique_deliveries=$(psql_payment 'select count(distinct message_id) from delivery_log')
duplicates=$((payments-unique_payments))
pending=$(psql_order "select count(*) from outbox_message where status in ('NEW','FAILED','IN_FLIGHT','DEAD')")
printf '| Проверка | Значение |\n|---|---:|\n'
printf '| Заказы | %s |\n| Доставки | %s |\n| Уникальные message_id | %s |\n| Платежи | %s |\n| Уникальные order_id | %s |\n| Заказы без платежа | %s |\n| Платежи-дубли | %s |\n| Платежи без заказа | %s |\n' \
  "$orders" "$deliveries" "$unique_deliveries" "$payments" "$unique_payments" "$missing" "$duplicates" "$unexpected"
psql_order "select '| Outbox ' || status || ' | ' || count(*) || ' |' from outbox_message group by status order by status"
((orders==unique_payments && missing==0 && unexpected==0 && duplicates==0 && pending==0))
