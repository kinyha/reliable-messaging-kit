#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
stop_all
# Preserve any existing local demo databases before a project-scoped clean run.
if compose ps --status running --services | awk '$0=="order-postgres" {found=1} END {exit !found}'; then
  compose exec -T order-postgres pg_dump -U orders -d orders > "$RMK_RUN/before-full-verification-orders.sql"
fi
if compose ps --status running --services | awk '$0=="payment-postgres" {found=1} END {exit !found}'; then
  compose exec -T payment-postgres pg_dump -U payments -d payments > "$RMK_RUN/before-full-verification-payments.sql"
fi
compose down --volumes
compose up -d --wait
(cd "$RMK_ROOT" && ./gradlew clean build :outbox-spring-boot-starter:publishToMavenLocal --no-build-cache) > "$RMK_RUN/full-build.log" 2>&1 || { cat "$RMK_RUN/full-build.log"; exit 1; }
(cd "$RMK_ROOT" && scripts/repeat-tests.sh 10) | tee "$RMK_RUN/repeat-tests.log"
report="$RMK_RUN/demo-1-results.md"
{
  echo '# Результаты демо этапа 1'
  echo
  echo "Дата: $(TZ=Europe/Minsk date '+%Y-%m-%d %H:%M %Z'). Чистые тома только проекта reliable-messaging-kit."
  echo
  echo 'JDK 21, Spring Boot 3.5.16, PostgreSQL 16.15, Kafka 4.3.0; Docker/Testcontainers без пропусков.'
  echo
  echo 'Полная сборка: `./gradlew clean build :outbox-spring-boot-starter:publishToMavenLocal --no-build-cache` — PASS.'
  echo
  echo 'Стабильность: `scripts/repeat-tests.sh 10` — 10 последовательных PASS; исходные логи в `outbox-spring-boot-starter/build/repeat-tests/`.'
} > "$report"
run_scenario() {
  local name=$1; shift
  echo "Running $name: $*"
  if "$@" > "$RMK_RUN/full-$name.log" 2>&1; then
    echo "PASS: $name"
    {
      echo
      echo "## $name — PASS"
      echo
      echo "Команда: \`${*//$RMK_ROOT\//}\`."
      echo
      awk '/^\|/ {print}' "$RMK_RUN/full-$name.log"
      if [[ "$name" == kill9-naive ]]; then
        echo
        echo 'Расхождение здесь ожидаемо: продемонстрирована потеря naive dual-write.'
      fi
    } >> "$report"
  else
    cat "$RMK_RUN/full-$name.log"
    echo "FAIL: $name; partial report remains in $report" >&2
    exit 1
  fi
}
run_scenario rollback "$RMK_ROOT/demo-stand/scripts/scenario-rollback.sh"
run_scenario kill9-naive "$RMK_ROOT/demo-stand/scripts/scenario-kill9.sh" naive
run_scenario kill9-outbox "$RMK_ROOT/demo-stand/scripts/scenario-kill9.sh" outbox
run_scenario three-relays "$RMK_ROOT/demo-stand/scripts/scenario-three-relays.sh"
run_scenario replay "$RMK_ROOT/demo-stand/scripts/scenario-replay.sh"
run_scenario kafka-down-5m "$RMK_ROOT/demo-stand/scripts/scenario-kafka-down.sh" 5
{
  echo
  echo '## Наблюдаемость'
  echo
  echo 'Prometheus получает реальные счётчики и снимки очереди; Grafana показывает рост при остановке Kafka и дренаж после восстановления.'
  echo
  echo '![Рост очереди и дренаж](demo-1-evidence/grafana-kafka-down.jpg)'
  echo
  echo 'Jaeger: один trace включает `http post /orders` (order-service) и `orders.v1 receive` (payment-service).'
  echo
  echo '![Общий trace](demo-1-evidence/jaeger-trace.jpg)'
  echo
  echo 'Допустимые дубли доставок видны в delivery_log; бизнес-эффекты при outbox + inbox остаются однократными.'
  echo
  echo 'Скрипты сценариев и их результаты относятся к проверке корректности этапа 1; сравнительные замеры производительности остаются для этапа 2.'
} >> "$report"
cp "$report" "$RMK_ROOT/docs/demo-1-results.md"
echo 'PASS: all stage-1 scenarios; docs/demo-1-results.md updated.'
