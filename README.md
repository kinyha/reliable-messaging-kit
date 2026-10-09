# Reliable Messaging Kit

Spring Boot стартер для **Transactional Outbox** и **Idempotent Consumer** в PostgreSQL + Kafka.
Бизнес-изменение и событие записываются в одной транзакции; релэй отправляет событие после коммита,
а inbox предотвращает повторный эффект в транзакции потребителя.

## Статус

Этап 1 реализован: публикатор, claim/send/ack, аренды и fencing, повторы с джиттером, DEAD,
метрики, inbox, аннотация потребителя и демонстрационные сервисы. Проверки и реальные результаты:
[`docs/demo-1-results.md`](docs/demo-1-results.md). Этап 2 ведётся в отдельной ветке `stage-2`: perf harness, virtual threads под флагом,
Toxiproxy, CI-гейт и одна оптимизация чтения пустых headers. [Текущий итог](docs/perf/final-report.md)
разделяет завершённые проверки и ещё не подтверждённые performance-критерии.

## Быстрый старт демо

Нужны **JDK 21 и работающий Docker с Compose**. Интеграционные тесты запускают настоящие
PostgreSQL и Kafka через Testcontainers; без Docker сборка не считается полной.

```bash
./gradlew build
make infra-up
# В двух терминалах:
make run-order
make run-payment
# В третьем:
curl -X POST localhost:8081/orders -H 'content-type: application/json' \
  -d '{"customerId":"c-1","total":42.50}'
make demo-reconcile
```

Сервисы слушают 8081/8082, PostgreSQL — 5433/5434, Kafka — 9092.
[Grafana](http://localhost:3000/d/reliable-messaging), [Prometheus](http://localhost:9090),
[Jaeger](http://localhost:16686). Grafana provisioned автоматически; демо использует локальный
анонимный доступ. Остановить инфраструктуру: `make infra-down`.

## Подключение за пять минут

В этом Gradle-проекте:

```kotlin
implementation(project(":outbox-spring-boot-starter"))
implementation("org.springframework.boot:spring-boot-starter-jdbc")
runtimeOnly("org.postgresql:postgresql")
```

Для отдельного проекта можно установить артефакт локально:
`./gradlew :outbox-spring-boot-starter:publishToMavenLocal`, добавить `mavenLocal()` и
`implementation("dev.reliablemessaging:outbox-spring-boot-starter:0.1.0-SNAPSHOT")`.
Публичный Maven-репозиторий пока не опубликован.

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5433/orders
    username: orders
    password: orders
  kafka:
    bootstrap-servers: localhost:9092
outbox:
  inbox:
    retention: 30d
```

`outbox.inbox.retention` обязателен. Он должен покрывать всё разрешённое окно повторной
доставки и ручного replay, включая Kafka retention. После удаления inbox-записи replay снова
применит эффект. Стартер не может выбрать безопасное значение за приложение.

Отправитель — внутри существующей бизнес-транзакции:

```java
@Transactional
public UUID place(String customerId, BigDecimal total) {
    var id = UUID.randomUUID();
    jdbc.update("insert into orders(id, customer_id, total) values (?, ?, ?)", id, customerId, total);
    outbox.publish(OutboxMessage.builder()
            .topic("orders.v1")
            .aggregate(new Aggregate("order", id.toString()))
            .payload(new OrderPlaced(id, customerId, total, Instant.now()))
            .build());
    return id;
}
```

Получатель — с JSON-конвертером `StringJsonMessageConverter` и десериализаторами строк Kafka:

```java
@IdempotentConsumer(name = "payment-on-order-placed")
@KafkaListener(topics = "orders.v1", groupId = "payment-service")
public void consume(OrderPlaced event, MessageMeta meta) {
    jdbc.update("insert into payments(order_id, amount) values (?, ?)", event.orderId(), event.total());
}
```

Исполняемые примеры — `demo-stand/order-service` и `demo-stand/payment-service`.
Программное API получателя: `IdempotentExecutor.execute(consumer, messageId, action)`.
Имя consumer — стабильная часть составного ключа inbox; изменение имени разрешает повторную обработку.

Публикатор отвергает вызов без транзакции своего DataSource. Prefix `rm-` зарезервирован:
стартер передаёт `rm-message-id`, `rm-aggregate-type`, `rm-aggregate-id`, `rm-occurred-at`.
Пользовательские заголовки — строки UTF-8. При наличии Micrometer Tracer/Propagator текущий
trace-контекст сохраняется в outbox и продолжается Kafka-потребителем.

Миграции стартера имеют отдельную историю `reliable_messaging_schema_history`, поэтому не
конфликтуют с `V1` приложения. Изначальная V1 не изменена; `claim_token` добавляет V2.
При переходе со старого каркаса, где V1 стартера уже записана в общую историю Flyway, требуется
отдельный перенос истории миграций с сохранением данных. Автоматического repair/удаления схемы
стартер не выполняет. Локальное демо проверялось на чистых томах проекта; предыдущие базы сохранены
SQL-дампами в `demo-stand/.run/pre-stage1-*.sql`.

## Гарантии и границы

| Свойство | Гарантия |
|---|---|
| Бизнес-изменение + outbox | Атомарны в одной транзакции PostgreSQL; откат удаляет оба |
| Доставка | At-least-once при доступных зависимостях и оставшемся бюджете попыток; дубли ожидаемы |
| Эффект потребителя | Один раз для того же message_id и consumer, в той же транзакции PostgreSQL и в пределах retention inbox |
| Истёкшая аренда | Reaper возвращает строку; старый ack отсекается claim_token |
| Порядок | Не гарантируется, даже внутри агрегата; обработчики должны быть коммутативными |
| Внешний HTTP/письмо/другая БД | Однократность не обеспечивается; нужен ключ идемпотентности внешнего получателя |
| DEAD | Наблюдаемая недоставка с last_error; дальнейшая доставка требует ручного разбора |
| Replay старше inbox retention | Может повторить эффект |

`MessageMeta` содержит идентификатор, время, агрегат и topic/partition/offset.
Порядкового номера агрегата нет; время события не заменяет бизнес-версию.

Продюсер релэя использует собственный KafkaTemplate с `acks=all` и producer idempotence.
Эти настройки не превращают переход PostgreSQL → Kafka в exactly-once.
Операторские DEAD-строки не удаляются автоматически; DLT потребителя `orders.v1.DLT` — отдельный
путь обработки ошибок демо, а не автоматическое перемещение DEAD из outbox в Kafka.

## Конфигурация

| Свойство outbox.* | По умолчанию | Смысл |
|---|---|---|
| migrations.enabled | true | Применять миграции стартера |
| relay.enabled | true | Запускать релэй и reaper |
| relay.workers | 2 | Число долгоживущих воркеров в обоих режимах |
| relay.virtual-threads | false | Виртуальная фабрика потоков с тем же claim/send/ack protocol |
| relay.fast-empty-headers | false | Обход JSON parser только для точного пустого JSONB объекта `{}` |
| relay.batch-size | 128 | Максимальная пачка claim |
| relay.poll-interval | 200ms | Пауза при неполной пачке или ошибке |
| relay.lease-duration | 30s | Срок аренды |
| relay.send-timeout | 10s | Общий таймаут пачки; строго меньше аренды |
| relay.reaper-interval | 10s | Частота возврата истёкших аренд |
| relay.max-attempts | 12 | Попытка расходуется при claim, в том числе при падении процесса |
| relay.backoff-base | 1s | База экспоненциальной задержки |
| relay.backoff-max | 5m | Верхняя граница задержки; equal jitter от половины до полной экспоненты |
| cleanup.retention | 7d | Хранение SENT после sent_at |
| cleanup.interval | 1m | Частота очистки |
| cleanup.batch-size | 1000 | Размер отдельного DELETE |
| metrics.refresh-interval | 5s | Частота запроса снимка; scrape не обращается к БД |
| inbox.retention | **нет** | Обязательный срок дедупликации |

Дефолты этапа 2 сохраняют `workers=2`, `batch-size=128`, `poll-interval=200ms` и
`virtual-threads=false`. Полная сетка H3 на300rps даёт для128/200 e2e p99 median253ms и
18,7 claim/ack SQL/s; ускорение poll до50ms уменьшает задержку ценой примерно3,65× SQL.
Batch512 при200ms не снижает SQL. В H2 virtual не дал устойчивого выигрыша: e2e median
−3,13% при Hikari10 и +1,54% при20, с сохранённым выбросом5,7s. Увеличение Hikari не
обосновано средним L≈0,6 занятого соединения. Три двухчасовых H5 прогона не показали
неограниченного роста таблицы на100rps: дополнительная миграция V3 не нужна для этого режима.
Полные данные и границы выводов — [`docs/perf/tuning.md`](docs/perf/tuning.md).

`fast-empty-headers` — единственная внутренняя оптимизация, выбранная по alloc flamegraph.
Флаг остаётся выключенным, пока сравнительные прогоны не оценят эффект; headers с trace
контекстом проходят обычный decoder. Протокол доставки и старые программные конструкторы
сохраняются. Предсказание и результаты — [`docs/perf/optimization.md`](docs/perf/optimization.md).

Числа и интервалы должны быть положительными; `backoff-base <= backoff-max`.
В приложении-получателе можно выключить relay, оставив inbox и очистку.
Диспетчер допускает замену пользовательским бином `OutboxDispatcher`.

## Метрики

Имена Micrometer ниже; Prometheus заменяет точки подчёркиваниями, счётчики получают `_total`,
таймер — `_seconds_count`/`_seconds_sum`.

| Метрика | Тип / метки |
|---|---|
| outbox.messages.pending | gauge: NEW + FAILED |
| outbox.messages.in_flight | gauge |
| outbox.messages.dead | gauge; алерт >0 в течение 30s |
| outbox.oldest_pending.age | gauge, секунды |
| outbox.relay.claimed | counter |
| outbox.relay.published | counter, result=sent/failed/dead |
| outbox.relay.reclaimed | counter |
| outbox.relay.fenced | counter |
| outbox.relay.dispatch | timer |
| outbox.cleanup.deleted | counter, table=outbox/inbox |
| inbox.messages | counter, consumer и result=processed/duplicate/failed |

Для нескольких релэев над одной БД дашборд берёт max глубины очереди, а не сумму одинаковых
снимков. Счётчики публикаций складываются по экземплярам.

## Сценарии демо и проверки

```bash
make demo-rollback       # 100 откатов: нет заказов и событий
make demo-kill9-naive    # 500 rps, 30s: ожидаемая потеря dual-write
make demo-kill9          # тот же kill -9 с outbox: расхождение 0
make demo-three-relays   # истёкшие аренды, дубли доставок, один бизнес-эффект
make demo-replay         # после успешного сценария: доставок больше, платежей столько же
make demo-kafka-down     # полные 5 минут без Kafka, затем дренаж
make demo-reconcile      # exit 1 при потерях, дублях эффектов или незавершённом outbox
scripts/repeat-tests.sh 10
```

Сценарии собирают bootJar, запускают JVM, сохраняют PID/логи в `demo-stand/.run/` и останавливают
свои процессы после завершения. Каждый сценарий, кроме replay, сначала сбрасывает данные **только
демо-баз проекта** и топики orders.v1/DLT. Убийство процесса намеренно вызывает HTTP-ошибки;
сверка использует только реально закоммиченные заказы. Неизменённое состояние инфраструктуры
сохраняется до `make infra-down`. Глобальный `docker volume prune` не используется.

Для короткого репетиционного отключения: `demo-stand/scripts/scenario-kafka-down.sh 1`.
Критерий приёмки проверяется полным пяти­минутным запуском.

## Документы

- [`docs/design.html`](docs/design.html) — архитектурный дизайн-документ: постановка задачи,
  модель данных, 5 ADR, жизненный цикл сообщения, публичный API, план обоих этапов,
  текст для карточки IVP.
  Опубликовано: <https://claude.ai/code/artifact/cf25043d-221e-45f1-8d40-50544cdd6f8d>
- [`docs/plan.md`](docs/plan.md) — понедельный план на 8 недель: записи для лога по дням
  (40 ч/неделю), ядро каждой недели, критерии закрытия, правки спеки по итогам ревью, stretch goals.
  **Рабочий формат** — отсюда копируется в задачу PM.
- [`docs/plan.html`](docs/plan.html) — тот же план, оформленный для чтения и показа.
  Опубликовано: <https://claude.ai/code/artifact/f04be4c7-0fc8-47b0-b1ee-3bc6176a0087>

- [`docs/implementation-plan.md`](docs/implementation-plan.md) — задание для агента-исполнителя:
  задачи T2.0–T4.5 от текущего каркаса до демо этапа 1, с SQL, тестами и критериями приёмки.
- [`docs/implementation-plan-stage-2.md`](docs/implementation-plan-stage-2.md) — то же для этапа 2,
  делается в отдельной ветке `stage-2` после тега `stage-1-demo`.
- [`docs/guide.html`](docs/guide.html) — учебник по проекту: Kafka, PostgreSQL, outbox/inbox,
  устройство стартера, этапы.

> Две версии плана держатся синхронно вручную. При правках менять обе — `plan.md` первым
> как источник истины, `plan.html` следом (и переопубликовывать по URL выше).

### Что в репозитории, а что нет

- `docs/` — проектная документация: как устроен стартер. Коммитится.
- `private/` — отчётность: описание для PM, записи для лога часов. **Исключено из git**
  через `.gitignore`, см. `private/README.md`.

Правило: если файл объясняет, *как устроен стартер* — он в `docs/`; если *как отчитаться
о работе над ним* — в `private/`.

### Статус документации

Дизайн-документ доведён до **v1.2**: закрыты все семь дефектов, найденных на ревью версии 1.0
(протокол claim/lease, обещание порядка, ADR‑5 про bloom-фильтр, противоречие в критериях
готовности, `inbox.retention`, границы однократности, переобещание в `DEAD`). Список правок —
в changelog v1.1 и v1.2 в начале документа.

> При обновлении документа переопубликовывать нужно **с явным указанием этого URL**.
> Файл переехал из корня `~/git` в `docs/`, а артефакт привязан к пути публикации —
> публикация нового пути без `url` создаст отдельный артефакт вместо обновления существующего.

## Структура

- `outbox-spring-boot-starter/`: API, публикатор, релэй, inbox, метрики, очистка и изолированные миграции.
- `demo-stand/`: два приложения, PostgreSQL, Kafka, Prometheus, Grafana, Jaeger, сценарии и сверка.
- `docs/demo-1-runbook.md`: показ на 15 минут; `docs/demo-1-results.md`: проверенные результаты.
- `scripts/repeat-tests.sh`: повторная проверка стартера.
- `perf-harness/` появится на этапе 2.

Java 21 · Spring Boot 3.5 · PostgreSQL 16 · Kafka · Flyway · Micrometer · Testcontainers · k6 · OpenTelemetry
