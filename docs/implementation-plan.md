# План реализации этапа 1 — для исполнителя-агента

Этот документ — рабочее задание для модели, которая пишет код. Он описывает путь от текущего
каркаса до проведённого демо этапа 1: какие файлы создать, что в них должно быть, как проверить,
что задача закрыта, и каким коммитом её зафиксировать.

Этап 2 описан отдельно в [`implementation-plan-stage-2.md`](implementation-plan-stage-2.md)
и делается **в отдельной ветке `stage-2`**, только после того как этап 1 закрыт и помечен тегом.

---

## 0. Прочитать до первой строчки кода

| Источник | Зачем |
|---|---|
| [`docs/design.html`](design.html) § 3–8 | Протокол, схема, ADR, API. Это спецификация. Если план и design расходятся — прав design, кроме пунктов из раздела 1 ниже. |
| [`docs/plan.md`](plan.md), недели 2–4 | Что считается «ядром недели» и критерием закрытия. |
| `outbox-spring-boot-starter/src/**` | Уже готовые `OutboxMessage`, `Aggregate`, `OutboxProperties`, миграция `V1`. Не переписывать без причины. |
| `demo-stand/docker-compose.yml` | Окружение: Postgres для заказов на `5433`, для платежей на `5434`, Kafka на `9092`. |

### Где мы сейчас

Готово: design v1.1, Gradle-модули, docker-compose, миграция `V1__create_messaging_tables.sql`,
публичные типы `OutboxMessage` / `Aggregate` / `OutboxPublisher` (только интерфейс),
`OutboxProperties`, автоконфигурация, которая подключает миграции стартера.

Не готово: всё остальное — реализация публикации, релэй, reaper, повторы, DEAD, метрики,
inbox и `@IdempotentConsumer`, бизнес-логика демо-сервисов, сценарии сбоя, дашборд, README.

### Правила работы

1. **Одна задача — один коммит.** Формат сообщения: `T2.3: Claim step with SKIP LOCKED`.
   Работа идёт в `main` маленькими коммитами (или в короткоживущих ветках `t2.3-claim`, сливаемых в `main`).
2. **После каждой задачи `./gradlew build` зелёный.** Интеграционные тесты требуют Docker.
3. **Не менять архитектурные решения.** ADR‑1…ADR‑5 зафиксированы. Если по ходу выясняется,
   что решение не работает — остановиться, записать проблему в `docs/implementation-notes.md`
   (раздел «Открытые вопросы») и сделать минимально безопасный вариант, явно помеченный `TODO(design)`.
4. **Отклонения фиксировать.** Любое отличие от design или от этого плана — строка в
   `docs/implementation-notes.md`: что, почему, где в коде.
5. **В стартер не попадает слово `order`/`payment`.** Всё доменное — только в `demo-stand`.
6. **Не трогать `private/`.** Это отчётность, к коду отношения не имеет.
7. **Комментарии и Javadoc — на английском**, как в существующем коде. Документы в `docs/` и README — на русском.
8. Стиль кода — как в существующих файлах: `record` для данных, конструкторная инъекция,
   никаких Lombok, `var` где тип очевиден, `JdbcTemplate`/`NamedParameterJdbcTemplate` (без JPA в стартере).

---

## 1. Известные расхождения с design v1.1 — решить в рамках плана

| # | Проблема | Решение в этом плане |
|---|---|---|
| D1 | **Коллизия версий Flyway.** Сейчас миграции стартера добавляются в общий Flyway приложения. Стартер занимает `V1`, а у любого приложения тоже есть `V1__…` — Flyway упадёт с «Found more than one migration with version 1». Демо-сервисам свои таблицы понадобятся уже в T4.1. | T2.0: стартер гоняет свои миграции **отдельным экземпляром Flyway** со своей таблицей истории `reliable_messaging_schema_history`. |
| D2 | **Фенсинг подтверждения.** Если аренда истекла и строку забрал другой воркер, первый воркер не должен своим ack перезаписать чужой захват. В схеме нет поля, которое это различает. | T2.0: миграция `V2` добавляет `claim_token uuid`. Ack обновляет строку только при совпадении токена. |
| D3 | ADR‑3 обещает передавать в обработчик «порядковый номер события внутри агрегата». Монотонный номер при конкурентной вставке требует отдельного механизма (sequence на агрегат или advisory lock). | На этапе 1 **не реализуется**. `MessageMeta` содержит `messageId`, `occurredAt`, агрегат, топик/партицию/оффсет. Записать в `implementation-notes.md` и в changelog design как v1.2. |
| D4 | В design § 10 строка рисков «явная группировка пачки по partition_key» — пережиток v1.0, противоречит ADR‑3 v1.1. | В T4.4 поправить строку в `design.html`: «порядок не гарантируется, см. ADR‑3». |
| D5 | Grafana в compose есть, а источника метрик нет: нет Prometheus и нет `micrometer-registry-prometheus`. | T4.3 добавляет Prometheus и provisioning Grafana. |

---

## 2. Целевая структура кода

```
outbox-spring-boot-starter/src/main/java/dev/reliablemessaging/outbox/
├── api/
│   ├── Aggregate.java                 (есть)
│   ├── OutboxMessage.java             (есть)
│   ├── OutboxPublisher.java           (есть)
│   ├── MessageHeaders.java            имена Kafka-заголовков стартера
│   ├── MessageMeta.java               метаданные для обработчика
│   └── IdempotentConsumer.java        аннотация
├── publisher/
│   ├── JdbcOutboxPublisher.java
│   └── PayloadSerializer.java
├── relay/
│   ├── OutboxRecord.java              строка outbox, прочитанная релэем
│   ├── OutboxRepository.java          весь SQL стороны отправителя
│   ├── OutboxDispatcher.java          интерфейс «отправить пачку» (оговорка ADR‑1)
│   ├── DispatchResult.java
│   ├── KafkaOutboxDispatcher.java
│   ├── RetryBackoff.java
│   ├── OutboxRelay.java               воркеры, SmartLifecycle
│   └── LeaseReaper.java
├── inbox/
│   ├── InboxRepository.java
│   ├── IdempotentExecutor.java        программное API: execute(consumer, messageId, action)
│   ├── IdempotentConsumerAspect.java
│   └── MessageMetaArgumentResolver.java
├── cleanup/
│   └── RetentionCleaner.java
├── metrics/
│   └── OutboxMetrics.java
└── config/
    ├── OutboxProperties.java          (есть, расширить)
    ├── ReliableMessagingAutoConfiguration.java   (есть: миграции → переделать в T2.0)
    ├── OutboxPublisherAutoConfiguration.java
    ├── OutboxRelayAutoConfiguration.java
    └── InboxAutoConfiguration.java
```

Все новые автоконфигурации регистрируются в
`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.

### Имена заголовков Kafka (`MessageHeaders`)

| Константа | Значение | Откуда |
|---|---|---|
| `MESSAGE_ID` | `rm-message-id` | `outbox_message.message_id` |
| `AGGREGATE_TYPE` | `rm-aggregate-type` | `aggregate_type` |
| `AGGREGATE_ID` | `rm-aggregate-id` | `aggregate_id` |
| `OCCURRED_AT` | `rm-occurred-at` | `created_at`, ISO‑8601 |

Пользовательские заголовки из `OutboxMessage.headers()` идут как есть, значения — UTF‑8 байты.
Префикс `rm-` зарезервирован: `OutboxMessage.Builder.header()` должен отвергать имена с этим префиксом
(добавить проверку и тест).

### Итоговые свойства конфигурации

```yaml
outbox:
  migrations:
    enabled: true
  relay:
    enabled: true
    workers: 2
    batch-size: 128
    poll-interval: 200ms
    lease-duration: 30s
    send-timeout: 10s        # новое; обязано быть < lease-duration, проверка при старте
    reaper-interval: 10s     # новое
    max-attempts: 12
    backoff-base: 1s         # новое
    backoff-max: 5m          # новое
  cleanup:
    retention: 7d
    interval: 1m             # новое
    batch-size: 1000         # новое
  metrics:
    refresh-interval: 5s     # новое
  inbox:
    retention: 30d           # обязательный, без дефолта (уже так)
```

---

## 3. Неделя 2 — сторона отправителя

### T2.0 · Фундамент: зависимости, отдельный Flyway, миграция V2, тестовая база

**Зачем.** Расхождения D1 и D2 ломают всё последующее, поэтому закрываются первыми.

**Сделать**

1. `outbox-spring-boot-starter/build.gradle.kts` — добавить:
   ```kotlin
   implementation("org.springframework.boot:spring-boot-starter-aop")
   compileOnly("io.micrometer:micrometer-tracing")          // опциональный проброс trace-контекста

   testImplementation("org.springframework.boot:spring-boot-starter-jdbc")
   testImplementation("org.springframework.boot:spring-boot-testcontainers")
   testImplementation("org.springframework.kafka:spring-kafka-test")
   testImplementation("org.testcontainers:junit-jupiter")
   testImplementation("org.testcontainers:postgresql")
   testImplementation("org.testcontainers:kafka")
   testImplementation("org.awaitility:awaitility")
   testRuntimeOnly("org.postgresql:postgresql")
   ```
   Версии не указывать — их даёт Spring Boot BOM.
2. Переделать `ReliableMessagingAutoConfiguration`: вместо `FlywayConfigurationCustomizer` —
   бин `ReliableMessagingSchemaMigrator` (`InitializingBean`), который при старте выполняет
   ```java
   Flyway.configure()
         .dataSource(dataSource)
         .locations("classpath:reliable-messaging/db/migration")
         .table("reliable_messaging_schema_history")
         .baselineOnMigrate(true)
         .baselineVersion("0")
         .load()
         .migrate();
   ```
   Условия: `@ConditionalOnBean(DataSource.class)`, `outbox.migrations.enabled=true` (по умолчанию).
   Автоконфигурация — `after = DataSourceAutoConfiguration.class`. Приложение может при этом
   пользоваться своим Flyway с собственным `V1` — они больше не пересекаются.
3. Новая миграция `reliable-messaging/db/migration/V2__relay_protocol.sql`:
   ```sql
   alter table outbox_message add column claim_token uuid;

   create index outbox_dead_idx on outbox_message (created_at) where status = 'DEAD';
   create index outbox_sent_idx on outbox_message (sent_at)    where status = 'SENT';
   create index inbox_processed_at_idx on inbox_message (processed_at);
   ```
   **`V1` не редактировать** — у кого-то уже может быть применённая схема.
4. Расширить `OutboxProperties` новыми полями из раздела 2 (`send-timeout`, `reaper-interval`,
   `backoff-base`, `backoff-max`, `cleanup.interval`, `cleanup.batch-size`, `metrics.refresh-interval`).
   В компактном конструкторе `Relay` — проверка `sendTimeout < leaseDuration`, иначе
   `IllegalArgumentException("outbox.relay.send-timeout must be shorter than outbox.relay.lease-duration")`.
5. Тестовая инфраструктура в `src/test/java/dev/reliablemessaging/outbox/support/`:
   - `TestApplication` — `@SpringBootConfiguration @EnableAutoConfiguration`.
   - `Containers` — интерфейс с `@Container @ServiceConnection static PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16.15-alpine")`
     и `static KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.0")` (`org.testcontainers.kafka.KafkaContainer`).
     Образы — те же, что в compose.
   - `@IntegrationTest` — мета-аннотация: `@SpringBootTest(classes = TestApplication.class)`, `@Testcontainers`,
     `outbox.inbox.retention=30d` в properties.
   - `OutboxTestRows` — хелпер для чтения строк `outbox_message` по статусам в тестах.

**Проверка**
- `SchemaMigrationIT`: приложение с собственной миграцией `V1__app.sql` в `classpath:db/migration`
  стартует; в базе есть обе таблицы истории; `outbox_message.claim_token` существует.
- `OutboxPropertiesTest`: обновить тест про Flyway под новую реализацию; тест на отказ при `send-timeout >= lease-duration`.
- `./gradlew build` зелёный.

**Коммит:** `T2.0: Isolated starter migrations, claim token, test containers`

---

### T2.1 · `OutboxPublisher` — запись в транзакции вызывающего

**Сделать**

1. `PayloadSerializer` — оборачивает `ObjectMapper` (берётся бин из контекста Spring Boot,
   иначе `new ObjectMapper().findAndRegisterModules()`). `String toJson(Object payload)`,
   `String toJson(Map<String,String> headers)`. Ошибка сериализации → `IllegalArgumentException`
   с именем класса payload.
2. `JdbcOutboxPublisher implements OutboxPublisher`:
   - Первой строкой: `if (!TransactionSynchronizationManager.isActualTransactionActive()) throw new IllegalTransactionStateException("OutboxPublisher.publish must be called inside an active transaction")`.
   - `UUID messageId = UUID.randomUUID()`.
   - Один `INSERT` через `JdbcTemplate`, который использует тот же `DataSource`, что и бизнес-код,
     и поэтому автоматически участвует в его транзакции:
     ```sql
     insert into outbox_message
       (message_id, aggregate_type, aggregate_id, topic, partition_key, payload, headers)
     values (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb)
     ```
   - Возвращает `messageId`.
3. Если активен `io.micrometer.tracing.Tracer` + `Propagator` (класс на classpath и бины есть) —
   записать текущий trace-контекст (`traceparent`) в заголовки сообщения. Это даёт сквозной
   трейс в Jaeger от HTTP-запроса до потребителя (используется в T4.3). Реализовать через
   отдельный бин `TraceHeadersSupplier` с no-op реализацией по умолчанию.
4. `OutboxPublisherAutoConfiguration`: бин `OutboxPublisher` при наличии `DataSource`,
   `@ConditionalOnMissingBean`.

**Проверка** — `OutboxPublisherIT`:
- внутри `TransactionTemplate` → строка `NEW`, payload и headers читаются как JSON, `partition_key = aggregate_id`;
- транзакция откатилась → строки нет (**критерий готовности «откат бизнес-транзакции»**);
- вызов без транзакции → `IllegalTransactionStateException`, строки нет;
- две публикации в одной транзакции → две строки с разными `message_id`.

**Коммит:** `T2.1: Transactional OutboxPublisher over JDBC`

---

### T2.2 · Такт захвата (claim) со `SKIP LOCKED`

**Сделать**

1. `OutboxRecord` — record: `id, messageId, aggregateType, aggregateId, topic, partitionKey, payload (String), headers (Map<String,String>), attempts, createdAt, claimToken`.
2. `OutboxRepository.claim(int batchSize, Duration lease, UUID claimToken) : List<OutboxRecord>` —
   **один оператор**, выполняемый в собственной короткой транзакции (`TransactionTemplate` с
   `PROPAGATION_REQUIRES_NEW`), коммит сразу после:
   ```sql
   with picked as (
       select id
       from outbox_message
       where status in ('NEW', 'FAILED')
         and next_attempt_at <= now()
       order by next_attempt_at, id
       limit :batch
       for update skip locked
   )
   update outbox_message m
      set status       = 'IN_FLIGHT',
          locked_until = now() + :lease,
          claim_token  = :token,
          attempts     = m.attempts + 1
     from picked
    where m.id = picked.id
   returning m.*
   ```
   `:lease` передавать как `interval` (`make_interval(secs => ?)` или `?::interval` со строкой `"30 seconds"`).
   Попытка считается в момент захвата — тогда строки, вернувшиеся через reaper, тоже расходуют попытки,
   и вечно падающий под не крутит сообщение бесконечно.
3. Предикат `where status in ('NEW','FAILED')` должен **буквально** совпадать с предикатом
   `outbox_pending_idx`, иначе планировщик индекс не возьмёт.

**Проверка**
- `ClaimIT.twoWorkersNeverClaimTheSameRow`: вставить 1 000 строк; два потока параллельно в цикле
  вызывают `claim(50, …)` с разными токенами до опустошения; пересечение множеств `id` пусто,
  объединение = все 1 000. Повторить 20 раз в одном тесте (`@RepeatedTest(20)` или цикл).
- `ClaimIT.respectsNextAttemptAt`: строка с `next_attempt_at` в будущем не захватывается.
- `ClaimIT.incrementsAttempts`.

**Коммит:** `T2.2: Claim step with FOR UPDATE SKIP LOCKED`

---

### T2.3 · Публикация в Kafka и такт подтверждения

**Сделать**

1. `OutboxDispatcher` — интерфейс: `List<DispatchResult> dispatch(List<OutboxRecord> batch)`.
   `DispatchResult` — `record(long id, boolean success, String error)`.
2. `KafkaOutboxDispatcher`:
   - Собственный `KafkaTemplate<String, String>`, построенный из `KafkaProperties.buildProducerProperties(sslBundles)`
     с принудительными переопределениями:
     `key/value.serializer = StringSerializer`, `acks = all`, `enable.idempotence = true`,
     `max.in.flight.requests.per.connection = 5`, `linger.ms = 5`,
     `delivery.timeout.ms = send-timeout`, `request.timeout.ms = min(send-timeout, 30s)` (и ≤ delivery.timeout).
     **Не использовать** общий `KafkaTemplate` приложения: у него могут быть другие сериализаторы и `acks`.
   - На каждую запись `ProducerRecord(topic, partitionKey, payload)` + заголовки из раздела 2 + пользовательские.
   - Отправить всю пачку асинхронно, затем дождаться всех future с общим таймаутом `send-timeout`.
     Успех/ошибка — по каждой записи отдельно. Таймаут — ошибка с текстом `"send timeout"`.
3. `RetryBackoff.nextDelay(int attempts) : Duration` — экспонента с «равным джиттером»:
   `exp = min(backoffMax, backoffBase * 2^(attempts-1))`; результат `exp/2 + random(0, exp/2)`.
   `Random` — инжектируемый (`RandomGenerator`) для тестов.
4. `OutboxRepository`:
   - `markSent(List<Long> ids, UUID token)` — одним оператором:
     ```sql
     update outbox_message
        set status = 'SENT', sent_at = now(), locked_until = null, claim_token = null, last_error = null
      where id = any(?) and claim_token = ? and status = 'IN_FLIGHT'
     ```
   - `markFailed(long id, UUID token, String error, Instant nextAttemptAt, boolean dead)` —
     `status = dead ? 'DEAD' : 'FAILED'`, `next_attempt_at`, `last_error` (обрезать до 2 000 символов),
     `locked_until = null`, `claim_token = null`, с тем же условием по `claim_token`.
     Неуспешные строки обновлять батчем (`batchUpdate`).
   - `dead = attempts >= maxAttempts`.
   - Всё подтверждение пачки — одна короткая транзакция.
   - Если фенсинг не совпал (0 строк) — это не ошибка: строку уже забрал другой воркер.
     Залогировать на `DEBUG`, увеличить счётчик `outbox.relay.fenced`.

**Проверка**
- `RetryBackoffTest`: для `attempts` 1…15 задержка в `[exp/2, exp]`; `backoffMax` соблюдается;
  при 10 000 вызовов с одинаковым `attempts = 5` ни одна секундная корзина не содержит больше 10 %
  значений (нет синхронной волны).
- `KafkaDispatchIT`: 100 строк → в топике 100 записей, ключ = `aggregate_id`,
  все четыре `rm-*` заголовка на месте, пользовательский заголовок на месте; все строки `SENT`.
- `AckFencingIT`: захватить строку токеном A, вручную «перезахватить» токеном B,
  `markSent(.., A)` → 0 строк, статус и токен B не тронуты.

**Коммит:** `T2.3: Kafka dispatch with acks=all and fenced acknowledgement`

---

### T2.4 · Цикл релэя и reaper

**Сделать**

1. `OutboxRelay implements SmartLifecycle`:
   - `workers` потоков из `Executors.newFixedThreadPool(workers, namedFactory("outbox-relay-"))`.
     Фабрику потоков вынести в метод — на этапе 2 сюда встанет флаг виртуальных потоков.
   - Цикл воркера: `token = randomUUID()` → `claim` → если пусто, `sleep(pollInterval)` →
     иначе `dispatch` → `acknowledge`. Если пачка была полной (`size == batchSize`) — следующий
     захват сразу, без сна.
   - Любое исключение в итерации логируется (`WARN`) и не убивает поток; после него — `sleep(pollInterval)`.
   - `stop()`: выставить флаг, дождаться потоков до 2 × `send-timeout`, затем `shutdownNow`.
     Незаконченные `IN_FLIGHT` вернёт reaper — это нормальный путь, а не авария.
   - `getPhase()` — `Integer.MAX_VALUE - 100`: стартует после всего, останавливается раньше Kafka-инфраструктуры.
2. `LeaseReaper` — `ScheduledExecutorService`, раз в `reaper-interval`:
   ```sql
   with expired as (
       select id from outbox_message
       where status = 'IN_FLIGHT' and locked_until < now()
       order by locked_until, id
       limit 1000
       for update skip locked
   )
   update outbox_message m
      set status = 'NEW', locked_until = null, claim_token = null, next_attempt_at = now()
     from expired
    where m.id = expired.id
   ```
   Число возвращённых строк → счётчик `outbox.relay.reclaimed`. Если возвращено > 0 — `WARN` в лог:
   это след упавшего или зависшего пода.
3. `OutboxRelayAutoConfiguration`: `@ConditionalOnProperty(outbox.relay.enabled, matchIfMissing=true)`,
   `@ConditionalOnClass(KafkaTemplate.class)`, `@ConditionalOnBean(DataSource.class)`.
   Бин `OutboxDispatcher` — `@ConditionalOnMissingBean` (оговорка ADR‑1: можно подменить реализацию).

**Проверка**
- `RelayEndToEndIT`: опубликовать 500 событий в транзакциях → через Awaitility все `SENT`,
  в топике 500 уникальных `rm-message-id`.
- `ReaperIT.crashedPodRowsAreRedelivered` (**критерий закрытия недели 2**): контекст 1 с подменённым
  `OutboxDispatcher`, который зависает навсегда; опубликовать 200 событий, дождаться, пока они станут
  `IN_FLIGHT`, закрыть контекст 1. Поднять контекст 2 с `lease-duration=2s`, `send-timeout=1s`,
  `reaper-interval=500ms` → все 200 `SENT`, в топике 200 уникальных `message_id`.
- `RelayDisabledTest`: `outbox.relay.enabled=false` → бина `OutboxRelay` нет.

**Коммит:** `T2.4: Relay workers and lease reaper`

---

### T2.5 · Метрики Micrometer и проверка индексов

**Сделать**

1. `OutboxMetrics` (бин при наличии `MeterRegistry`):
   - Гейджи, значения которых обновляются фоновым запросом раз в `metrics.refresh-interval`
     (не на каждом скрейпе — запрос к базе на каждый скрейп недопустим):
     | Метрика | Запрос |
     |---|---|
     | `outbox.messages.pending` | `count(*) where status in ('NEW','FAILED')` |
     | `outbox.messages.in_flight` | `count(*) where status = 'IN_FLIGHT'` |
     | `outbox.messages.dead` | `count(*) where status = 'DEAD'` |
     | `outbox.oldest_pending.age` (секунды) | `extract(epoch from now() - min(created_at)) where status in ('NEW','FAILED')` |
   - Счётчики: `outbox.relay.claimed`, `outbox.relay.published{result=sent|failed|dead}`,
     `outbox.relay.reclaimed`, `outbox.relay.fenced`, `outbox.cleanup.deleted{table=outbox|inbox}`.
   - Таймер: `outbox.relay.dispatch` (время отправки пачки).
2. Протянуть вызовы метрик в релэй, reaper и подтверждение. Без `MeterRegistry` — no-op.

**Проверка**
- `OutboxMetricsIT`: после публикации 10 событий с выключенным релэем `pending = 10`,
  `oldest_pending.age > 0`; после включения — `published{result=sent} = 10`.
- `IndexUsageIT` (это «EXPLAIN горячих запросов» из лога): вставить 20 000 `SENT` и 50 `NEW`,
  `ANALYZE outbox_message`, выполнить `EXPLAIN (FORMAT JSON)` для подзапроса захвата и подзапроса reaper;
  в плане присутствуют `outbox_pending_idx` и `outbox_expired_lease_idx` соответственно.
  **Не** выключать `enable_seqscan` — тест должен доказывать, что индекс выбирается честно.

**Коммит:** `T2.5: Micrometer metrics and index usage checks`

**Ядро недели 2 закрыто, если:** событие доезжает до Kafka по трёхтактному протоколу; конкурентный
захват не даёт пересечений; reaper возвращает строки «убитого» контекста.

---

## 4. Неделя 3 — сторона получателя и отказы

### T3.1 · Inbox и `IdempotentExecutor`

**Сделать**

1. `InboxRepository.tryRecord(UUID messageId, String consumer) : boolean`:
   ```sql
   insert into inbox_message (message_id, consumer) values (?, ?)
   on conflict do nothing
   ```
   `true`, если вставлена 1 строка. **Никакого предварительного `SELECT`** (ADR‑5).
2. `IdempotentExecutor.execute(String consumer, UUID messageId, Runnable action) : boolean` —
   в `TransactionTemplate` (`PROPAGATION_REQUIRED`, чтобы присоединиться к транзакции вызывающего,
   если она есть): `tryRecord` → если `false`, вернуть `false` без вызова `action` →
   иначе `action.run()` → коммит. Исключение из `action` откатывает и запись в inbox.
3. Счётчик `inbox.messages{consumer, result=processed|duplicate|failed}`.

**Проверка** — `IdempotentExecutorIT`:
- повторный вызов с тем же `messageId` → действие выполнено один раз (**критерий «повторная доставка»**);
- тот же `messageId`, другой `consumer` → выполнено для каждого (составной ключ);
- действие бросает исключение → в inbox записи нет, повторный вызов выполняет действие;
- **параллельно** 8 потоков с одним `messageId`, действие пишет строку в тестовую таблицу
  `effects` → ровно одна строка. (Второй `INSERT` ждёт на уникальном индексе до коммита первого и
  получает конфликт.)
- **граница гарантии** `ExternalCallBoundaryIT`: действие делает «внешний вызов» (инкремент
  `AtomicInteger`, имитирующий HTTP), затем пишет в базу и бросает исключение на первой попытке;
  на повторной — успех. Итог: строка в базе одна, внешних вызовов **два**. Тест назван и
  прокомментирован как фиксация ограничения из design § 7, а не как баг.

**Коммит:** `T3.1: Inbox table and idempotent executor`

---

### T3.2 · `@IdempotentConsumer` и `MessageMeta`

**Сделать**

1. `MessageMeta` — record: `UUID messageId, String aggregateType, String aggregateId, Instant occurredAt, String topic, int partition, long offset, Map<String,String> headers`.
2. `MessageMetaArgumentResolver implements HandlerMethodArgumentResolver` (messaging) —
   собирает `MessageMeta` из заголовков `Message<?>` (`rm-*` + `KafkaHeaders.RECEIVED_TOPIC/PARTITION/OFFSET`).
   Регистрация — бин `KafkaListenerConfigurer`, вызывающий
   `registrar.setCustomMethodArgumentResolvers(new MessageMetaArgumentResolver())`.
3. `@IdempotentConsumer(String name)` — на методе.
4. `IdempotentConsumerAspect` (`@Around("@annotation(consumer)")`): найти среди аргументов
   `MessageMeta`; если его нет — `IllegalStateException` на старте не поймать, поэтому проверка
   при первом вызове с понятным сообщением «метод с @IdempotentConsumer обязан принимать MessageMeta».
   Нет заголовка `rm-message-id` → `IllegalArgumentException` (сообщение не от стартера; уйдёт в
   обработчик ошибок контейнера). Дальше — `IdempotentExecutor.execute(name, meta.messageId(), joinPoint::proceed)`
   (checked-исключения обернуть и развернуть обратно).
5. `InboxAutoConfiguration`: `@ConditionalOnClass({KafkaListener.class, Aspect.class})`, бины
   `InboxRepository`, `IdempotentExecutor`, аспект, конфигурер резолвера.

**Проверка** — `IdempotentConsumerIT` с реальной Kafka: тестовый `@KafkaListener` с аннотацией;
отправить одну и ту же запись (тот же `rm-message-id`) три раза → эффект один, счётчик `duplicate = 2`.

**Коммит:** `T3.2: @IdempotentConsumer annotation and MessageMeta resolution`

---

### T3.3 · Повторы с джиттером под недоступный брокер

Логика `RetryBackoff` готова в T2.3. Здесь — интеграционное доказательство.

**Сделать / проверить** — `BrokerOutageIT`:
- Kafka-контейнер ставится на паузу (`KAFKA.getDockerClient().pauseContainerCmd(id)`) —
  отдельный контейнер в этом тесте, не общий.
- `send-timeout=2s`, `lease-duration=5s`, `backoff-base=500ms`, `backoff-max=5s`.
- Опубликовать 300 событий; дождаться, что все `FAILED` с `attempts ≥ 2`; проверить, что значения
  `next_attempt_at` рассеяны (разброс между min и max > 1 с).
- Снять паузу → все 300 `SENT`, в топике 300 уникальных `message_id`.

**Коммит:** `T3.3: Broker outage test for jittered retries`

---

### T3.4 · `DEAD`, `last_error`, очистка

**Сделать**

1. Переход в `DEAD` уже есть в T2.3. Добавить: `WARN`-лог с `message_id` и `last_error` при переходе;
   метрика `outbox.messages.dead` уже есть.
2. `RetentionCleaner` — раз в `cleanup.interval`, батчами по `cleanup.batch-size`, пока удаляется
   полный батч:
   ```sql
   delete from outbox_message where id in (
       select id from outbox_message
       where status = 'SENT' and sent_at < now() - ?::interval
       limit ?)
   ```
   ```sql
   delete from inbox_message where ctid in (
       select ctid from inbox_message
       where processed_at < now() - ?::interval
       limit ?)
   ```
   `DEAD` **никогда не удаляется автоматически** — это ручной разбор.
3. Javadoc на `OutboxProperties.Inbox.retention` и абзац в README (T3.5): почему retention inbox
   должен быть не меньше окна replay.

**Проверка**
- `DeadLetterIT`: диспетчер, всегда возвращающий ошибку `"boom"`, `max-attempts=3`,
  `backoff-base=10ms` → строка `DEAD`, `attempts = 3`, `last_error = "boom"`, `dead` гейдж = 1.
- `RetentionCleanerIT`: старые `SENT` и старые inbox удалены, свежие и `DEAD` остались.

**Коммит:** `T3.4: DEAD state reporting and retention cleanup`

---

### T3.5 · Стабилизация и документация API

**Сделать**

1. `scripts/repeat-tests.sh N` — запускает `./gradlew :outbox-spring-boot-starter:test --rerun-tasks` N раз,
   останавливается на первом падении, печатает итог.
2. Прогнать `scripts/repeat-tests.sh 10`. Любой флейк — чинить причину (ожидания через Awaitility,
   а не `Thread.sleep`; уникальные топики на тест; очистка таблиц в `@BeforeEach`).
3. Javadoc на все публичные типы пакета `api`.
4. `docs/implementation-notes.md` — актуальный список отклонений (D1–D3 как минимум).

**Критерий закрытия недели 3:** `scripts/repeat-tests.sh 10` проходит десять раз подряд.

**Коммит:** `T3.5: Test stabilization and public API docs`

---

## 5. Неделя 4 — демо-стенд, сценарии сбоя, демо

### T4.1 · `order-service` и `payment-service` поверх стартера

**order-service** (`dev.reliablemessaging.demo.order`)

- Миграция `src/main/resources/db/migration/V1__orders.sql` (своя, теперь не конфликтует):
  `orders(id uuid pk, customer_id text not null, total numeric(12,2) not null, created_at timestamptz default now())`.
- `POST /orders` `{ "customerId": "c-1", "total": 42.50 }` → `201 { "orderId": "…" }`.
  Параметр `?fail=true` — бросить исключение **после** `outbox.publish` (демонстрация отката).
- Событие `OrderPlaced(UUID orderId, String customerId, BigDecimal total, Instant placedAt)`, топик `orders.v1`.
- Режим `demo.delivery-mode: outbox | naive` (по умолчанию `outbox`):
  - `outbox` — `outbox.publish(...)` внутри `@Transactional`.
  - `naive` — наивный dual-write: `TransactionSynchronization.afterCommit` → асинхронный
    `kafkaTemplate.send(...)` со своим `KafkaTemplate` (`linger.ms=1000`, `acks=1`), заголовок
    `rm-message-id` = новый UUID. Большой `linger` делает окно потери видимым на `kill -9`.
    В этом режиме `outbox.relay.enabled=false`.
- `NewTopic` бин: `orders.v1`, 6 партиций.
- `demo.chaos.publish-delay: 0ms` — `BeanPostProcessor`, который оборачивает бин `OutboxDispatcher`
  задержкой перед отправкой. Нужен для сценария трёх релэев (T4.2).
- Зависимости: добавить `spring-boot-starter-validation`, `micrometer-registry-prometheus`.

**payment-service** (`dev.reliablemessaging.demo.payment`)

- Миграция `V1__payments.sql`:
  `payments(id bigserial pk, order_id uuid not null, amount numeric(12,2), created_at timestamptz default now())` —
  **без уникального индекса на `order_id`**, намеренно: двойной эффект должен быть виден, а не спрятан
  констрейнтом. Комментарий в миграции объясняет это.
  `delivery_log(message_id uuid not null, received_at timestamptz default now())` — сырые доставки, без ключа.
- `RecordInterceptor` бин: на каждую запись — `insert into delivery_log` в автокоммите (до обработчика).
  Так видно, сколько раз Kafka реально доставила сообщение.
- `StringJsonMessageConverter` бин — чтобы метод слушателя принимал `OrderPlaced`.
- Режим `demo.consumer-mode: idempotent | naive`:
  - `idempotent` — `@IdempotentConsumer(name = "payment-on-order-placed") @KafkaListener(topics = "orders.v1", groupId = "payment-service")`
    → `insert into payments`.
  - `naive` — тот же слушатель без аннотации.
- `DefaultErrorHandler` с `FixedBackOff(1000, 3)` + `DeadLetterPublishingRecoverer` → `orders.v1.DLT`.
- Зависимость `micrometer-registry-prometheus`.

**Проверка:** `make infra-up`, `make run-order`, `make run-payment`,
`curl -XPOST localhost:8081/orders -H 'content-type: application/json' -d '{"customerId":"c-1","total":10}'`
→ через секунду строка в `payments`. Тест `OrderFlowIT` в order-service на Testcontainers (заказ → строка outbox).

**Коммит:** `T4.1: Demo services on top of the starter`

---

### T4.2 · Скрипты сценариев сбоя

Каталог `demo-stand/scripts/`, всё на bash (`set -euo pipefail`), без зависимостей кроме Docker и JDK.
Приложения запускаются из собранных jar (`./gradlew bootJar`), PID и логи — в `demo-stand/.run/`
(добавить в `.gitignore`).

| Скрипт | Что делает |
|---|---|
| `lib.sh` | общие функции: `start_order PORT [args…]`, `start_payment [args…]`, `stop_all`, `kill9 PID`, `psql_order SQL`, `psql_payment SQL`, `wait_drained TIMEOUT` (pending + in_flight = 0 и число `payments` перестало расти 5 с) |
| `reset.sh` | остановить приложения, `truncate` всех таблиц в обеих базах, пересоздать `orders.v1` и `orders.v1.DLT`, сбросить группу потребителя |
| `load.sh RPS SECONDS` | нагрузка через `docker run grafana/k6` (скрипт `load.js` рядом, `constant-arrival-rate`, `POST /orders` на `host.docker.internal:8081`) |
| `reconcile.sh` | таблица: заказы; outbox по статусам; доставки (`delivery_log`); уникальные `message_id` в доставках; платежи; уникальные `order_id` в платежах; заказы без платежа; платежи-дубли. Exit code 1, если `orders ≠ distinct payments` или есть дубли платежей |
| `scenario-kill9.sh [outbox\|naive]` | reset → старт сервисов → `load.sh 500 30` в фоне → через 10 с `kill -9` order-service → рестарт → дождаться окончания нагрузки и дренажа → `reconcile.sh`. В `naive` ожидается расхождение, в `outbox` — ноль |
| `scenario-kafka-down.sh [MINUTES=5]` | нагрузка 100 rps → `docker compose stop kafka` → MINUTES минут, раз в 10 с печатать `pending` и `oldest_pending.age` → `docker compose start kafka` → дренаж → `reconcile.sh` |
| `scenario-three-relays.sh` | три order-service на портах 8081/8083/8085 с `--demo.chaos.publish-delay=3s --outbox.relay.lease-duration=2s --outbox.relay.send-timeout=1s --outbox.relay.reaper-interval=1s` → нагрузка → дренаж → `reconcile.sh`. Ожидается: доставок больше, чем уникальных `message_id` (дубли есть), платежей ровно столько, сколько заказов |
| `scenario-rollback.sh` | 100 запросов `POST /orders?fail=true` → заказов и outbox-строк не прибавилось |
| `scenario-replay.sh` | после любого сценария: остановить payment-service, `kafka-consumer-groups.sh --reset-offsets --to-earliest --execute`, запустить → доставок стало больше, платежей столько же |

Цели `Makefile`: `demo-reset`, `demo-reconcile`, `demo-kill9`, `demo-kill9-naive`, `demo-kafka-down`,
`demo-three-relays`, `demo-rollback`, `demo-replay`.

**Проверка:** каждый сценарий отработан с чистого состояния, вывод `reconcile.sh` сохранён в
`docs/demo-1-results.md` (дата, команда, таблица).

**Коммит:** `T4.2: Failure scenario scripts and reconciliation`

---

### T4.3 · Наблюдаемость: Prometheus, Grafana, Jaeger

**Сделать**

1. `docker-compose.yml`: сервис `prometheus` (`prom/prometheus:v3.5.0`), порт `9090`, конфиг
   `demo-stand/prometheus/prometheus.yml` — скрейп `host.docker.internal:8081,8082,8083,8085/actuator/prometheus`
   каждые 2 с; `extra_hosts: ["host.docker.internal:host-gateway"]` для Linux.
   `demo-stand/prometheus/alerts.yml`: правило `OutboxDeadMessages` — `outbox_messages_dead > 0` за 30 с.
2. Grafana provisioning: `demo-stand/grafana/provisioning/datasources/prometheus.yml` и
   `dashboards/dashboards.yml` + `demo-stand/grafana/dashboards/reliable-messaging.json`. Панели:
   - Outbox pending (time series) и oldest pending age;
   - In-flight и dead (stat, dead — красный при > 0);
   - Published rate по `result`; reclaimed rate; fenced rate;
   - Inbox processed vs duplicate rate;
   - HTTP rps order-service.
3. Трейсы: в order-service и payment-service включить `spring.kafka.listener.observation-enabled=true`
   и `spring.kafka.template.observation-enabled=true`; заголовок `traceparent`, записанный публикатором
   (T2.1), продолжает трейс HTTP-запроса через Kafka до потребителя. Проверить в Jaeger: один трейс
   содержит `POST /orders` и обработку в payment-service.

**Проверка:** во время `scenario-kafka-down.sh` в Grafana видно рост `pending` и дренаж после подъёма брокера.
Скриншоты дашборда и трейса — в `docs/demo-1-results.md`.

**Коммит:** `T4.3: Prometheus, Grafana dashboard, end-to-end tracing`

---

### T4.4 · README и правки design

**Сделать**

1. README:
   - «Статус» — актуальный.
   - «Подключение за 5 минут»: зависимость, `outbox.inbox.retention`, пример публикатора и потребителя
     (код из design § 7, проверенный компиляцией в демо).
   - «Гарантии» — таблица: что гарантируется (атомарность записи, at-least-once, однократный эффект
     в той же транзакции) и **что нет** (порядок; внешние вызовы; доставка `DEAD` без человека;
     дедуп старше `inbox.retention`).
   - «Конфигурация» — все свойства с дефолтами и смыслом.
   - «Метрики» — таблица из T2.5.
   - «Демо» — команды `make demo-*`.
2. `design.html`: changelog v1.2 (D1–D4), поправить строку рисков (D4). Опубликованную версию
   артефакта автор обновит сам.

**Коммит:** `T4.4: README with guarantees, design v1.2 notes`

---

### T4.5 · Генеральный прогон

**Сделать**

1. `docs/demo-1-runbook.md` — сценарий показа на 15 минут: порядок команд, что говорить на каждом
   шаге, что должно быть на экране, запасной план (заранее сохранённые результаты), если что-то не взлетело.
   Порядок из design § 8: naive + kill -9 → outbox + kill -9 → Kafka down в Grafana → три релэя
   → трейс в Jaeger.
2. Прогнать все сценарии подряд с `make infra-down && docker volume prune` (чистое состояние) —
   обновить `docs/demo-1-results.md`.
3. Поставить тег `stage-1-demo`.

**Коммит:** `T4.5: Demo 1 runbook and final results`

---

## 6. Критерии готовности этапа 1 (из design § 8)

| Проверка | Чем доказывается | Ожидаемо |
|---|---|---|
| `kill -9` под нагрузкой 500 rps | `scenario-kill9.sh outbox` + `reconcile.sh` | расхождение 0 |
| То же без outbox | `scenario-kill9.sh naive` | расхождение > 0 (контраст) |
| Kafka недоступна 5 минут | `scenario-kafka-down.sh`, Grafana | приём заказов не прерывается, очередь растёт, затем дренируется полностью |
| Три релэя параллельно | `scenario-three-relays.sh` | дубли доставок есть, бизнес-эффект однократен |
| Откат бизнес-транзакции | `OutboxPublisherIT`, `scenario-rollback.sh` | событие не публикуется |
| Повторная доставка | `IdempotentExecutorIT`, `scenario-replay.sh` | эффект один раз |
| Стабильность тестов | `scripts/repeat-tests.sh 10` | 10 зелёных прогонов подряд |

## 7. Чего не делать на этапе 1

Нагрузочные замеры, JMH, профилирование, виртуальные потоки, Toxiproxy, CI-гейт — всё это этап 2.
Приоритеты сообщений, другие брокеры, другие СУБД, реестр схем, saga — вне рамок проекта (design § 10).
Bloom-фильтр — удалён как неверный, не возвращать.
