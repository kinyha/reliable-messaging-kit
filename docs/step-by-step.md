# Как писать этот проект руками

Пошаговое руководство для того, кто открыл репозиторий и не знает, с чего начать.
Каждый шаг устроен одинаково: **что делаешь → какие файлы → как проверить руками →
готово, когда**. Шаги идут строго по порядку: следующий не заработает без предыдущего.

Спека — в [`design.html`](design.html) (§4 модель данных, §5 ADR, §6 поведение, §7 API).
Сроки и что в какую неделю — в [`plan.md`](plan.md). Здесь только «как руками».

---

## 0. Что уже есть, а чего нет

Болванка собирается и проходит тесты, но **доставки в ней нет ни строчки**.

| Есть | Нет |
|---|---|
| Gradle-мультипроект, Java 21, три модуля | Реализация `OutboxPublisher` |
| Миграция `V1__create_messaging_tables.sql` (обе таблицы, оба индекса, check-констрейнт) | Релэй: ни claim, ни publish, ни ack |
| Автоконфигурация: подмешивает миграции стартера в Flyway приложения | Reaper |
| `OutboxProperties` — все настройки с дефолтами | Inbox / идемпотентный потребитель |
| `OutboxPublisher` — интерфейс без реализации | Хоть какая-то бизнес-логика в демо-сервисах |
| Демо-стенд: 2 Postgres, Kafka, Jaeger, Grafana | Testcontainers-тесты |
| 2 юнит-теста на конфигурацию | Модуль `perf-harness` (это этап 2) |

Релэй в демо-сервисах выключен (`outbox.relay.enabled: false`) — это честно, потому что
включать нечего.

### Две вещи, которые надо решить до первой строчки кода

1. **Форма метода `publish`.** В дизайн-документе (§7) пример такой:

   ```java
   outbox.publish(OutboxMessage.builder()
           .topic("orders.v1")
           .aggregate(new Aggregate("order", order.id().toString()))
           .payload(new OrderPlaced(...))
           .build());
   ```

   А в коде сейчас — плоский метод с четырьмя аргументами. Расходятся.
   **Рекомендация: привести код к документу**, то есть сделать билдер. Причина ниже.

2. **Откуда берётся `partition_key`.** В таблице колонка `partition_key text not null`,
   а в текущей сигнатуре `publish` её негде передать. Это дыра: код физически не сможет
   вставить строку. Разумный дефолт — `partition_key = aggregate.id()`: события одного
   агрегата ложатся в одну партицию Kafka. Но иногда нужно иначе (например, ключ =
   customerId, чтобы все заказы клиента шли по порядку), поэтому нужна возможность
   переопределить.

   Ровно это и есть аргумент за билдер: обязательных полей три, необязательных
   (`partitionKey`, `headers`, `messageId`) — тоже три, и их станет больше. Четыре
   перегрузки конструктора превратятся в двенадцать.

---

## 1. Подготовка окружения

Нужны **JDK 21** и **Docker с Compose**. Проверь:

```bash
java -version          # должно быть 21
docker compose version
```

Собери и подними инфраструктуру:

```bash
./gradlew build        # должно быть BUILD SUCCESSFUL
make infra-up          # ждёт, пока все контейнеры не станут healthy
```

Убедись, что базы живые и в них применились миграции стартера:

```bash
docker compose -f demo-stand/docker-compose.yml exec order-postgres \
  psql -U orders -d orders -c '\dt'
```

Пока сервисы ни разу не запускались, таблиц не будет — Flyway отрабатывает **при старте
приложения**, а не при старте Postgres. Так что запусти сервис (в отдельном терминале):

```bash
make run-order
```

и повтори `\dt`. Должны появиться `outbox_message`, `inbox_message` и служебная
`flyway_schema_history`. Если появились — окружение в порядке, дальше можно писать код.

Проверь заодно Kafka:

```bash
docker compose -f demo-stand/docker-compose.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

> **Мини-словарь команд.** Дальше в тексте `psql-orders` означает
> `docker compose -f demo-stand/docker-compose.yml exec order-postgres psql -U orders -d orders`.
> Удобно завести алиас в своём шелле, иначе замучаешься печатать.

**Готово, когда:** `./gradlew build` зелёный, в базе `orders` есть таблица `outbox_message`.

---

## 2. Шаг 1 — модель сообщения и репозиторий

**Что делаешь.** Описываешь строку таблицы как Java-тип и учишься её писать и читать.
Никакой Kafka, никакой конкурентности — только SQL.

**Файлы** (все в `outbox-spring-boot-starter/src/main/java/dev/reliablemessaging/outbox/`):

```
api/OutboxMessage.java      публичный тип: то, что кладёт пользователь
api/MessageStatus.java      enum: NEW, IN_FLIGHT, FAILED, SENT, DEAD
store/OutboxRecord.java     внутренний тип: строка как она есть в базе
store/OutboxStore.java      весь SQL живёт здесь и больше нигде
```

**Почему два типа, а не один.** `OutboxMessage` — это *намерение* пользователя: тема,
агрегат, полезная нагрузка. `OutboxRecord` — *состояние* строки: плюс статус, счётчик
попыток, аренда, последняя ошибка. Пользователь не должен видеть слово `IN_FLIGHT` вообще
никогда. Если смешать их в один класс, публичный API стартера начнёт протекать
подробностями реализации, и поменять протокол релэя станет ломающим изменением.

**Скелет:**

```java
public record OutboxMessage(
        UUID messageId,          // может быть null → сгенерируем
        String topic,
        Aggregate aggregate,
        String partitionKey,     // может быть null → возьмём aggregate.id()
        Object payload,
        Map<String, String> headers
) {
    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        // ... topic(), aggregate(), payload() — обязательные
        // ... partitionKey(), headers(), messageId() — опциональные
        public OutboxMessage build() {
            Objects.requireNonNull(topic, "topic");
            Objects.requireNonNull(aggregate, "aggregate");
            Objects.requireNonNull(payload, "payload");
            return new OutboxMessage(
                    messageId != null ? messageId : UUID.randomUUID(),
                    topic, aggregate,
                    partitionKey != null ? partitionKey : aggregate.id(),
                    payload,
                    headers != null ? Map.copyOf(headers) : Map.of());
        }
    }
}
```

`OutboxStore` пиши на `JdbcClient` (он есть в Spring 6.1+, стартер уже тянет `spring-jdbc`).
Не на JPA: тут нужен точный контроль над SQL, а Hibernate будет мешать — он захочет
управлять `SELECT ... FOR UPDATE` по-своему и кэшировать сущности, которые мы намеренно
читаем в обход кэша.

Первый метод — только вставка:

```sql
insert into outbox_message
    (message_id, aggregate_type, aggregate_id, topic, partition_key, payload, headers)
values (:messageId, :aggregateType, :aggregateId, :topic, :partitionKey,
        cast(:payload as jsonb), cast(:headers as jsonb))
```

`payload` сериализуй Jackson'ом в строку и приводи через `cast(... as jsonb)` — JDBC не
умеет отдавать `jsonb` напрямую. Статус, `attempts`, `created_at`, `next_attempt_at` не
указываем: за них отвечают дефолты в DDL, и это правильно — одно место, где записано,
как выглядит новая строка.

**Как проверить руками:** пока никак — реализации `publish` ещё нет. Достаточно, что
компилируется.

**Готово, когда:** `./gradlew build` зелёный, в `OutboxStore` есть один метод `insert`.

---

## 3. Шаг 2 — реализация `OutboxPublisher`

**Что делаешь.** Соединяешь пользовательский вызов с записью в таблицу и, главное,
защищаешься от вызова без транзакции.

**Файлы:**

```
outbox/DefaultOutboxPublisher.java
config/ReliableMessagingAutoConfiguration.java   ← регистрируешь бин
```

```java
class DefaultOutboxPublisher implements OutboxPublisher {
    @Override
    public UUID publish(OutboxMessage message) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException(
                    "outbox.publish must be called inside a transaction; "
                            + "otherwise the event is not atomic with the business change");
        }
        store.insert(message);
        return message.messageId();
    }
}
```

**Зачем эта проверка — это суть всего проекта.** Если вызвать `publish` вне транзакции,
вставка в outbox закоммитится сама по себе, отдельно от изменения бизнес-данных. И мы
получим ровно ту проблему, ради которой всё затевалось: два независимых коммита, между
которыми процесс может умереть. Библиотека, которая молча это допускает, хуже отсутствия
библиотеки — она создаёт ложное чувство надёжности. Поэтому падаем громко.

**Как проверить руками.** Добавь в `order-service` эндпоинт:

```java
@PostMapping("/orders")
@Transactional
public Map<String, Object> place(@RequestBody PlaceOrder cmd) {
    long id = orders.insert(cmd);                       // бизнес-запись
    outbox.publish(OutboxMessage.builder()
            .topic("orders.v1")
            .aggregate(new Aggregate("order", String.valueOf(id)))
            .payload(new OrderPlaced(id, cmd.total()))
            .build());
    return Map.of("orderId", id);
}
```

Потом:

```bash
curl -sX POST localhost:8081/orders \
  -H 'content-type: application/json' \
  -d '{"total": 100}'
```

и в psql:

```sql
select id, message_id, topic, status, attempts, partition_key from outbox_message;
```

Должна быть одна строка со `status = 'NEW'`, `attempts = 0`.

**Обязательно проверь откат.** Сделай временный эндпоинт, который после `publish` кидает
исключение, дёрни его и убедись, что в таблице **не появилось** новой строки. Это и есть
доказательство атомарности — без него весь проект бессмысленен.

**Тест, который стоит написать сразу** (первый Testcontainers-тест):

```kotlin
// в outbox-spring-boot-starter/build.gradle.kts
testImplementation("org.springframework.boot:spring-boot-testcontainers")
testImplementation("org.testcontainers:junit-jupiter")
testImplementation("org.testcontainers:postgresql")
```

Сценарий: в транзакции вызвать `publish`, откатить, убедиться, что `count(*) = 0`.

**Готово, когда:** через curl создаётся заказ, в таблице лежит строка `NEW`, а при
исключении — не лежит ничего.

---

## 4. Шаг 3 — релэй, такт первый: захват

Дальше начинается самое интересное. Релэй работает в **три такта** (§6 дизайн-документа):
захват с коммитом → публикация вне транзакции → подтверждение. Делай их по одному.

**Что делаешь.** Фоновый цикл, который забирает пачку готовых строк и помечает их как
взятые в работу.

**Файлы:**

```
relay/OutboxRelay.java          цикл: poll → claim → (пока просто лог)
store/OutboxStore.java          + метод claimBatch
```

**Запрос захвата** — целиком, это ядро всего:

```sql
with claimed as (
    select id
    from outbox_message
    where status in ('NEW', 'FAILED')
      and next_attempt_at <= now()
    order by next_attempt_at, id
    limit :batchSize
    for update skip locked
)
update outbox_message m
   set status       = 'IN_FLIGHT',
       locked_until = now() + :lease,
       attempts     = m.attempts + 1
  from claimed c
 where m.id = c.id
returning m.id, m.message_id, m.topic, m.partition_key, m.payload, m.headers, m.attempts
```

Разбор по строчкам, потому что здесь каждая что-то значит:

- `status in ('NEW','FAILED')` и `next_attempt_at <= now()` — ровно условие частичного
  индекса `outbox_pending_idx`. Совпадение не случайно: индекс покрывает только строки,
  которые ждут отправки, поэтому он маленький и остаётся маленьким, сколько бы миллионов
  отправленных строк ни лежало в таблице.
- `for update skip locked` — если строку уже держит другой под, не ждём её, а **пропускаем**.
  Это то, что позволяет всем подам тянуть параллельно без выбора лидера (ADR-2).
  Без `skip locked` поды выстроились бы в очередь на одну и ту же первую строку.
- `attempts + 1` **на захвате, а не на ошибке.** Тонкий момент: если под умрёт после
  захвата, счётчик всё равно увеличится, и такая строка не будет захватываться вечно.
  Инкремент на ошибке этого не даёт — умерший под ошибку не запишет.
- `returning` — забираем данные тем же запросом, отдельный `select` не нужен.

**Метод должен быть в своей транзакции и коммитить сразу:**

```java
@Transactional
List<OutboxRecord> claimBatch(int batchSize, Duration lease) { ... }
```

**Почему коммит обязателен именно здесь.** Блокировка `FOR UPDATE` живёт ровно столько,
сколько живёт транзакция. Если держать транзакцию открытой на время отправки в Kafka,
получится длинная транзакция на каждый сетевой вызов — а это удерживаемые снапшоты,
раздувание таблицы и заблокированный autovacuum. Поэтому «занято» кодируется **не
блокировкой, а статусом** `IN_FLIGHT` с временем аренды `locked_until`. Блокировка нужна
только на миллисекунды самого захвата.

**Как проверить руками — самое наглядное упражнение во всём проекте.** Открой два psql
в разных терминалах и выполни в каждом:

```sql
begin;
select id from outbox_message
 where status in ('NEW','FAILED') and next_attempt_at <= now()
 order by next_attempt_at, id limit 5 for update skip locked;
```

Второй сеанс вернёт **другие** строки, а не подвиснет. Теперь убери `skip locked`
и повтори: второй сеанс зависнет, пока первый не сделает `commit`. Вот и вся разница,
ради которой не нужен ни ZooKeeper, ни выбор лидера. Не забудь `rollback;` в обоих.

**Готово, когда:** включил `outbox.relay.enabled: true`, создал заказ, и в логе видно,
что релэй захватил строку; в базе она в статусе `IN_FLIGHT` с непустым `locked_until`.

---

## 5. Шаг 4 — такты второй и третий: отправка и подтверждение

**Что делаешь.** Между захватом и подтверждением появляется Kafka.

```java
for (OutboxRecord r : claimed) {
    try {
        kafka.send(new ProducerRecord<>(r.topic(), r.partitionKey(), r.payload()))
             .get(sendTimeout.toMillis(), MILLISECONDS);   // ждём подтверждения брокера
        store.markSent(r.id());
    } catch (Exception e) {
        store.markFailed(r.id(), e, backoffFor(r.attempts()));
    }
}
```

**Подтверждение:**

```sql
update outbox_message
   set status = 'SENT', sent_at = now(), locked_until = null
 where id = :id and status = 'IN_FLIGHT'
```

**Ошибка:**

```sql
update outbox_message
   set status          = case when attempts >= :maxAttempts then 'DEAD' else 'FAILED' end,
       next_attempt_at = now() + :backoff,
       locked_until    = null,
       last_error      = :error
 where id = :id and status = 'IN_FLIGHT'
```

Три вещи, которые легко упустить:

1. **`and status = 'IN_FLIGHT'` в обоих запросах.** Пока мы отправляли, аренда могла
   истечь, и reaper мог вернуть строку в `FAILED`. Тогда наш `update` не должен ничего
   делать: строка уже не наша. Без этого условия мы бы перетёрли чужую работу.
2. **`.get(timeout)` обязателен.** `KafkaTemplate.send` асинхронный. Если не дождаться
   подтверждения брокера, мы пометим `SENT` сообщение, которое ещё нигде не лежит, — и
   потеряем его при падении. Это тихая потеря данных, худший вид бага в такой системе.
3. **Дубликаты — норма.** Если под умрёт между успешной отправкой и `markSent`, аренда
   истечёт и сообщение уедет второй раз. Так и задумано: at-least-once. Именно поэтому
   на другом конце обязателен inbox (шаг 6).

**Backoff с джиттером:**

```java
Duration backoffFor(int attempts) {
    long base = Math.min(baseDelay.toMillis() << Math.min(attempts, 10), maxDelay.toMillis());
    return Duration.ofMillis(base / 2 + ThreadLocalRandom.current().nextLong(base / 2 + 1));
}
```

Джиттер не косметика: без него сотня сообщений, упавших из-за одной недоступности брокера,
проснётся строго одновременно и ударит по нему одной волной — и уронит снова. Ровно тот же
приём, что и в любых ретраях к внешнему сервису.

**Как проверить руками.**

```bash
docker compose -f demo-stand/docker-compose.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.v1 --from-beginning
```

Оставь запущенным и создай заказ через curl — сообщение должно появиться в консоли,
а строка в базе перейти в `SENT`.

**Проверь отказ.** Останови Kafka (`docker compose ... stop kafka`), создай пару заказов
и смотри, как строки уходят в `FAILED` с растущим `attempts` и уезжающим `next_attempt_at`:

```sql
select id, status, attempts, next_attempt_at, left(last_error, 60) from outbox_message
 order by id desc limit 10;
```

Подними Kafka обратно — всё должно доехать само, без единого ручного действия. Это главный
трюк для демо: показать, что данные не потерялись, пока брокер лежал.

**Готово, когда:** сообщения доезжают, а после падения и подъёма Kafka доезжают сами.

---

## 6. Шаг 5 — reaper

**Что делаешь.** Возвращаешь в работу строки, которые кто-то захватил и не довёл до конца.

```sql
with expired as (
    select id from outbox_message
     where status = 'IN_FLIGHT' and locked_until < now()
     order by locked_until, id
     limit :batchSize
     for update skip locked
)
update outbox_message m
   set status = 'FAILED', locked_until = null, next_attempt_at = now()
  from expired e
 where m.id = e.id
returning m.id
```

**Почему без reaper всё разваливается.** Строка, застрявшая в `IN_FLIGHT`, — это худший
класс отказа из возможных. Ошибки не было, значит в логах пусто. В `outbox_pending_idx`
она не попадает, значит очередь не растёт и мониторинг молчит. Событие просто тихо
исчезает, и узнают об этом через неделю по жалобе клиента. Отдельный индекс
`outbox_expired_lease_idx` существует ровно затем, чтобы такие строки можно было находить
дёшево, — тот же частичный индекс, который экономит время в горячем пути, здесь работал
бы против нас, скрывая потерянные строки.

Запускать реже, чем основной цикл: раз в несколько секунд достаточно.

**Как проверить руками.** Захвати строку вручную и не отпускай:

```sql
update outbox_message
   set status = 'IN_FLIGHT', locked_until = now() - interval '1 minute'
 where id = <id>;
```

Аренда уже истекла — reaper должен в течение своего интервала вернуть строку в `FAILED`,
после чего релэй её подберёт и отправит. Заведи на это метрику-счётчик: сколько строк
вернул reaper. Ненулевое значение в проде — сигнал, что поды умирают на отправке.

**Готово, когда:** руками просроченная строка сама доезжает до `SENT`.

---

## 7. Шаг 6 — inbox: идемпотентный потребитель

**Что делаешь.** Вторую половину гарантии. Outbox обещает «доставим хотя бы раз» —
потребитель обязан обеспечить «применим ровно один раз».

**Файлы:**

```
api/IdempotentConsumer.java            аннотация
inbox/IdempotentConsumerAspect.java    или обёртка вокруг слушателя
```

Суть — один запрос:

```sql
insert into inbox_message (message_id, consumer) values (:messageId, :consumer)
on conflict do nothing
```

Если вернулось `0` изменённых строк — сообщение уже обработано, тихо выходим.
Если `1` — выполняем бизнес-логику. **В той же транзакции.**

**Почему нельзя сначала `select`, потом `insert`.** Классическая ловушка: два пода
одновременно делают `select` (оба видят «нет такого»), оба выполняют бизнес-логику, оба
вставляют. Двойное списание денег. Проверка и захват должны быть одной атомарной
операцией — а `insert ... on conflict do nothing` именно такая, потому что уникальность
обеспечивает первичный ключ `(message_id, consumer)`, а не наш код.

Кстати, ровно поэтому bloom-фильтр из версии 1.0 дизайна был выкинут: вставку всё равно
делать при любом ответе фильтра, экономить нечего.

**Почему в ключе есть `consumer`.** Одно сообщение может обрабатываться несколькими
независимыми потребителями, и каждый должен обработать его один раз *для себя*. Без
колонки `consumer` первый потребитель «съел» бы сообщение у всех остальных.

**Граница гарантии, которую надо понимать.** Однократность распространяется только на то,
что делается **в той же транзакции Postgres**, что и вставка в inbox. Отправить письмо или
дёрнуть чужой HTTP API «ровно один раз» этот механизм не может: откат транзакции не отменит
письмо. Для таких эффектов — свой outbox уже в этом сервисе.

**Как проверить руками.** Отправь одно и то же сообщение дважды (проще всего — вручную
через `kafka-console-producer.sh` с одинаковым `messageId`) и убедись, что бизнес-эффект
произошёл один раз, а в `inbox_message` одна строка.

**Готово, когда:** дубликат в топике не приводит к двойному эффекту.

---

## 8. Шаг 7 — сценарий для демо

К этому моменту у тебя работающий стартер. Демо — это не «показать код», а **показать,
что система переживает отказ**. Сценарий на 10 минут:

1. Создаём заказ → показываем строку в `outbox_message` и сообщение в топике. Скучно, но
   задаёт контекст.
2. Гасим Kafka → создаём три заказа → показываем, что бизнес-данные записались, а строки
   лежат в `FAILED` с растущим `attempts`. **Ничего не потеряно.**
3. Поднимаем Kafka → ничего не делаем руками → показываем, как всё доезжает.
4. Запускаем два экземпляра `order-service` на разных портах → показываем, что оба тянут
   из одной таблицы и не дублируют работу (`skip locked`).
5. Руками просрочиваем аренду → показываем reaper.
6. Отправляем дубликат → показываем, что inbox его съел.

Шаги 2–3 — главные. Именно там видно разницу между «записали в базу и отправили в Kafka»
и «сделали это надёжно».

---

## 9. Отладка: что смотреть, когда непонятно

**Общая картина по таблице:**

```sql
select status, count(*), min(created_at), max(attempts)
  from outbox_message group by status order by 2 desc;
```

**Кто застрял:**

```sql
select id, status, attempts, locked_until, next_attempt_at, left(last_error, 80)
  from outbox_message
 where status in ('IN_FLIGHT', 'FAILED', 'DEAD')
 order by id desc limit 20;
```

**Реально ли используется индекс** (если что-то тормозит):

```sql
explain (analyze, buffers)
select id from outbox_message
 where status in ('NEW','FAILED') and next_attempt_at <= now()
 order by next_attempt_at, id limit 128 for update skip locked;
```

Хочешь видеть `Index Scan using outbox_pending_idx`. Если видишь `Seq Scan` — либо строк
слишком мало и планировщик прав, либо условие в запросе разошлось с условием индекса.

**Таблица распухла** (много `SENT`, которые не чистятся):

```sql
select pg_size_pretty(pg_total_relation_size('outbox_message')),
       n_live_tup, n_dead_tup, last_autovacuum
  from pg_stat_user_tables where relname = 'outbox_message';
```

Outbox — таблица с высоким churn: каждая строка несколько раз обновляется, потом удаляется.
Если `n_dead_tup` растёт и `last_autovacuum` старый — нужен более агрессивный autovacuum
именно на эту таблицу. Это как раз тема этапа 2.

**Симптомы и причины:**

| Симптом | Куда смотреть |
|---|---|
| Строки висят в `NEW`, релэй молчит | `outbox.relay.enabled` в `application.yml` — в болванке он `false` |
| `IllegalStateException` про транзакцию | Нет `@Transactional` на вызывающем методе, или он вызван изнутри того же класса (self-invocation — прокси Spring не сработает) |
| Строки висят в `IN_FLIGHT` | Reaper не написан или не запущен |
| Сообщение уехало дважды | Это нормально. Вопрос не «почему», а «где inbox» |
| `attempts` растёт, `last_error` про сериализацию | Jackson не умеет твой payload — проверь, что это простой record |

---

## 10. Чек-лист перед тем, как считать шаг сделанным

- `./gradlew build` зелёный
- новое поведение покрыто тестом на Testcontainers, а не только руками через curl
- ни одного `Thread.sleep` в тестах — только `Awaitility` или ожидание по условию
- SQL живёт в `*Store`-классе, а не размазан по сервисам
- в публичном API не появилось слов `IN_FLIGHT`, `locked_until`, `attempts`
- README обновлён, если изменилось что-то из настроек

И отдельно: **если код разошёлся с `design.html` — правь документ**. Расхождение между
спекой и кодом всегда решается в пользу кода, но молча его оставлять нельзя: через месяц
непонятно, где правда. Как раз такое расхождение уже есть — форма `publish` и
`partition_key` из раздела 0.
