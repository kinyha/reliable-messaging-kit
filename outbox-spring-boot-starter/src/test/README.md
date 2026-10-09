# Тесты стартера как документация

Каждый тестовый класс начинается с комментария «ЧТО ПРОВЕРЯЕМ», у каждого теста есть сценарий
и ожидание. Ниже — порядок чтения: он повторяет путь одного события через стартер.

Все интеграционные тесты (`*IT`) работают на настоящих PostgreSQL 16 и Kafka 4.3 в Docker
(Testcontainers). Нужен запущенный Docker.

## 0. Как устроены тесты

| Файл | Что внутри |
|---|---|
| `support/IntegrationTest.java` | Мета-аннотация: Spring-контекст + контейнеры, релэй по умолчанию выключен |
| `support/Containers.java` | Postgres и Kafka как Spring-бины |
| `support/TestApplication.java` | Пустое приложение, к которому стартер подключается сам |
| `support/KafkaTestSupport.java` | Свой топик на тест, чтение записей |

## 1. Отправитель: событие пишется вместе с бизнес-данными

| Тест | Что доказывает |
|---|---|
| `api/OutboxMessageTest` | Ключ партиции = id агрегата, заголовки `rm-*` зарезервированы |
| `publisher/OutboxPublisherIT` | Откат — нет события; без транзакции — исключение |
| `publisher/TraceHeadersSupplierTest` | traceparent едет через outbox, трейсинг опционален |

## 2. Релэй: доставка в Kafka

| Тест | Что доказывает |
|---|---|
| `relay/ClaimIT` | Два воркера никогда не берут одну строку (`SKIP LOCKED`) |
| `relay/KafkaDispatchIT` | Ключ и заголовки в Kafka, все строки `SENT` |
| `relay/AckFencingIT` | «Проснувшийся» под не портит чужой захват (`claim_token`) |
| `relay/RelayEndToEndIT` | Живой релэй сам отправляет 500 событий без потерь |

## 3. Отказы

| Тест | Что доказывает |
|---|---|
| `relay/ReaperIT` | Под умер посреди пачки — всё дошло после рестарта; последняя попытка → `DEAD` |
| `relay/BrokerOutageIT` | Kafka на паузе — повторы рассеяны джиттером, потом всё дошло |
| `relay/RetryBackoffTest` | Формула паузы между повторами |
| `relay/DeadLetterIT` | Невозможная доставка → `DEAD` с текстом ошибки и метрикой |

## 4. Получатель: дубль без эффекта

| Тест | Что доказывает |
|---|---|
| `inbox/IdempotentExecutorIT` | Дубль не выполняется; ошибка откатывает и inbox; 8 параллельных доставок → 1 эффект |
| `inbox/IdempotentConsumerIT` | `@IdempotentConsumer` на настоящем `@KafkaListener` |
| `inbox/ExternalCallBoundaryIT` | Где гарантия заканчивается: внешний вызов повторяется |

## 5. Эксплуатация

| Тест | Что доказывает |
|---|---|
| `metrics/OutboxMetricsIT` | Гейджи кешируются, а не читают базу на каждый скрейп |
| `relay/IndexUsageIT` | Postgres выбирает частичные индексы |
| `cleanup/RetentionCleanerIT` | Старые `SENT` и inbox удаляются, `DEAD` — никогда |
| `config/OutboxPropertiesTest` | Опасные настройки роняют старт |
| `config/SchemaMigrationIT` | Миграции стартера не конфликтуют с миграциями приложения |
| `relay/RelayDisabledTest` | Отправку можно выключить |

## Запуск

```bash
./gradlew :outbox-spring-boot-starter:test                       # все
./gradlew :outbox-spring-boot-starter:test --tests '*ClaimIT'    # один класс
```

Отчёт: `outbox-spring-boot-starter/build/reports/tests/test/index.html`.
