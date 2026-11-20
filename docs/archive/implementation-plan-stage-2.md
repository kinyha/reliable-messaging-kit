# План реализации этапа 2 — ветка `stage-2`

**Не начинать, пока этап 1 не закрыт.** Условие старта — тег `stage-1-demo` и выполненные
критерии из раздела 6 [`implementation-plan.md`](implementation-plan.md).

```bash
git switch main && git pull
git switch -c stage-2 stage-1-demo
```

Вся работа этапа 2 идёт в ветке `stage-2`. В `main` она попадает только по решению автора,
одним merge после демо 2. Исключение — найденные на этапе 2 **баги корректности** стартера:
их фикс делается отдельным коммитом в `main` и затем подтягивается в `stage-2` (`git merge main`).

Правила из раздела 0 основного плана действуют и здесь. Дополнительно:

- **Этап 2 не добавляет функциональность.** Меняются только дефолты и внутренности под флагами.
  Новая возможность стартера — не сюда.
- **Предсказание до замера.** Для каждой гипотезы H1–H5 сначала заполняется колонка «Предсказание»
  в `docs/perf/hypotheses.md` и коммитится, потом снимается замер. Порядок коммитов — доказательство.
- **Каждый замер — три прогона, в отчёт идёт медиана и разброс.** Прогрев JVM обязателен (первые 60 с не считаются).
- **H4 (bloom-фильтр) из design § 9 не проверяется** — фильтр удалён в v1.1. Таблица гипотез: H1, H2, H3, H5.

---

## Структура

```
perf-harness/
├── compose.perf.yml          лимиты CPU/памяти поверх demo-stand/docker-compose.yml
├── k6/
│   ├── place-orders.js       constant-arrival-rate по POST /orders
│   └── thresholds.json       пороги для CI-гейта
├── toxiproxy/
│   └── profiles/*.json       latency, timeout, reset_peer, bandwidth
├── scripts/
│   ├── run-baseline.sh       3 прогона, сбор метрик, медиана
│   ├── run-profile.sh        JFR + async-profiler под нагрузкой
│   └── summarize.py          (или .java) сводка в markdown
└── results/                  CSV/JSON прогонов, коммитятся
docs/perf/
├── hypotheses.md             H1–H5: предсказание → факт → объяснение
├── baseline.md
├── profiling.md              флеймграфы со ссылками на кадры
└── final-report.md
```

---

## Неделя 5 — перф-стенд и базовая линия

**T5.1 · Изоляция окружения.** `compose.perf.yml`: `cpus`/`mem_limit` для Postgres, Kafka, order-service,
payment-service (сервисы запускаются в контейнерах, а не с хоста — иначе лимиты бессмысленны).
Dockerfile для обоих сервисов (layered jar).

**T5.2 · Сквозная задержка.** В payment-service — `Timer` `demo.e2e.latency` = `now() - rm-occurred-at`
(гистограмма с перцентилями, `publishPercentileHistogram`). Отдельно метрика задержки HTTP в order-service.

**T5.3 · k6-сценарий.** `place-orders.js`: `constant-arrival-rate` (open model — защищает от coordinated
omission), ступени 100 / 300 / 500 / 1000 rps, по 3 минуты. Вывод — JSON summary в `results/`.

**T5.4 · Базовая линия.** `run-baseline.sh`: чистый стенд → прогрев → 3 прогона → сбор p50/p95/p99
HTTP и e2e, CPU сервисов, rps. Результат — `docs/perf/baseline.md`.

**T5.5 · Накладные расходы и насыщение.** Сравнение order-service с `outbox.relay.enabled=false`
против `true` при одинаковом rps (цель design: < 5 % к p99). Поиск точки, где `outbox.messages.pending`
начинает расти неограниченно — это пропускная способность релэя (цель: > 3 000 соб./с).

**Критерий:** повторный прогон базовой линии попадает в разброс первого.

## Неделя 6 — профилирование

**T6.1 · JFR.** `run-profile.sh` снимает `jcmd <pid> JFR.start settings=profile duration=120s` под нагрузкой.
**T6.2 · async-profiler.** CPU и alloc флеймграфы (HTML) в `results/`. Проверка H1: доля JDBC-кадров
против кадров Kafka-продюсера.
**T6.3 · Первая оптимизация.** Ровно одна, по данным флеймграфа; повторный замер против базовой линии.
**T6.4 · Блокировки.** События `jdk.JavaMonitorEnter`, `jdk.ThreadPark` — где ждут воркеры.
**T6.5 · Отчёт** `docs/perf/profiling.md`: узкое место названо с указанием кадра стека.

## Неделя 7 — виртуальные потоки и настройка

**T7.1 · Флаг.** `outbox.relay.virtual-threads: false` — в `OutboxRelay` фабрика потоков из T2.4
переключается на `Thread.ofVirtual().name("outbox-relay-", 0).factory()`. Только флаг, поведение то же.
**T7.2 · Замер H2** при фиксированном пуле Hikari и при увеличенном.
**T7.3 · Pinning.** `-Djdk.tracePinnedThreads=full`, событие JFR `jdk.VirtualThreadPinned`.
**T7.4 · Закон Литтла.** Расчёт размера пула соединений (`L = λ · W`) и проверка замером.
**T7.5 · H3:** сетка `batch-size` {16, 64, 128, 512} × `poll-interval` {50, 200, 1000 мс}, график
«e2e p99 / запросов к БД в секунду».
**T7.6 · H5:** прогон 2 часа, `pg_stat_user_tables` (n_dead_tup, autovacuum_count), размер таблицы;
настройка `autovacuum_vacuum_scale_factor` для `outbox_message` (в миграции `V3`, если оправдано).
**T7.7 · Дефолты.** Обновить дефолты `OutboxProperties` по результатам, с обоснованием в README.

## Неделя 8 — хаос и CI-гейт

**T8.1 · Toxiproxy** в `compose.perf.yml` между сервисами и Postgres/Kafka; профили сбоев.
**T8.2 · Хаос-тесты** на Testcontainers + `ToxiproxyContainer`: латентность Kafka 2 с, разрыв соединения
с Postgres посреди подтверждения, рестарт пода посреди пачки. Проверка: потерь 0, дубли безвредны.
**T8.3 · CI-гейт.** `.github/workflows/perf-gate.yml`: сборка → стенд → короткий k6 → сравнение p99 с
`thresholds.json` → отчёт как артефакт сборки → падение при превышении. Проверка — PR с намеренным
`Thread.sleep` в релэе отклоняется.
**T8.4 · Финальный отчёт** `docs/perf/final-report.md`: таблица «предсказал / получил / почему».
**T8.5 · Демо 2:** runbook и прогон, тег `stage-2-demo`.

## Stretch goals (только при запасе времени)

Сравнение G1 / ZGC, JMH-бенчмарки сериализации, `OutboxDispatcher` поверх Debezium (CDC).
Описание — в [`plan.md`](plan.md), раздел «Stretch goals».
