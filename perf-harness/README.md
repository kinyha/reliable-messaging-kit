# Performance-стенд этапа 2

Все команды запускаются в checkout ветки `stage-2`. JDK 21, Docker Compose с `!override`, Python 3.9+.
Первый запуск собирает layered images с JDK 21.0.12 и async-profiler 4.5; base image закреплён digest.
Профилировщик берётся из [официального release](https://github.com/async-profiler/async-profiler/releases/tag/v4.5).
Раскладка jar соответствует [Spring Boot Dockerfiles](https://docs.spring.io/spring-boot/reference/packaging/container-images/dockerfiles.html).

```bash
perf-harness/scripts/run-baseline.sh --name first
perf-harness/scripts/run-baseline.sh --name repeat --skip-build
python3 perf-harness/scripts/benchmark.py --suite overhead --skip-build
python3 perf-harness/scripts/benchmark.py --suite saturation --skip-build
perf-harness/scripts/run-profile.sh
perf-harness/scripts/run-virtual-threads.sh --skip-build
perf-harness/scripts/run-profile.sh --virtual --name virtual-profile
perf-harness/scripts/run-matrix.sh --skip-build
perf-harness/scripts/run-soak.sh --skip-build
perf-harness/scripts/run-ci.sh --skip-build
```

Стандартный короткий замер: **60 s прогрева и 3 × 180 s измерения**. Базовая линия повторяет
100/300/500/1000 rps. H5: **3 × 7200 s**, SENT retention=30s и cleanup interval=5s для оборота строк.
Процессы не перезапускаются между тремя повторениями одной конфигурации; таблицы очищаются после
дренажа. При смене конфигурации перезапускаются JVM и повторяется прогрев. Это не сценарий удаления
производственных данных: проект `rmk-perf` и его тома отделены от `reliable-messaging-kit`.

Одновременно допустима только одна perf-команда: файловая блокировка останавливает второй запуск.
Во время измерений не следует выполнять другие нагрузочные тесты или сборки на том же Docker host.
Время прогрева и короткие проверки инфраструктуры не попадают в таблицу результатов.

HTTP p99 берётся из настоящего open-model k6 Trend, без усреднения отдельных перцентилей.
E2e — разность cumulative histogram buckets до нагрузки и после дренажа, с линейной интерполяцией
внутри bucket. Его начало — `rm-occurred-at`, созданный до коммита заказа; конец — commit платежа.
Это близкая к commit-to-consumer оценка, а не точный timestamp коммита PostgreSQL.
Дубликаты и rollback не попадают в Timer. Очередь и CPU — периодические снимки, а не точный максимум
между снимками. +Inf quantile не заменяется фиктивным конечным числом. CI отвергает отсутствующий p99.

Порты: HTTP 18081/18082, отдельные management connectors 18091/18092, Prometheus 19090,
Toxiproxy 18474, PostgreSQL 15433/15434, Kafka 19092. Пробы management не стоят за очередью HTTP-заказов.
У приложения остаётся та же CPU-квота. Трейсинг отключён одинаково во всех perf-конфигурациях.
Топология через Toxiproxy применяется и в baseline, чтобы сетевой путь не менялся между сравнениями.
Kafka рекламирует `toxiproxy:29092`, поэтому клиент после bootstrap не обходит proxy.

`results/`: k6 summary, снимки/CPU/SQL, manifest с commit, параметрами, image IDs и ресурсами;
CPU/alloc/wall HTML и collapsed stacks; JFR gzip. `docs/perf/measurements.md` — сводка median [min; max].
Короткий `--suite validation --duration 15 --warmup 5 --runs 1` проверяет только инструменты;
его данные явно помечены `validation_only`, а не используются как performance-доказательство.

Overhead сравнивает работающий relay с остановленным; publisher в обоих случаях всё равно записывает
outbox. Это **стоимость фонового relay**, а не стоимость outbox INSERT относительно отсутствующего outbox.
Во втором режиме недоставленная очередь ожидаема и очищается только в этом изолированном стенде.

CI-контроль отклонения выполняется локально той же командой:

```bash
perf-harness/scripts/run-ci.sh --chaos-delay 2s --skip-build
```

Это настоящий `Thread.sleep` перед первой отправкой через существующий demo-флаг. Ожидается exit 1
из-за e2e p99, при сохранении однократных платежей. Пороги не пересчитываются автоматически после
неудачного прогона. Workflow публикует сырые результаты независимо от успеха проверки.
