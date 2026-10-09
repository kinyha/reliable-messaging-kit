# Демо 2: текущий проверенный результат

Статус приёмки и незавершённые проверки — [final-report](perf/final-report.md).
Ветка `stage-2`; тег не ставится до полной приёмки. Исходная рабочая ветка пользователя сохранена.

1. Открыть [baseline](perf/baseline.md):12 полных окон, median/minmax, HTTP и E2e,0 потерь.
2. Открыть [profiling](perf/profiling.md): CPU, alloc и wall HTML. В wall показать
   `KafkaOutboxDispatcher.dispatch → CompletableFuture.get → Unsafe.park`; в alloc — Jackson parser.
3. Показать [одну оптимизацию](perf/optimization.md): commit с прогнозом до реализации,
   `fast-empty-headers=false` по умолчанию, off/on реальные PostgreSQL checks. Пока нет заявления о выигрыше.
4. Показать [H2/H3/H5](perf/tuning.md): virtual outlier сохранён, batch/poll12×3 график,
   три двухчасовых окна и2 160 002 корректно доставленных заказа; отсутствие основания для V3.
5. Показать live SIGKILL [UUID сверку](../perf-harness/results/20261009T093926Z-live-restart.json),
   ожидаемые повторные доставки и единственный бизнес-эффект. Автотест ChaosIT покрывает оба thread mode.
6. Открыть [намеренно красный Draft PR1](https://github.com/kinyha/reliable-messaging-kit/pull/1)
   и [завершённое доказательство CI p99 отказа](perf/ci-gate.md). Порог сохранён, причина отказа конкретна.
7. Показать незакрытые пункты отчёта и ссылки на уже запущенные CI campaigns; не показывать pending как PASS.

Повторить проверки корректности:

```bash
./gradlew clean build --no-build-cache
python3 -m unittest discover -s perf-harness/scripts/tests
```

Полные performance suites требуют отдельного времени, Docker и одинакового hardware/stand.
Их нельзя совмещать на одном perf project:

```bash
perf-harness/scripts/run-baseline.sh --name first
perf-harness/scripts/run-baseline.sh --name repeat --skip-build
python3 perf-harness/scripts/verify-baseline.py perf-harness/results --first first --repeat repeat --output perf-harness/results/baseline-reproducibility.json
python3 perf-harness/scripts/benchmark.py --suite overhead --name capacity --skip-build
python3 perf-harness/scripts/benchmark.py --suite saturation --name capacity --skip-build
python3 perf-harness/scripts/benchmark.py --suite optimization --name optimization
python3 perf-harness/scripts/profile.py --events alloc --fast-empty-headers false --name optimization-off
python3 perf-harness/scripts/profile.py --events alloc --fast-empty-headers true --name optimization-on
```

`verify-baseline.py` возвращает exit1, если хотя бы одна repeat median выходит за исходный
min/max. Это не повод менять порог или удалять выброс. `collect-profiles.py` отклоняет
незавершённые/пустые профили, mixed commit/mode, плохие часы, missing JFR и dropped load iterations.
Raw files сохраняются; PNG/SVG строятся из фактических3 повторений.
