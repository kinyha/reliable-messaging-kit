# Проверка performance-гейта

Порог зафиксирован в `perf-harness/k6/thresholds.json`: HTTP p99 ≤100 ms, e2e p99 ≤1500 ms,
ошибки HTTP ≤1%, пропущенные итерации, потерянные платежи, повторные бизнес-эффекты и очередь
после дренажа — 0. Квантиль e2e получен из разности histogram buckets, с интерполяцией.

Реальные GitHub Actions прогоны 9 октября 2026 года, Ubuntu 24.04 / x86_64. Оба выполнили
`./gradlew clean build --no-build-cache`, проверки алгоритмов harness, 60 s прогрева и 60 s
open-model нагрузки на 100 requests/s. Контейнеры ограничены одним и тем же perf overlay.
В каждом Java-отчёте 64 теста: 62 стартера и по одному order/payment; ошибок, отказов и skip — 0.

| Прогон | HTTP p99, ms | E2e p99, ms | Заказов / платежей | Потери / повторные эффекты | Результат |
|---|---:|---:|---:|---:|---|
| [Обычный stage-2](https://github.com/kinyha/reliable-messaging-kit/actions/runs/37927612061) | 4,99 | 227,64 | 6001 / 6001 | 0 / 0 | PASS |
| [PR с двумя секундами sleep](https://github.com/kinyha/reliable-messaging-kit/actions/runs/37927657115) | 5,22 | 3881,71 | 6001 / 6001 | 0 / 0 | FAIL: только e2e p99 |

Во втором прогоне exit 1 возникает после сохранения реальных k6/measurement JSON и точной
сверки UUID. Все остальные проверки гейта прошли. Это подтверждает отклонение замедления,
а не случайную ошибку сборки или стенда. [Черновой PR №1](https://github.com/kinyha/reliable-messaging-kit/pull/1)
остаётся намеренно красным и не предназначен для merge.

Сырые файлы и manifests: `perf-harness/results/20261009T120743Z*` и
`perf-harness/results/20261009T120809Z*`. Manifest содержит commit, image IDs, CPU/память,
effective compose и длительности. С полным artifact доступен HTML-отчёт Java-тестов.

Первый GitHub запуск `37925727274` не засчитывается: Linux runner не разрешил k6 UID 12345
записать summary в bind mount. Исправление `5b9f2d3` запускает k6 с UID/GID владельца результатов.
Два приведённых выше прогона выполнены после этого исправления. Локальные аналогичные проверки
до публикации также сохранены (`20261009T093144Z*` и `20261009T093512Z*`), но не подменяют CI-доказательство.

## Текущий результат после добавления проверенной оптимизации

На sourcefbb712d [run37998371136](https://github.com/kinyha/reliable-messaging-kit/actions/runs/37998371136)
Java и Python checks прошли, но HTTP p99=197,08ms превысил неизменённый порог100ms.
E2e p99=412,00ms и все проверки корректной доставки прошли. Причина этого роста ещё не
установлена; это не объявляется инфраструктурной ошибкой или исключённым выбросом.
[Raw JSON, manifest и failure log](../../perf-harness/results/ci-current-positive/README.md)
сохранены. Старые PASS/negative proof выше остаются историческими измерениями.
Полная приёмка текущей ветки не заявлена до разрешения этого отказа и длинных сравнений.

На обновлённом [negative run37998420212](https://github.com/kinyha/reliable-messaging-kit/actions/runs/37998420212)
гейт повторно отверг только e2e p99=3912,34ms >1500ms. HTTP p99=4,62ms; остальные проверки
прошли,6001 заказ/платёж, потерь0, повторных эффектов0. Java reports:69 starter +order1 +payment1,
failures0 в обоих текущих CI запусках. [Сырые файлы и отказ](../../perf-harness/results/ci-current-negative/README.md).
