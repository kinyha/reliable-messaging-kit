# Этап 2: текущий итог при завершении работы

Работа сохранена в отдельной ветке `stage-2`, основанной на локальном `stage-1-demo`.
Реализация и проверки корректности готовы; все performance-критерии этапа ещё не закрыты.
Пользователь попросил завершить текущую работу, не дожидаясь оставшихся длинных кампаний.
Тег `stage-2-demo` не создан, потому что он означал бы полную приёмку.

## Что реализовано и проверено

- Изолированный Docker perf stand с CPU/heap/memory quotas, настоящим Toxiproxy для JDBC/Kafka.
- E2e Timer после успешного commit; rollback и duplicate delivery не создают latency sample.
- k6 constant-arrival-rate,60s прогрев,3×180s короткие окна,длительность H5 именно7200s.
- Virtual threads под default=false флагом, те же два воркера и claim/send/ack protocol.
- Ровно одна оптимизация `fast-empty-headers`, default=false; прогноз зафиксирован до кода.
- Реальные Testcontainers chaos: Kafka latency2s, PostgreSQL reset во время ack, рестарт после send.
- Live Docker SIGKILL137 в обоих режимах: по200 заказов/платежей, потерь0, повторных эффектов0;
  повторных доставок20 platform /52 virtual. [Исходная сверка](../../perf-harness/results/20261009T093926Z-live-restart.json).
- Последний чистый `./gradlew clean build --no-build-cache`:71 Java-тест (69 starter +order1 +payment1),
  failures/errors/skipped0. Последующий `./gradlew build` также зелёный.27 Python acceptance checks зелёные.

## Предсказал / получил / почему

|Проверка|Измеренный результат|Вывод|
|---|---|---|
|H1: JDBC >50% active wall|JDBC33,33%, Kafka65,63% median|Wall-предсказание опровергнуто; ожидание Kafka future. CPU JDBC50% против Kafka35,71%, мало samples.|
|H2: малый выигрыш при тех же воркерах|E2e virtual−3,13% pool10; +1,54% pool20. Сохранён max5,7s.|Нет устойчивого основания включать virtual по умолчанию.|
|H3: batch снижает SQL, poll определяет latency|Все12×3 окна; poll1s batch16→512 SQL39,93→3,94/s; poll50 недозаполненные пачки.|Сохраняются128/200ms: хороший запас к1,5s без частого SQL.|
|H5: vacuum удержит таблицу|3×7200s,2 160 002 заказа, по120 vacuum; max relation5,89–6,91MiB|На100rps/retention30s роста без границы нет; V3 не оправдана для этого режима.|
|Одна оптимизация: relay bytes/event−10–20%|Корректность обоих флагов проверена; off/on performance сравнение ещё идёт|Default=false, выигрыш не заявлен до завершённых измерений.|

[Гипотезы с исходными предсказаниями](hypotheses.md), [H2/H3/H5 таблицы и PNG/SVG](tuning.md),
[baseline](baseline.md), [CPU/alloc/wall/JFR и ограничения](profiling.md), [одна оптимизация](optimization.md).
H4 исключена: Bloom-фильтр не обеспечивает корректную дедупликацию.

## CI-гейт

Настоящий GitHub gate прошёл на обычной версии: e2e p99=227,64ms. Намеренный Thread.sleep2s
отклонён именно за e2e p99=3881,71ms >1500ms; все другие проверки, UUID сверка и Java-тесты прошли.
[Подробности и исходные данные](ci-gate.md). [Draft PR1](https://github.com/kinyha/reliable-messaging-kit/pull/1)
обновлён поверх текущей реализации: единственное кодовое отличие — задержка2s. Он не для merge.
Последний [positive37998371136](https://github.com/kinyha/reliable-messaging-kit/actions/runs/37998371136)
прошёл Java/harness checks, но отказал на настоящем HTTP p99=197,08ms >100ms.
E2e p99=412,00ms, потери/повторные эффекты/HTTP errors/dropped iterations/pending0.
Причина роста HTTP latency не установлена; это performance failure, не ошибка инфраструктуры.
[Исходные данные и log](../../perf-harness/results/ci-current-positive/README.md) сохранены,
порог не ослаблен. [Новый negative run37998420212](https://github.com/kinyha/reliable-messaging-kit/actions/runs/37998420212)
отклонил только e2e p99=3912,34ms >1500ms; HTTP4,62ms, все остальные проверки прошли.
Точные данные приведены в ci-gate.md. Прежнее завершённое доказательство
не делает текущий positive gate зелёным.

## Незакрытая часть полной приёмки

1. Текущий HTTP CI gate отказал197,08ms >100ms; нужна диагностика причины и фактическая повторная проверка.
2. Повтор полной baseline должен попасть медианой в исходный min/max по обоим p99 на всех4 rates.
   Локальный повтор остановлен; partial данные сохранены отдельно, критерий не подтверждён.
3. HTTP overhead relay off/on <5% и граница устойчивой capacity >3000 events/s ещё не установлены.
   Отдельная [CI baseline/repeat/overhead/capacity кампания](https://github.com/kinyha/reliable-messaging-kit/actions/runs/37996553037)
   уже запущена на одной VM с одинаковыми frozen images; её результаты не объявлены заранее.
4. Off/on нормированные alloc bytes/event и latency по3 повторения ещё не собраны полностью.
   [CI optimization campaign](https://github.com/kinyha/reliable-messaging-kit/actions/runs/37998079014) уже запущена.
5. Virtual profiling имеет два окна с dropped iterations; это qualitative JFR observation,
   а не полностью принятая устойчивaя500rps performance кампания.

Уже запущенные CI jobs сохраняют complete/partial JSON и JFR как artifacts. После их завершения
нужны проверка collectors, обновление этого отчёта фактическими числами и решение о приёмке.
Локальные load и app containers остановлены; другие проекты и их volumes сохранены.
Ни tag, ни merge в main не заменяют незавершённые проверки.

## Демо

[Runbook демо2](../demo-2-runbook.md) показывает завершённые измерения, flamegraphs, chaos и красный PR.
Он пригоден для просмотра текущего результата; это не запись успешной полной приёмки этапа.
