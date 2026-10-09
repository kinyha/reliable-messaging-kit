# CPU, alloc, wall и JFR

Базовая локальная кампания500rps: по3×120s CPU/alloc/wall после60s прогрева, platform,
два воркера, Hikari10, batch128, poll200ms. Все9 captures имеют настоящие непустые collapsed
stacks, HTML и валидные часы. CPU captures дополнительно содержат сжатые JFR. Версии и image
IDs — в [manifest](../../perf-harness/results/20261009T214435Z-baseline-profile-fixed.profile-manifest.json).

| Активный relay | CPU share % median [min;max] | Wall share % median [min;max] | Alloc share % median [min;max] |
|---|---:|---:|---:|
|JDBC|50,00 [41,51;63,16]|33,33 [31,91;37,23]|36,21 [31,04;36,46]|
|Kafka|35,71 [34,21;41,51]|65,63 [62,77;68,09]|44,42 [44,20;47,03]|
|Jackson|1,89 [0;2,38]|0 [0;0,52]|15,10 [14,95;17,10]|

Idle `Thread.sleep` исключён из знаменателя. Категории взаимоисключающие: Jackson, затем
JDBC, затем Kafka, затем other. Это только кадры relay worker; background Kafka Sender не
входит в «Kafka share relay». CPU содержит38–53 активных worker samples на120s, поэтому
точные процентные доли имеют ограниченную статистическую точность.

Доминирующее активное ожидание — `KafkaOutboxDispatcher.dispatch → CompletableFuture.get →
timedGet → LockSupport.parkNanos → Unsafe.park`. JDBC занимает второе место. Это опровергает
wall-предсказание H1; это не доказывает, что тот же кадр первым ограничит максимальную capacity.
Для пропускной способности нужны отдельные saturation окна и рост pending.

Читаемые HTML виды для демо, полученные только фильтрацией настоящих взвешенных stacks:
[CPU](../../perf-harness/results/20261009T214435Z-baseline-profile-fixed-cpu-r2-relay.html),
[alloc](../../perf-harness/results/20261009T214435Z-baseline-profile-fixed-alloc-r2-relay.html),
[wall](../../perf-harness/results/20261009T214435Z-baseline-profile-fixed-wall-r2-relay.html).
Все три визуально проверены в браузере. Исходные общие profiles сохранены; derivative `.render.json`
фиксирует source/output SHA256, фильтр, точный суммарный вес и hash converter4.5.

JFR platform: суммарное ThreadPark время двух воркеров в трёх CPU записях7,159 /6,274 /6,298s.
Это события >=1ms, а не интеграл всего wall времени. CPU и wall снимаются раздельными окнами,
поэтому численные отношения между JFR waits и async-profiler samples не являются точным балансом.
Alloc выявил `OutboxRepository.read → ObjectMapper.readValue → JsonFactory.createParser →
ReaderBasedJsonParser`. По этому кадру выбрана [ровно одна оптимизация](optimization.md).

Старый harness читал текущий git HEAD после каждого capture: commit поля меняются8091865→
8415247→5a44bae, хотя Docker images заморожены и starter/demo исходники этих версий одинаковы.
Новый harness закрепляет commit из начального manifest. Старые raw JSON сохранены без переписывания;
строгий новый collector отклоняет их metadata mismatch. В этой исторической кампании нет
нормировки alloc по sent delta; она используется для выбора кадра, не как off/on сравнение.

## Виртуальные потоки и pinning

[Workflow37995202285](https://github.com/kinyha/reliable-messaging-kit/actions/runs/37995202285)
завершил все9 полных120s captures на Ubuntu24.04/x64, source8091865. Raw данные сохранены
в [virtual-profile-observation](../../perf-harness/results/virtual-profile-observation/README.md).
Два окна имеют dropped k6 iterations: CPU r1 —742, alloc r2 —23; HTTP errors0. Коллектор
не принимает всю эту кампанию как устойчивую performance линию500rps. Выбросы не заменены.

Во всех трёх настоящих JFR записях наблюдалось0 `jdk.VirtualThreadPinned` с threshold1ms;
`-Djdk.tracePinnedThreads=full` включён. Это не утверждение об отсутствии короткого pinning
или pinning при других нагрузках. Monitor contention первых virtual workers:1,180s суммарно
в первом CPU окне,1,433ms во втором,0 зарегистрировано в третьем. Основные кадры:
`RecordAccumulator.append`, `TransactionManager.maybeAddPartition`, `ProducerMetadata.add`.
Вход в monitor с конкуренцией не равен зарегистрированному VirtualThreadPinned.
[Точные экспортированные кадры](../../perf-harness/results/virtual-profile-observation/jfr-monitor-observation.json).

Async-profiler virtual CPU/wall может закончить стек на continuation barrier. В wall окнах
атрибутировано лишь8 /16 /14 активных relay samples. Эти доли не считаются полным временем
виртуальных воркеров и не объясняют причинно сохранённый выброс5,7s из отдельного H2 прогона.
H2 latency и Little's law выводы основаны на собственной полной принятой кампании.

Предложенный дополнительный300rps profiling прогон не запущен: пользователь попросил завершить
текущую работу. Неполное подтверждение устойчивого500rps profiling режима оставлено явным.
