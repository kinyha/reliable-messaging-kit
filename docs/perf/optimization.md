# Одна оптимизация, выбранная по профилю

Решение и предсказание фиксируются до изменения реализации и до сравнительных замеров.

Исходная кампания `20261009T214435Z-baseline-profile-fixed`: 500rps, два platform worker,
Hikari10, batch128, poll200ms, 60s прогрев и три настоящих120s записи каждого CPU/alloc/wall режима.
[Manifest](../../perf-harness/results/20261009T214435Z-baseline-profile-fixed.profile-manifest.json)
содержит точные image IDs и исходный commit. Образы собраны до документационных коммитов;
дальнейшее изменение HEAD не меняет эти образы. В старых per-capture JSON поле commit
читалось из текущего worktree: оно меняется8091865→8415247→5a44bae. Между этими тремя
commit нет изменений в starter/demo исходниках; Docker image IDs оставались теми же.
Новый harness закрепляет commit из начального manifest. Строгий новый коллектор отклоняет
старую кампанию из-за этих метаданных; её raw profiles сохранены с этим ограничением,
не исправлены задним числом и не используются для сравнения alloc bytes/event.
Это локальный ARM64 стенд, не Linux/x64 VM H2/H3.

В alloc профилях relay оценено284687841 /282066406 /249036325 bytes;
Jackson42991534 /48234404 /37224377 bytes (15,10 /17,10 /14,95%).
Это sampled allocation estimate с `--total`, а не точный счётчик всех выделений JVM.
Повторяющийся стек: `OutboxRepository.read → ObjectMapper.readValue →
JsonFactory.createParser → ReaderBasedJsonParser`. Demo публикует пустые headers `{}`.
[Alloc flamegraph](../../perf-harness/results/20261009T214435Z-baseline-profile-fixed-alloc-r2.html).

Выбрана ровно одна внутренняя оптимизация: `outbox.relay.fast-empty-headers`, default=false.
При точном JSONB тексте `{}` возвращать неизменяемый `Map.of()`, не создавая JSON parser.
Непустые значения и значения другой формы проходят прежний decoder. Никаких изменений claim,
ack, retry, fencing, Kafka, inbox и delivery protocol.

Предсказание: при этих пустых headers allocated bytes/event именно relay уменьшатся на10–20%.
Во всей JVM эффект ожидается около2%, потому что Jackson в relay составляет лишь1,6–2,1%
общего alloc estimate. Заметного снижения HTTP/e2e p99 не ожидается: активное wall время
определяет преимущественно Kafka future, а CPU Jackson составляет0–2,38% active relay samples.
Порог не является обязательным «выигрышем»: фактическое опровержение будет сохранено.

Сравнение выполняется в одном образе на одной отдельной Linux/x64 VM: флаг false/true,
500rps, два platform worker, Hikari10, batch128, poll200ms. Для каждого режима — три180s
нагрузочных окна после60s прогрева и три120s alloc capture после60s прогрева. Нормировка
alloc estimate — по delta `outbox_relay_published_total{result="sent"}` непосредственно
во время capture; не смешивать архитектуры или старые raw profiles без этого счётчика.
Raw результаты сохраняются вместе с manifest/image IDs, все выбросы учитываются.

Проверки корректности: реальный PostgreSQL при обоих значениях флага; пустая и непустая
карта, Unicode и trace headers, неизменяемость результата, откат claim при несовместимой
форме JSON. Старые программные конструкторы сохраняются. Новый флаг должен быть виден
в startup log; harness отказывается измерять запрошенный режим, если образ его не применил.
