package dev.reliablemessaging.outbox.relay;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;
// ЧТО ПРОВЕРЯЕМ: шаг 2 протокола, захват пачки (OutboxRepository.claim).
// Один SQL-оператор: SELECT ... FOR UPDATE SKIP LOCKED, перевод строк в IN_FLIGHT с арендой и токеном, коммит.
// Именно это позволяет нескольким подам разбирать одну таблицу без лидера и без двойной отправки.
@IntegrationTest
class ClaimIT {
    @Autowired JdbcTemplate jdbc;
    @Autowired OutboxRepository repository;
    @BeforeEach void clean() { jdbc.execute("truncate outbox_message"); }
    // Вставляет count строк NEW одним SQL через generate_series: быстрее, чем через publisher.
    private void insert(int count) {
        jdbc.update("""
            insert into outbox_message(message_id, aggregate_type, aggregate_id, topic, partition_key, payload)
            select gen_random_uuid(), 'test', i::text, 'events', i::text, '{}'::jsonb from generate_series(1, ?) i
            """, count);
    }
    // Сценарий (повторяется 20 раз, чтобы поймать гонку): 1000 строк, два потока одновременно
    // (CyclicBarrier стартует их в один момент) захватывают пачки по 50 со своими токенами, пока таблица не опустеет.
    // Ожидаем: множества id у потоков не пересекаются (никто не взял чужую строку),
    // а вместе они покрывают все 1000 (ничего не пропущено).
    // Упражнение: убери skip locked из запроса claim, и потоки начнут ждать друг друга на блокировках.
    @RepeatedTest(20) void twoWorkersNeverClaimTheSameRow() throws Exception {
        insert(1000);
        try (var pool = Executors.newFixedThreadPool(2)) {
            var barrier = new CyclicBarrier(2);
            Callable<Set<Long>> worker = () -> {
                barrier.await();
                var ids = new HashSet<Long>();
                while (true) {
                    var rows = repository.claim(50, Duration.ofSeconds(30), UUID.randomUUID());
                    if (rows.isEmpty()) return ids;
                    // внутри одного потока id тоже не должны повторяться
                    for (var row : rows) assertThat(ids.add(row.id())).isTrue();
                }
            };
            var first = pool.submit(worker); var second = pool.submit(worker);
            // a и b: какие строки достались каждому из двух «подов»
            var a = first.get(30, TimeUnit.SECONDS); var b = second.get(30, TimeUnit.SECONDS);
            assertThat(a).doesNotContainAnyElementsOf(b);
            a.addAll(b); assertThat(a).hasSize(1000);
        }
    }
    // Сценарий: 2 строки, у одной next_attempt_at через час (так выглядит FAILED, ждущая повтора).
    // Ожидаем: захвачена только вторая, и у неё attempts = 1. Попытка засчитывается в момент захвата,
    // поэтому даже под, который падает до ack, тратит попытки и не крутит сообщение вечно.
    @Test void respectsNextAttemptAtAndIncrementsAttempts() {
        insert(2);
        jdbc.update("update outbox_message set next_attempt_at=now()+interval '1 hour' where aggregate_id='1'");
        var rows = repository.claim(50, Duration.ofSeconds(30), UUID.randomUUID());
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().attempts()).isEqualTo(1);
        assertThat(rows.getFirst().aggregateId()).isEqualTo("2");
    }
}
