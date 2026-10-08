package dev.reliablemessaging.outbox.relay;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;
@IntegrationTest
class ClaimIT {
    @Autowired JdbcTemplate jdbc;
    @Autowired OutboxRepository repository;
    @BeforeEach void clean() { jdbc.execute("truncate outbox_message"); }
    private void insert(int count) {
        jdbc.update("""
            insert into outbox_message(message_id, aggregate_type, aggregate_id, topic, partition_key, payload)
            select gen_random_uuid(), 'test', i::text, 'events', i::text, '{}'::jsonb from generate_series(1, ?) i
            """, count);
    }
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
                    for (var row : rows) assertThat(ids.add(row.id())).isTrue();
                }
            };
            var first = pool.submit(worker); var second = pool.submit(worker);
            var a = first.get(30, TimeUnit.SECONDS); var b = second.get(30, TimeUnit.SECONDS);
            assertThat(a).doesNotContainAnyElementsOf(b);
            a.addAll(b); assertThat(a).hasSize(1000);
        }
    }
    @Test void respectsNextAttemptAtAndIncrementsAttempts() {
        insert(2);
        jdbc.update("update outbox_message set next_attempt_at=now()+interval '1 hour' where aggregate_id='1'");
        var rows = repository.claim(50, Duration.ofSeconds(30), UUID.randomUUID());
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().attempts()).isEqualTo(1);
        assertThat(rows.getFirst().aggregateId()).isEqualTo("2");
    }
}
