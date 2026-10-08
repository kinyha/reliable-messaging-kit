package dev.reliablemessaging.outbox.publisher;
import dev.reliablemessaging.outbox.api.*;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.IllegalTransactionStateException;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;
@IntegrationTest
class OutboxPublisherIT {
    @Autowired OutboxPublisher publisher;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager tm;
    @BeforeEach void clean() { jdbc.execute("truncate outbox_message"); }
    private OutboxMessage event() {
        return OutboxMessage.builder().topic("test-events").aggregate(new Aggregate("test", "a-1"))
                .payload(Map.of("value", 42)).header("custom", "yes").build();
    }
    @Test void writesJsonAndDistinctIdsInCallingTransaction() {
        new TransactionTemplate(tm).executeWithoutResult(status -> {
            assertThat(publisher.publish(event())).isNotEqualTo(publisher.publish(event()));
        });
        assertThat(jdbc.queryForList("select status, partition_key, payload->>'value' as value, headers->>'custom' as custom from outbox_message"))
                .hasSize(2).allSatisfy(row -> {
                    assertThat(row).containsEntry("status", "NEW").containsEntry("partition_key", "a-1")
                            .containsEntry("value", "42").containsEntry("custom", "yes");
                });
    }
    @Test void rollbackRemovesBusinessChangeAndEvent() {
        new TransactionTemplate(tm).executeWithoutResult(status -> {
            var id = publisher.publish(event());
            jdbc.update("insert into effects values (?)", id);
            status.setRollbackOnly();
        });
        assertThat(jdbc.queryForObject("select count(*) from outbox_message", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("select count(*) from effects", Integer.class)).isZero();
    }
    @Test void rejectsAutocommit() {
        assertThatThrownBy(() -> publisher.publish(event())).isInstanceOf(IllegalTransactionStateException.class);
        assertThat(jdbc.queryForObject("select count(*) from outbox_message", Integer.class)).isZero();
    }
}
