package dev.reliablemessaging.outbox.metrics;
import dev.reliablemessaging.outbox.api.*;
import dev.reliablemessaging.outbox.relay.OutboxRelay;
import dev.reliablemessaging.outbox.support.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import java.time.Duration;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;
@IntegrationTest
class OutboxMetricsIT {
    @Autowired JdbcTemplate jdbc;
    @Autowired OutboxPublisher publisher;
    @Autowired PlatformTransactionManager tm;
    @Test void cachesGaugesInsteadOfReadingAtScrapeTime() {
        jdbc.execute("truncate outbox_message");
        var registry=new SimpleMeterRegistry(); var metrics=new OutboxMetrics(jdbc,registry,Duration.ofSeconds(5));
        new TransactionTemplate(tm).executeWithoutResult(s -> {
            for(int i=0;i<10;i++) publisher.publish(OutboxMessage.builder().topic("events")
                .aggregate(new Aggregate("test","a")).payload(Map.of("i",i)).build());
        });
        metrics.refresh();
        assertThat(registry.get("outbox.messages.pending").gauge().value()).isEqualTo(10);
        assertThat(registry.get("outbox.oldest_pending.age").gauge().value()).isPositive();
        jdbc.execute("truncate outbox_message");
        assertThat(registry.get("outbox.messages.pending").gauge().value()).isEqualTo(10);
        metrics.refresh();
        assertThat(registry.get("outbox.messages.pending").gauge().value()).isZero();
    }
}
