package dev.reliablemessaging.outbox.relay;
import dev.reliablemessaging.outbox.api.*;
import dev.reliablemessaging.outbox.support.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import java.time.Duration;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
@IntegrationTest
@Import(DeadLetterIT.Failing.class)
@TestPropertySource(properties={"outbox.relay.enabled=true","outbox.relay.max-attempts=3",
    "outbox.relay.backoff-base=10ms","outbox.relay.backoff-max=20ms","outbox.relay.poll-interval=10ms","outbox.metrics.refresh-interval=10ms"})
@DirtiesContext
class DeadLetterIT {
    @Autowired OutboxPublisher publisher;
    @Autowired PlatformTransactionManager tm;
    @Autowired JdbcTemplate jdbc;
    @Autowired MeterRegistry registry;
    @Test void exhaustsExactlyThreeAttemptsAndKeepsDiagnostic() {
        new TransactionTemplate(tm).executeWithoutResult(s -> publisher.publish(OutboxMessage.builder().topic("events")
            .aggregate(new Aggregate("test","a")).payload(Map.of("v",1)).build()));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(jdbc.queryForMap("select status,attempts,last_error from outbox_message"))
                .containsEntry("status","DEAD").containsEntry("attempts",3).containsEntry("last_error","boom");
            assertThat(registry.get("outbox.messages.dead").gauge().value()).isEqualTo(1);
            assertThat(registry.get("outbox.relay.published").tag("result","dead").counter().count()).isEqualTo(1);
        });
    }
    @TestConfiguration(proxyBeanMethods=false)
    static class Failing {
        @Bean MeterRegistry registry() { return new SimpleMeterRegistry(); }
        @Bean OutboxDispatcher dispatcher() {
            return rows -> rows.stream().map(r -> new DispatchResult(r.id(),false,"boom")).toList();
        }
    }
}
