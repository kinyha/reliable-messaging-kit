package dev.reliablemessaging.demo.payment;

import dev.reliablemessaging.outbox.api.MessageMeta;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties={"spring.kafka.listener.auto-startup=false","spring.kafka.admin.auto-create=false",
        "management.tracing.enabled=false"})
@Import(PaymentLatencyIT.Database.class)
class PaymentLatencyIT {
    @Autowired PaymentListeners.Idempotent listener;
    @Autowired MeterRegistry registry;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager tm;

    @Test void countsOnlyCommittedBusinessEffectsAndExcludesDuplicatesAndRollback() {
        var id=UUID.randomUUID(); var at=Instant.now().minusSeconds(1);
        var event=new OrderPlaced(UUID.randomUUID(),"latency-test",BigDecimal.TEN,at);
        var meta=new MessageMeta(id,"test","a",at,"events",0,0,Map.of());
        listener.consume(event,meta);
        listener.consume(event,meta);
        assertThat(registry.get("demo.e2e.latency").timer().count()).isEqualTo(1);
        new TransactionTemplate(tm).executeWithoutResult(s -> {
            listener.consume(event,new MessageMeta(UUID.randomUUID(),"test","b",at,"events",0,1,Map.of()));
            s.setRollbackOnly();
        });
        assertThat(registry.get("demo.e2e.latency").timer().count()).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from payments",Integer.class)).isEqualTo(1);
    }
    @TestConfiguration(proxyBeanMethods=false)
    static class Database {
        @Bean @ServiceConnection PostgreSQLContainer<?> postgres() {
            return new PostgreSQLContainer<>("postgres:16.15-alpine");
        }
    }
}
