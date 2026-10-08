package dev.reliablemessaging.outbox.relay;
import dev.reliablemessaging.outbox.api.*;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.Test;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
@IntegrationTest
@TestPropertySource(properties={"outbox.relay.enabled=true","outbox.relay.poll-interval=20ms"})
@Import(RelayEndToEndIT.Meters.class)
@DirtiesContext
class RelayEndToEndIT {
    @Autowired OutboxPublisher publisher;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager tm;
    @Autowired KafkaContainer kafka;
    @Autowired MeterRegistry registry;
    @Test void relaysFiveHundredCommittedEvents() throws Exception {
        var topic=KafkaTestSupport.topic(kafka);
        new TransactionTemplate(tm).executeWithoutResult(s -> {
            for(int i=0;i<500;i++) publisher.publish(OutboxMessage.builder().topic(topic)
                    .aggregate(new Aggregate("test",Integer.toString(i))).payload(Map.of("i",i)).build());
        });
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(jdbc.queryForObject("select count(*) from outbox_message where status='SENT'",Integer.class)).isEqualTo(500));
        assertThat(registry.get("outbox.relay.published").tag("result","sent").counter().count()).isEqualTo(500);
        assertThat(registry.get("outbox.relay.dispatch").timer().count()).isPositive();
        assertThat(KafkaTestSupport.read(kafka,topic,500)).extracting(r ->
                new String(r.headers().lastHeader(MessageHeaders.MESSAGE_ID).value(),StandardCharsets.UTF_8))
                .doesNotHaveDuplicates().hasSize(500);
    }
    @TestConfiguration(proxyBeanMethods=false)
    static class Meters { @Bean MeterRegistry registry() { return new SimpleMeterRegistry(); } }
}
