package dev.reliablemessaging.outbox.inbox;
import dev.reliablemessaging.outbox.api.*;
import dev.reliablemessaging.outbox.support.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
@IntegrationTest
@Import(IdempotentConsumerIT.Listeners.class)
@TestPropertySource(properties={"outbox.relay.enabled=false","spring.kafka.consumer.auto-offset-reset=earliest",
    "spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
    "spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer"})
@DirtiesContext
class IdempotentConsumerIT {
    @Autowired JdbcTemplate jdbc;
    @Autowired KafkaTemplate<Object,Object> kafka;
    @Autowired MeterRegistry registry;
    @Test void realKafkaRedeliveryAppliesOnceAndCountsTwoDuplicates() throws Exception {
        var id=UUID.randomUUID();
        for(int i=0;i<3;i++) {
            var record=new ProducerRecord<Object,Object>("idempotent-test","a","{}");
            record.headers().add(MessageHeaders.MESSAGE_ID,id.toString().getBytes(StandardCharsets.UTF_8));
            record.headers().add(MessageHeaders.AGGREGATE_TYPE,"test".getBytes(StandardCharsets.UTF_8));
            record.headers().add(MessageHeaders.AGGREGATE_ID,"a".getBytes(StandardCharsets.UTF_8));
            kafka.send(record).get();
        }
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(jdbc.queryForObject("select count(*) from effects where id=?",Integer.class,id)).isEqualTo(1);
            assertThat(registry.get("inbox.messages").tags("consumer","listener-test","result","duplicate").counter().count()).isEqualTo(2);
            assertThat(registry.get("inbox.messages").tags("consumer","listener-test","result","processed").counter().count()).isEqualTo(1);
        });
    }
    @TestConfiguration(proxyBeanMethods=false)
    static class Listeners {
        @Bean MeterRegistry registry() { return new SimpleMeterRegistry(); }
        @Bean Listener listener(JdbcTemplate jdbc) { return new Listener(jdbc); }
    }
    public static class Listener {
        private final JdbcTemplate jdbc;
        public Listener(JdbcTemplate jdbc) { this.jdbc=jdbc; }
        @IdempotentConsumer(name="listener-test")
        @KafkaListener(topics="idempotent-test",groupId="listener-test")
        public void consume(String value,MessageMeta meta) {
            assertThat(meta.topic()).isEqualTo("idempotent-test");
            assertThat(meta.partition()).isGreaterThanOrEqualTo(0);
            assertThat(meta.offset()).isGreaterThanOrEqualTo(0);
            assertThat(meta.aggregateId()).isEqualTo("a");
            jdbc.update("insert into effects values (?)",meta.messageId());
        }
    }
}
