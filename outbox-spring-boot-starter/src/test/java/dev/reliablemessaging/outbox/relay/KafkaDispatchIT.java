package dev.reliablemessaging.outbox.relay;
import dev.reliablemessaging.outbox.api.*;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
// ЧТО ПРОВЕРЯЕМ: шаги 2-4 протокола вручную, без фонового релэя: claim, отправка в настоящую Kafka, acknowledge.
// По сути это «контракт с Kafka»: ключ партиции, служебные заголовки rm-*, пользовательские заголовки,
// и все строки стали SENT после подтверждения.
@IntegrationTest
class KafkaDispatchIT {
    @Autowired KafkaContainer kafka;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager tm;
    @Autowired OutboxPublisher publisher;
    @Autowired OutboxRepository repository;
    @BeforeEach void clean() { jdbc.execute("truncate outbox_message"); }
    @Test void sendsHundredEventsWithProtocolAndUserHeaders() throws Exception {
        var topic=KafkaTestSupport.topic(kafka);
        new TransactionTemplate(tm).executeWithoutResult(s -> {
            for (int i=0;i<100;i++) publisher.publish(OutboxMessage.builder().topic(topic)
                    .aggregate(new Aggregate("test","a-1")).payload(Map.of("i",i)).header("custom","yes").build());
        });
        // Шаг 2: захватываем все 100 строк одним токеном.
        var token=UUID.randomUUID(); var batch=repository.claim(100,Duration.ofSeconds(30),token);
        // Шаг 3: собственный продюсер стартера (acks=all, идемпотентность), таймаут отправки 10 с.
        var properties=new KafkaProperties(); properties.setBootstrapServers(List.of(kafka.getBootstrapServers()));
        var dispatcher=new KafkaOutboxDispatcher(properties,null,Duration.ofSeconds(10));
        try {
            var results=dispatcher.dispatch(batch);
            assertThat(results).allSatisfy(r -> assertThat(r.success()).as(r.error()).isTrue());
            // Шаг 4: подтверждение. Все 100 должны стать SENT.
            assertThat(repository.acknowledge(batch,results,token,12,new RetryBackoff(Duration.ofSeconds(1),Duration.ofMinutes(5),new Random())).sent()).isEqualTo(100);
            // Читаем топик и проверяем каждую запись: ключ равен id агрегата (все события агрегата a-1 попадут
            // в одну партицию), есть rm-message-id / rm-aggregate-type / rm-aggregate-id / rm-occurred-at
            // и пользовательский заголовок custom.
            var records=KafkaTestSupport.read(kafka,topic,100);
            assertThat(records).hasSize(100).allSatisfy(r -> {
                assertThat(r.key()).isEqualTo("a-1");
                for(var h : List.of(MessageHeaders.MESSAGE_ID,MessageHeaders.AGGREGATE_TYPE,MessageHeaders.AGGREGATE_ID,MessageHeaders.OCCURRED_AT,"custom"))
                    assertThat(r.headers().lastHeader(h)).isNotNull();
                assertThat(new String(r.headers().lastHeader("custom").value(),StandardCharsets.UTF_8)).isEqualTo("yes");
            });
            assertThat(jdbc.queryForObject("select count(*) from outbox_message where status='SENT'",Integer.class)).isEqualTo(100);
        } finally { dispatcher.destroy(); }
    }
}
