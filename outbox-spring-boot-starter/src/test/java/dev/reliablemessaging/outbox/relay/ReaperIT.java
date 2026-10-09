package dev.reliablemessaging.outbox.relay;
import dev.reliablemessaging.outbox.api.*;
import dev.reliablemessaging.outbox.config.OutboxProperties;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.bind.*;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
// ЧТО ПРОВЕРЯЕМ: страховку на случай смерти пода, то есть аренду (lease) и LeaseReaper.
@IntegrationTest
class ReaperIT {
    @Autowired JdbcTemplate jdbc;
    @Autowired OutboxPublisher publisher;
    @Autowired PlatformTransactionManager tm;
    @Autowired OutboxRepository repository;
    @Autowired KafkaContainer kafka;
    // Короткие тайминги, чтобы тест шёл секунды: аренда 2 с, reaper каждые 100 мс.
    private OutboxProperties settings() {
        return new Binder(new MapConfigurationPropertySource(Map.of("outbox.inbox.retention","30d",
                "outbox.relay.lease-duration","2s","outbox.relay.send-timeout","1s",
                "outbox.relay.reaper-interval","100ms","outbox.relay.batch-size","100")))
                .bind("outbox",Bindable.of(OutboxProperties.class)).get();
    }
    // Сценарий «под умер посреди пачки»:
    // 1) Первый «под»: релэй с диспетчером, который зависает навсегда (имитация зависшей отправки).
    //    Он захватывает все 200 строк, и они становятся IN_FLIGHT. Закрываем его контекст: под «умер», ack не случился.
    // 2) Второй «под»: нормальный релэй с настоящей Kafka и reaper.
    // Ожидаем: reaper видит просроченную аренду и возвращает строки в NEW, второй релэй их отправляет:
    // все 200 SENT, в Kafka 200 записей. Без reaper эти строки висели бы в IN_FLIGHT вечно и молча.
    @Test void crashedPodRowsAreRedeliveredAfterBoundedShutdown() throws Exception {
        jdbc.execute("truncate outbox_message");
        var topic=KafkaTestSupport.topic(kafka); var properties=settings();
        var backoff=new RetryBackoff(Duration.ofMillis(10),Duration.ofSeconds(1),new Random());
        // latch, который никто не отпустит: диспетчер первого пода будет ждать, пока его не остановят
        var blocked=new CountDownLatch(1);
        var first=new OutboxRelay(repository,batch -> {
            try { blocked.await(); } catch(InterruptedException ex) { Thread.currentThread().interrupt(); }
            throw new IllegalStateException("crashed dispatcher");
        },properties.relay(),backoff);
        var firstContext=new AnnotationConfigApplicationContext();
        firstContext.registerBean(OutboxRelay.class,() -> first);
        firstContext.refresh();
        try {
            new TransactionTemplate(tm).executeWithoutResult(s -> {
                for(int i=0;i<200;i++) publisher.publish(OutboxMessage.builder().topic(topic).aggregate(new Aggregate("test","a"))
                        .payload(Map.of("i",i)).build());
            });
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(jdbc.queryForObject("select count(*) from outbox_message where status='IN_FLIGHT'",Integer.class)).isEqualTo(200));
        // закрытие контекста = остановка пода; захваченные строки так и остаются IN_FLIGHT
        } finally { firstContext.close(); }
        // Второй «под»: настоящий диспетчер, релэй и reaper.
        var kafkaProperties=new KafkaProperties(); kafkaProperties.setBootstrapServers(List.of(kafka.getBootstrapServers()));
        var dispatcher=new KafkaOutboxDispatcher(kafkaProperties,null,Duration.ofSeconds(1));
        var second=new OutboxRelay(repository,dispatcher,properties.relay(),backoff);
        var reaper=new LeaseReaper(repository,properties.relay());
        try(var secondContext=new AnnotationConfigApplicationContext()) {
            secondContext.registerBean(KafkaOutboxDispatcher.class,() -> dispatcher);
            secondContext.registerBean(LeaseReaper.class,() -> reaper);
            secondContext.registerBean(OutboxRelay.class,() -> second);
            secondContext.refresh();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(jdbc.queryForObject("select count(*) from outbox_message where status='SENT'",Integer.class)).isEqualTo(200));
            assertThat(KafkaTestSupport.read(kafka,topic,200)).hasSize(200);
        }
    }
    // Сценарий: строка IN_FLIGHT с истёкшей арендой, и это уже была последняя разрешённая попытка (3 из 3).
    // Ожидаем: reaper не возвращает её в очередь, а сразу переводит в DEAD и отражает это в метриках.
    // Иначе под, который падает на этом сообщении каждый раз, крутил бы его бесконечно в обход max-attempts.
    // Этот случай нашёлся при реализации, см. D7 в docs/implementation-notes.md.
    @Test void finalExpiredAttemptBecomesDeadInsteadOfRetryingForever() {
        jdbc.execute("truncate outbox_message");
        jdbc.update("""
            insert into outbox_message(message_id,aggregate_type,aggregate_id,topic,partition_key,payload,status,attempts,locked_until)
            values (?,'test','a','events','a','{}','IN_FLIGHT',3,now()-interval '1 second')
            """,UUID.randomUUID());
        var properties=new Binder(new MapConfigurationPropertySource(Map.of("outbox.inbox.retention","30d",
                "outbox.relay.max-attempts","3"))).bind("outbox",Bindable.of(OutboxProperties.class)).get();
        var registry=new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        var metrics=new dev.reliablemessaging.outbox.metrics.OutboxMetrics(jdbc,registry,Duration.ofSeconds(5));
        new LeaseReaper(repository,properties.relay(),metrics).tick();
        assertThat(registry.get("outbox.relay.published").tag("result","dead").counter().count()).isEqualTo(1);
        assertThat(registry.get("outbox.relay.reclaimed").counter().count()).isEqualTo(1);
        assertThat(jdbc.queryForObject("select status from outbox_message",String.class)).isEqualTo("DEAD");
    }
}
