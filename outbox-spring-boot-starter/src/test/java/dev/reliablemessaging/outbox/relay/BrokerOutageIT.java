package dev.reliablemessaging.outbox.relay;
import dev.reliablemessaging.outbox.api.*;
import dev.reliablemessaging.outbox.config.OutboxProperties;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.bind.*;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;
import java.time.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
@IntegrationTest
class BrokerOutageIT {
    @Autowired JdbcTemplate jdbc;
    @Autowired OutboxPublisher publisher;
    @Autowired OutboxRepository repository;
    @Autowired PlatformTransactionManager tm;
    @Test void pausedBrokerProducesJitteredRetriesThenDrainsWithoutLosingIds() throws Exception {
        jdbc.execute("truncate outbox_message");
        try(var outage=new KafkaContainer("apache/kafka:4.3.0")) {
            outage.start(); var topic=KafkaTestSupport.topic(outage);
            var settings=new Binder(new MapConfigurationPropertySource(Map.of(
                    "outbox.inbox.retention","30d","outbox.relay.workers","1","outbox.relay.batch-size","300",
                    "outbox.relay.send-timeout","2s","outbox.relay.lease-duration","5s",
                    "outbox.relay.backoff-base","500ms","outbox.relay.backoff-max","5s",
                    "outbox.relay.max-attempts","30"))).bind("outbox",Bindable.of(OutboxProperties.class)).get();
            var props=new KafkaProperties(); props.setBootstrapServers(List.of(outage.getBootstrapServers()));
            var dispatcher=new KafkaOutboxDispatcher(props,null,Duration.ofSeconds(2));
            var backoff=new RetryBackoff(Duration.ofMillis(500),Duration.ofSeconds(5),new Random());
            var relay=new OutboxRelay(repository,dispatcher,settings.relay(),backoff);
            var warmId=UUID.randomUUID();
            assertThat(dispatcher.dispatch(List.of(new OutboxRecord(-1,warmId,"test","warm",topic,"warm","{}",Map.of(),1,Instant.now(),UUID.randomUUID()))).getFirst().success()).isTrue();
            boolean paused=false;
            try {
                outage.getDockerClient().pauseContainerCmd(outage.getContainerId()).exec(); paused=true;
                new TransactionTemplate(tm).executeWithoutResult(s -> {
                    for(int i=0;i<300;i++) publisher.publish(OutboxMessage.builder().topic(topic)
                        .aggregate(new Aggregate("test",Integer.toString(i))).payload(Map.of("i",i)).build());
                });
                relay.start();
                await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                    assertThat(jdbc.queryForObject("select min(attempts) from outbox_message",Integer.class)).isGreaterThanOrEqualTo(2));
                relay.stop();
                assertThat(jdbc.queryForObject("select count(*) from outbox_message where status='FAILED'",Integer.class)).isEqualTo(300);
                assertThat(jdbc.queryForObject("select extract(epoch from max(next_attempt_at)-min(next_attempt_at)) from outbox_message",Double.class)).isGreaterThan(1);
                outage.getDockerClient().unpauseContainerCmd(outage.getContainerId()).exec(); paused=false;
                relay.start();
                await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                    assertThat(jdbc.queryForObject("select count(*) from outbox_message where status='SENT'",Integer.class)).isEqualTo(300));
                var ids=KafkaTestSupport.read(outage,topic,301).stream().map(r ->
                    new String(r.headers().lastHeader(MessageHeaders.MESSAGE_ID).value(),StandardCharsets.UTF_8))
                    .filter(id -> !id.equals(warmId.toString())).collect(java.util.stream.Collectors.toSet());
                assertThat(ids).hasSize(300);
            } finally {
                if(paused) outage.getDockerClient().unpauseContainerCmd(outage.getContainerId()).exec();
                relay.stop(); dispatcher.destroy();
            }
        }
    }
}
