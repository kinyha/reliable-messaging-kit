package dev.reliablemessaging.outbox.relay;

import dev.reliablemessaging.outbox.api.*;
import dev.reliablemessaging.outbox.config.OutboxProperties;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.bind.*;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@IntegrationTest
class RelayThreadModeIT {
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager tm;
    @Autowired OutboxRepository repository;
    @Autowired OutboxPublisher publisher;
    @Autowired KafkaContainer kafka;
    @ParameterizedTest @ValueSource(booleans={false,true})
    void bothThreadModesDeliverThroughTheSameProtocolAndStop(boolean virtual) throws Exception {
        jdbc.execute("truncate outbox_message");
        var props=new Binder(new MapConfigurationPropertySource(Map.of("outbox.inbox.retention","30d",
                "outbox.relay.virtual-threads",virtual,"outbox.relay.poll-interval","20ms")))
                .bind("outbox",Bindable.of(OutboxProperties.class)).get();
        var kafkaProps=new KafkaProperties(); kafkaProps.setBootstrapServers(List.of(kafka.getBootstrapServers()));
        var observed=new ConcurrentLinkedQueue<Boolean>();
        var topic=KafkaTestSupport.topic(kafka);
        var dispatcher=new KafkaOutboxDispatcher(kafkaProps,null,Duration.ofSeconds(10));
        try {
            var relay=new OutboxRelay(repository,batch -> {
                observed.add(Thread.currentThread().isVirtual());
                assertThat(Thread.currentThread().getName()).startsWith("outbox-relay-");
                return dispatcher.dispatch(batch);
            },props.relay(),new RetryBackoff(Duration.ofMillis(10),Duration.ofSeconds(1),new Random()));
            try {
                relay.start();
                new TransactionTemplate(tm).executeWithoutResult(s -> {
                    for(int i=0;i<200;i++) publisher.publish(OutboxMessage.builder().aggregate(new Aggregate("test","a"))
                            .topic(topic).payload(Map.of("i",i)).build());
                });
                await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(jdbc.queryForObject(
                        "select count(*) from outbox_message where status='SENT'",Integer.class)).isEqualTo(200));
                assertThat(KafkaTestSupport.readUnique(kafka,topic,200)).hasSize(200);
                assertThat(observed).isNotEmpty().allMatch(value -> value==virtual);
            } finally { relay.stop(); }
            assertThat(relay.isRunning()).isFalse();
        } finally { dispatcher.destroy(); }
    }
}
