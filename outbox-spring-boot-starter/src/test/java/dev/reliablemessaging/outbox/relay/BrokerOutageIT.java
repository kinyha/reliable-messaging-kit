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
// ЧТО ПРОВЕРЯЕМ: поведение при недоступной Kafka. Повторы с экспонентой и джиттером, потом дренаж без потерь.
// Тест поднимает СВОЙ отдельный контейнер Kafka, потому что ставит его на паузу (docker pause).
// Общий контейнер ломать нельзя: им пользуются другие тесты.
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
            // Тайминги ужаты: отправка ждёт 2 с, аренда 5 с, повторы от 0,5 до 5 с, до 30 попыток.
            var settings=new Binder(new MapConfigurationPropertySource(Map.of(
                    "outbox.inbox.retention","30d","outbox.relay.workers","1","outbox.relay.batch-size","300",
                    "outbox.relay.send-timeout","2s","outbox.relay.lease-duration","5s",
                    "outbox.relay.backoff-base","500ms","outbox.relay.backoff-max","5s",
                    "outbox.relay.max-attempts","30"))).bind("outbox",Bindable.of(OutboxProperties.class)).get();
            var props=new KafkaProperties(); props.setBootstrapServers(List.of(outage.getBootstrapServers()));
            var dispatcher=new KafkaOutboxDispatcher(props,null,Duration.ofSeconds(2));
            var backoff=new RetryBackoff(Duration.ofMillis(500),Duration.ofSeconds(5),new Random());
            var relay=new OutboxRelay(repository,dispatcher,settings.relay(),backoff);
            // «Прогрев»: одна успешная отправка до паузы, чтобы продюсер уже знал метаданные кластера.
            // При сверке в конце эта запись отфильтровывается.
            var warmId=UUID.randomUUID();
            assertThat(dispatcher.dispatch(List.of(new OutboxRecord(-1,warmId,"test","warm",topic,"warm","{}",Map.of(),1,Instant.now(),UUID.randomUUID()))).getFirst().success()).isTrue();
            boolean paused=false;
            try {
                // Kafka «упала»: контейнер заморожен, соединения висят.
                outage.getDockerClient().pauseContainerCmd(outage.getContainerId()).exec(); paused=true;
                // Кладём 300 событий и запускаем релэй. Все отправки будут падать по таймауту.
                new TransactionTemplate(tm).executeWithoutResult(s -> {
                    for(int i=0;i<300;i++) publisher.publish(OutboxMessage.builder().topic(topic)
                        .aggregate(new Aggregate("test",Integer.toString(i))).payload(Map.of("i",i)).build());
                });
                relay.start();
                // Ждём, пока каждая строка провалится хотя бы 2 раза.
                await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                    assertThat(jdbc.queryForObject("select min(attempts) from outbox_message",Integer.class)).isGreaterThanOrEqualTo(2));
                relay.stop();
                // Все 300 в FAILED: ничего не потеряно, просто ждут повтора...
                assertThat(jdbc.queryForObject("select count(*) from outbox_message where status='FAILED'",Integer.class)).isEqualTo(300);
                // ...и время следующей попытки у них РАЗНОЕ (разброс больше 1 с). Это джиттер: после восстановления
                // брокер не получит синхронную волну из 300 одновременных повторов.
                assertThat(jdbc.queryForObject("select extract(epoch from max(next_attempt_at)-min(next_attempt_at)) from outbox_message",Double.class)).isGreaterThan(1);
                // Kafka «поднялась». Ждём, пока все 300 станут SENT.
                outage.getDockerClient().unpauseContainerCmd(outage.getContainerId()).exec(); paused=false;
                relay.start();
                await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                    assertThat(jdbc.queryForObject("select count(*) from outbox_message where status='SENT'",Integer.class)).isEqualTo(300));
                // Сверка: в топике ровно те 300 message_id, что лежат в outbox. readUnique, потому что дубли здесь
                // допустимы (отправка могла дойти, а ack не успеть), а вот потерь быть не должно.
                var ids=KafkaTestSupport.readUnique(outage,topic,301).stream().map(r ->
                    new String(r.headers().lastHeader(MessageHeaders.MESSAGE_ID).value(),StandardCharsets.UTF_8))
                    .filter(id -> !id.equals(warmId.toString())).collect(java.util.stream.Collectors.toSet());
                assertThat(ids).hasSize(300).containsExactlyInAnyOrderElementsOf(jdbc.queryForList("select message_id::text from outbox_message",String.class));
            } finally {
                if(paused) outage.getDockerClient().unpauseContainerCmd(outage.getContainerId()).exec();
                relay.stop(); dispatcher.destroy();
            }
        }
    }
}
