package dev.reliablemessaging.outbox.relay;

import com.zaxxer.hikari.HikariDataSource;
import dev.reliablemessaging.outbox.api.MessageHeaders;
import dev.reliablemessaging.outbox.config.OutboxProperties;
import dev.reliablemessaging.outbox.inbox.*;
import dev.reliablemessaging.outbox.metrics.OutboxMetrics;
import dev.reliablemessaging.outbox.support.KafkaTestSupport;
import eu.rekawek.toxiproxy.*;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.bind.*;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.*;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ChaosIT {
    private final Network network=Network.newNetwork();
    private final PostgreSQLContainer<?> postgres=new PostgreSQLContainer<>("postgres:16.15-alpine")
            .withNetwork(network).withNetworkAliases("database");
    private final ToxiproxyContainer proxy=new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0")
            .asCompatibleSubstituteFor("shopify/toxiproxy")).withNetwork(network);
    private final KafkaContainer kafka=new KafkaContainer("apache/kafka:4.3.0").withNetwork(network).withNetworkAliases("broker")
            .withListener("broker:19092",() -> proxy.getHost()+":"+proxy.getMappedPort(8667));
    private HikariDataSource datasource;
    private JdbcTemplate jdbc,observer;
    private DataSourceTransactionManager tm;
    private OutboxRepository repository;
    private Proxy kafkaProxy,dbProxy;
    private KafkaOutboxDispatcher dispatcher;
    private final RetryBackoff backoff=new RetryBackoff(Duration.ofMillis(50),Duration.ofSeconds(1),new Random());

    @BeforeAll void infrastructure() throws Exception {
        postgres.start(); proxy.start();
        var client=new ToxiproxyClient(proxy.getHost(),proxy.getControlPort());
        dbProxy=client.createProxy("db","0.0.0.0:8666","database:5432");
        kafkaProxy=client.createProxy("kafka","0.0.0.0:8667","broker:19092");
        kafka.start();
        datasource=new HikariDataSource();
        datasource.setJdbcUrl("jdbc:postgresql://"+proxy.getHost()+":"+proxy.getMappedPort(8666)+"/test?socketTimeout=1&connectTimeout=1");
        datasource.setUsername(postgres.getUsername()); datasource.setPassword(postgres.getPassword());
        datasource.setMaximumPoolSize(4); datasource.setConnectionTimeout(1000); datasource.setValidationTimeout(1000);
        jdbc=new JdbcTemplate(datasource); tm=new DataSourceTransactionManager(datasource); repository=new OutboxRepository(jdbc,tm);
        observer=new JdbcTemplate(new DriverManagerDataSource(postgres.getJdbcUrl(),postgres.getUsername(),postgres.getPassword()));
        Flyway.configure().dataSource(datasource).locations("classpath:reliable-messaging/db/migration")
                .table("reliable_messaging_schema_history").load().migrate();
        jdbc.execute("create table effects(message_id uuid not null)");
        var kp=new KafkaProperties(); kp.setBootstrapServers(List.of(proxy.getHost()+":"+proxy.getMappedPort(8667)));
        dispatcher=new KafkaOutboxDispatcher(kp,null,Duration.ofSeconds(6));
    }
    @AfterAll void close() {
        if(dispatcher!=null) dispatcher.destroy(); if(datasource!=null) datasource.close();
        kafka.close(); proxy.close(); postgres.close(); network.close();
    }
    private OutboxProperties settings(boolean virtual) {
        return new Binder(new MapConfigurationPropertySource(Map.of("outbox.inbox.retention","30d",
                "outbox.relay.virtual-threads",virtual,"outbox.relay.send-timeout","2s","outbox.relay.lease-duration","3s",
                "outbox.relay.poll-interval","20ms","outbox.relay.reaper-interval","50ms","outbox.relay.max-attempts","30")))
                .bind("outbox",Bindable.of(OutboxProperties.class)).get();
    }
    private Set<UUID> seed(String topic) {
        jdbc.execute("truncate outbox_message,inbox_message,effects restart identity");
        var ids=new HashSet<UUID>();
        for(int i=0;i<100;i++) {
            var id=UUID.randomUUID(); ids.add(id);
            jdbc.update("insert into outbox_message(message_id,aggregate_type,aggregate_id,topic,partition_key,payload) values (?,'test','a',?,'one-key','{}')",id,topic);
        }
        return ids;
    }
    private void sent() {
        await().atMost(Duration.ofSeconds(90)).untilAsserted(() -> assertThat(observer.queryForObject(
                "select count(*) from outbox_message where status='SENT'",Integer.class)).isEqualTo(100));
    }
    private void effects(String topic,Set<UUID> expected,int deliveries) {
        var records=deliveries==expected.size() ? KafkaTestSupport.readUnique(kafka,topic,deliveries)
                : KafkaTestSupport.read(kafka,topic,deliveries);
        var executor=new IdempotentExecutor(new InboxRepository(jdbc),tm,OutboxMetrics.noop());
        var found=new HashSet<UUID>();
        for(var record:records) {
            var id=UUID.fromString(new String(record.headers().lastHeader(MessageHeaders.MESSAGE_ID).value(),StandardCharsets.UTF_8));
            found.add(id);
            executor.execute("chaos",id,() -> jdbc.update("insert into effects(message_id) values (?)",id));
        }
        assertThat(found).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(jdbc.queryForObject("select count(*) from effects",Integer.class)).isEqualTo(expected.size());
        assertThat(jdbc.queryForList("select message_id from effects",UUID.class)).containsExactlyInAnyOrderElementsOf(expected);
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void twoSecondKafkaLatencyDoesNotLoseMessages(boolean virtual) throws Exception {
        var topic=KafkaTestSupport.topic(kafka); var expected=seed(topic); var props=settings(virtual);
        var toxic=kafkaProxy.toxics().latency("two-seconds",ToxicDirection.DOWNSTREAM,2000);
        var relay=new OutboxRelay(repository,dispatcher,props.relay(),backoff);
        var reaper=new LeaseReaper(repository,props.relay());
        try { reaper.start(); relay.start(); sent(); }
        finally { toxic.remove(); relay.stop(); reaper.stop(); }
        effects(topic,expected,100);
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void postgresResetDuringAckRedeliversWithoutDuplicateEffects(boolean virtual) throws Exception {
        var topic=KafkaTestSupport.topic(kafka); var expected=seed(topic); var token=UUID.randomUUID();
        var batch=repository.claim(100,Duration.ofSeconds(3),token);
        var results=dispatcher.dispatch(batch); assertThat(results).allMatch(DispatchResult::success);
        observer.execute("""
                create or replace function chaos_pause_ack() returns trigger language plpgsql as $$
                begin if new.status='SENT' and new.id=%d then perform pg_sleep(2); end if; return new; end $$;
                create trigger chaos_pause before update on outbox_message for each row execute function chaos_pause_ack()
                """.formatted(batch.getFirst().id()));
        var ackExecutor=java.util.concurrent.Executors.newSingleThreadExecutor();
        var ack=ackExecutor.submit(() -> repository.acknowledge(batch,results,token,30,backoff));
        eu.rekawek.toxiproxy.model.toxic.ResetPeer toxic=null;
        try {
            await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> assertThat(
                    observer.queryForObject("select count(*) from pg_stat_activity where wait_event='PgSleep' and query like '%update outbox_message%'",Integer.class)).isPositive());
            toxic=dbProxy.toxics().resetPeer("ack-reset",ToxicDirection.DOWNSTREAM,0);
            assertThatThrownBy(() -> ack.get(10,java.util.concurrent.TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class).hasCauseInstanceOf(RuntimeException.class);
            assertThat(observer.queryForObject("select count(*) from outbox_message where status='SENT'",Integer.class)).isZero();
        } finally {
            if(toxic!=null) toxic.remove(); ackExecutor.shutdownNow();
            observer.execute("drop trigger if exists chaos_pause on outbox_message; drop function if exists chaos_pause_ack()");
        }
        observer.update("update outbox_message set locked_until=now()-interval '1 second'");
        var props=settings(virtual); var relay=new OutboxRelay(repository,dispatcher,props.relay(),backoff);
        var reaper=new LeaseReaper(repository,props.relay());
        try { reaper.start(); relay.start(); sent(); } finally { relay.stop(); reaper.stop(); }
        effects(topic,expected,200);
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void restartedContextRecoversBatchSentBeforeAck(boolean virtual) throws Exception {
        var topic=KafkaTestSupport.topic(kafka); var expected=seed(topic); var props=settings(virtual);
        var published=new CountDownLatch(1); var block=new CountDownLatch(1);
        try(var first=new AnnotationConfigApplicationContext()) {
            first.registerBean(OutboxRelay.class,() -> new OutboxRelay(repository,batch -> {
                dispatcher.dispatch(batch); published.countDown();
                try { block.await(); } catch(InterruptedException ex) { Thread.currentThread().interrupt(); }
                throw new IllegalStateException("Stopped after Kafka send, before database ack");
            },props.relay(),backoff)); first.refresh();
            assertThat(published.await(30,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
        try(var second=new AnnotationConfigApplicationContext()) {
            second.registerBean(LeaseReaper.class,() -> new LeaseReaper(repository,props.relay()));
            second.registerBean(OutboxRelay.class,() -> new OutboxRelay(repository,dispatcher,props.relay(),backoff));
            second.refresh(); sent();
        }
        effects(topic,expected,200);
    }
}
