package dev.reliablemessaging.demo.payment;
import dev.reliablemessaging.outbox.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
public final class PaymentListeners {
    private PaymentListeners() { }
    public static class Idempotent {
        private final JdbcTemplate jdbc;
        private final Timer latency;
        public Idempotent(JdbcTemplate jdbc, MeterRegistry registry) {
            this.jdbc=jdbc;
            latency=Timer.builder("demo.e2e.latency").description("Event occurrence to committed payment")
                    .publishPercentileHistogram().serviceLevelObjectives(Duration.ofMillis(50),Duration.ofMillis(100),
                            Duration.ofMillis(200),Duration.ofMillis(500),Duration.ofSeconds(1),Duration.ofMillis(1500),
                            Duration.ofSeconds(3),Duration.ofSeconds(10),Duration.ofSeconds(30),Duration.ofMinutes(1))
                    .register(registry);
        }
        @IdempotentConsumer(name="payment-on-order-placed")
        @KafkaListener(topics="orders.v1",groupId="payment-service")
        public void consume(OrderPlaced event,MessageMeta meta) {
            jdbc.update("insert into payments(order_id,amount) values (?,?)",event.orderId(),event.total());
            if(meta.occurredAt()!=null) TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override public void afterCommit() {
                    var elapsed=Duration.between(meta.occurredAt(),Instant.now());
                    if(!elapsed.isNegative()) latency.record(elapsed);
                }
            });
        }
    }
    public static class Naive {
        private final JdbcTemplate jdbc;
        public Naive(JdbcTemplate jdbc) { this.jdbc=jdbc; }
        @KafkaListener(topics="orders.v1",groupId="payment-service")
        public void consume(OrderPlaced event) {
            jdbc.update("insert into payments(order_id,amount) values (?,?)",event.orderId(),event.total());
        }
    }
}
