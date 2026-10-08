package dev.reliablemessaging.demo.payment;
import dev.reliablemessaging.outbox.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
public final class PaymentListeners {
    private PaymentListeners() { }
    public static class Idempotent {
        private final JdbcTemplate jdbc;
        public Idempotent(JdbcTemplate jdbc) { this.jdbc=jdbc; }
        @IdempotentConsumer(name="payment-on-order-placed")
        @KafkaListener(topics="orders.v1",groupId="payment-service")
        public void consume(OrderPlaced event,MessageMeta meta) {
            jdbc.update("insert into payments(order_id,amount) values (?,?)",event.orderId(),event.total());
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
