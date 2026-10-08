package dev.reliablemessaging.outbox.support;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
public interface Containers {
    @Container @ServiceConnection
    PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16.15-alpine");
    @Container @ServiceConnection
    KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.0");
}
