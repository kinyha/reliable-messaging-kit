package dev.reliablemessaging.outbox.support;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

// Spring owns container lifetime, so cached contexts never retain a stopped JUnit container.
@TestConfiguration(proxyBeanMethods = false)
public class Containers {
    @Bean @ServiceConnection
    PostgreSQLContainer<?> postgres() { return new PostgreSQLContainer<>("postgres:16.15-alpine"); }
    @Bean @ServiceConnection
    KafkaContainer kafka() { return new KafkaContainer("apache/kafka:4.3.0"); }
}
