package dev.reliablemessaging.outbox.support;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

// Spring owns container lifetime, so cached contexts never retain a stopped JUnit container.
// ИНФРАСТРУКТУРА ТЕСТОВ: настоящие PostgreSQL и Kafka в Docker (Testcontainers), не моки.
// Версии те же, что в docker-compose демо-стенда: Postgres 16.15, Kafka 4.3.0.
// @ServiceConnection сам подставляет адреса контейнеров в spring.datasource.* и spring.kafka.*,
// поэтому ни в одном тесте нет захардкоженных url и портов.
// Контейнеры объявлены Spring-бинами, а не static-полями JUnit: так контейнер живёт ровно столько же,
// сколько закешированный Spring-контекст, и тест не получает DataSource от уже остановленного контейнера.
@TestConfiguration(proxyBeanMethods = false)
public class Containers {
    @Bean @ServiceConnection
    PostgreSQLContainer<?> postgres() { return new PostgreSQLContainer<>("postgres:16.15-alpine"); }
    @Bean @ServiceConnection
    KafkaContainer kafka() { return new KafkaContainer("apache/kafka:4.3.0"); }
}
