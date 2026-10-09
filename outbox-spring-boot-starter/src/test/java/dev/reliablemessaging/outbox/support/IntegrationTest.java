package dev.reliablemessaging.outbox.support;
import java.lang.annotation.*;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.junit.jupiter.Testcontainers;
// Мета-аннотация для интеграционных тестов. @IntegrationTest на классе даёт:
//  - полноценный Spring Boot контекст со всеми автоконфигурациями стартера, как в реальном приложении;
//  - Postgres и Kafka из Containers;
//  - outbox.inbox.retention=30d: без этой настройки стартер намеренно не стартует (см. OutboxPropertiesTest);
//  - outbox.relay.enabled=false: фоновый релэй по умолчанию ВЫКЛЮЧЕН. Тест сам вызывает шаги
//    claim / dispatch / ack, и релэй не утаскивает строки из-под носа. Тесты, которым нужен живой релэй,
//    включают его явно через @TestPropertySource (RelayEndToEndIT, DeadLetterIT).
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest(classes = TestApplication.class, properties = {
    "outbox.inbox.retention=30d"})
@org.springframework.test.context.TestPropertySource(properties="outbox.relay.enabled=false")
@org.springframework.context.annotation.Import(Containers.class)
@Testcontainers
public @interface IntegrationTest { }
