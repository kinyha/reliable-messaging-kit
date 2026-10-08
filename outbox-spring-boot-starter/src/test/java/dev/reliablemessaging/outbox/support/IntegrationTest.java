package dev.reliablemessaging.outbox.support;
import java.lang.annotation.*;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.junit.jupiter.Testcontainers;
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest(classes = TestApplication.class, properties = {
    "outbox.inbox.retention=30d", "outbox.relay.enabled=false"})
@Testcontainers
public @interface IntegrationTest { }
