package dev.reliablemessaging.outbox.relay;
import dev.reliablemessaging.outbox.config.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import static org.assertj.core.api.Assertions.*;
// ЧТО ПРОВЕРЯЕМ: outbox.relay.enabled=false выключает отправку. В контексте нет OutboxRelay,
// то есть этот инстанс сам ничего в Kafka не отправляет.
class RelayDisabledTest {
    @Test void disabledRelayHasNoWorkers() {
        new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(
                ReliableMessagingAutoConfiguration.class,OutboxRelayAutoConfiguration.class))
                .withPropertyValues("outbox.inbox.retention=30d","outbox.relay.enabled=false")
                .run(c -> assertThat(c).doesNotHaveBean(OutboxRelay.class));
    }
}
