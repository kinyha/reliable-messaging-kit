package dev.reliablemessaging.outbox.config;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;

import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

// ЧТО ПРОВЕРЯЕМ: настройки стартера и защиту от опасной конфигурации.
// Принцип: ошибка в настройках должна ронять старт приложения, а не всплывать потом в проде.
class OutboxPropertiesTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ReliableMessagingAutoConfiguration.class))
            .withPropertyValues("outbox.inbox.retention=30d");

    // Значения по умолчанию: пачка 128 строк, аренда 30 с.
    @Test
    void defaultsAreExplicitAndConservative() {
        contextRunner.run(context -> {
            var properties = context.getBean(OutboxProperties.class);

            assertThat(properties.relay().batchSize()).isEqualTo(128);
            assertThat(properties.relay().leaseDuration()).isEqualTo(Duration.ofSeconds(30));
            assertThat(properties.inbox().retention()).isEqualTo(Duration.ofDays(30));
        });
    }

    // Без outbox.inbox.retention приложение НЕ стартует. Слишком короткий срок сломал бы дедупликацию
    // при replay топика, а безопасного универсального значения нет. Поэтому стартер требует задать его явно.
    @Test
    void refusesToStartWithoutInboxRetention() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ReliableMessagingAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasRootCauseMessage(
                                    "outbox.inbox.retention must be configured explicitly"
                            );
                });
    }

    // Нет DataSource, значит нет и мигратора схемы: стартер ничего не делает там, где нет базы.
    @Test
    void doesNotModifyApplicationFlywayWithoutDataSource() {
        contextRunner.run(context -> assertThat(context).doesNotHaveBean(ReliableMessagingSchemaMigrator.class));
    }

    // send-timeout >= lease-duration запрещён. Иначе аренда истечёт прямо во время отправки, reaper отдаст
    // строку другому поду, и получится лишний дубль. Проверяется при старте.
    @Test
    void rejectsSendTimeoutAtLeaseBoundary() {
        contextRunner.withPropertyValues("outbox.relay.send-timeout=30s").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseMessage(
                    "outbox.relay.send-timeout must be shorter than outbox.relay.lease-duration");
        });
    }
}
