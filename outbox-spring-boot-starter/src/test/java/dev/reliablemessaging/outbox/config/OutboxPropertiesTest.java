package dev.reliablemessaging.outbox.config;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;

import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxPropertiesTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ReliableMessagingAutoConfiguration.class))
            .withPropertyValues("outbox.inbox.retention=30d");

    @Test
    void defaultsAreExplicitAndConservative() {
        contextRunner.run(context -> {
            var properties = context.getBean(OutboxProperties.class);

            assertThat(properties.relay().batchSize()).isEqualTo(128);
            assertThat(properties.relay().fastEmptyHeaders()).isFalse();
            assertThat(properties.relay().virtualThreads()).isFalse();
            assertThat(properties.relay().leaseDuration()).isEqualTo(Duration.ofSeconds(30));
            assertThat(properties.inbox().retention()).isEqualTo(Duration.ofDays(30));
        });
    }

    @Test
    void bindsTheIndependentOptimizationSwitch() {
        contextRunner.withPropertyValues("outbox.relay.fast-empty-headers=true").run(context -> {
            assertThat(context.getBean(OutboxProperties.class).relay().fastEmptyHeaders()).isTrue();
            assertThat(context.getBean(OutboxProperties.class).relay().virtualThreads()).isFalse();
        });
    }

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

    @Test
    void doesNotModifyApplicationFlywayWithoutDataSource() {
        contextRunner.run(context -> assertThat(context).doesNotHaveBean(ReliableMessagingSchemaMigrator.class));
    }

    @Test
    void rejectsSendTimeoutAtLeaseBoundary() {
        contextRunner.withPropertyValues("outbox.relay.send-timeout=30s").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseMessage(
                    "outbox.relay.send-timeout must be shorter than outbox.relay.lease-duration");
        });
    }
}
