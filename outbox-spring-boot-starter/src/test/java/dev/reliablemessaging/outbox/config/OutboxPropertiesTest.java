package dev.reliablemessaging.outbox.config;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.flyway.FlywayConfigurationCustomizer;
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
            assertThat(properties.relay().leaseDuration()).isEqualTo(Duration.ofSeconds(30));
            assertThat(properties.inbox().retention()).isEqualTo(Duration.ofDays(30));
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
    void addsStarterMigrationsToTheApplicationsFlywayConfiguration() {
        contextRunner.run(context -> {
            var configuration = Flyway.configure().locations("classpath:db/migration");

            context.getBean(FlywayConfigurationCustomizer.class).customize(configuration);

            assertThat(configuration.getLocations())
                    .extracting(location -> location.getDescriptor())
                    .containsExactly(
                            "classpath:db/migration",
                            "classpath:reliable-messaging/db/migration"
                    );
        });
    }
}
