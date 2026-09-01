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
            .withConfiguration(AutoConfigurations.of(ReliableMessagingAutoConfiguration.class));

    @Test
    void defaultsAreExplicitAndConservative() {
        contextRunner.run(context -> {
            var properties = context.getBean(OutboxProperties.class);

            assertThat(properties.relay().batchSize()).isEqualTo(128);
            assertThat(properties.relay().leaseDuration()).isEqualTo(Duration.ofSeconds(30));
            assertThat(properties.inbox().retention()).isNull();
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
