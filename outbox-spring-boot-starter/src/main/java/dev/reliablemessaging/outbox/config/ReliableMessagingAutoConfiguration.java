package dev.reliablemessaging.outbox.config;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.Location;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.flyway.FlywayConfigurationCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.stream.Stream;

@AutoConfiguration(before = FlywayAutoConfiguration.class)
@ConditionalOnClass(Flyway.class)
@EnableConfigurationProperties(OutboxProperties.class)
public class ReliableMessagingAutoConfiguration {
    @Bean
    @ConditionalOnProperty(
            prefix = "outbox.migrations",
            name = "enabled",
            havingValue = "true",
            matchIfMissing = true
    )
    FlywayConfigurationCustomizer reliableMessagingFlywayConfigurationCustomizer() {
        var starterLocation = new Location("classpath:reliable-messaging/db/migration");

        return configuration -> {
            var locations = Stream.concat(
                            Arrays.stream(configuration.getLocations()),
                            Stream.of(starterLocation)
                    )
                    .distinct()
                    .toArray(Location[]::new);
            configuration.locations(locations);
        };
    }
}
