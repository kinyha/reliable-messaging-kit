package dev.reliablemessaging.outbox.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

@ConfigurationProperties("outbox")
public record OutboxProperties(
        @DefaultValue Migrations migrations,
        @DefaultValue Relay relay,
        @DefaultValue Cleanup cleanup,
        @DefaultValue Inbox inbox
) {
    public record Migrations(@DefaultValue("true") boolean enabled) {
    }

    public record Relay(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("2") int workers,
            @DefaultValue("128") int batchSize,
            @DefaultValue("200ms") Duration pollInterval,
            @DefaultValue("30s") Duration leaseDuration,
            @DefaultValue("12") int maxAttempts
    ) {
    }

    public record Cleanup(@DefaultValue("7d") Duration retention) {
    }

    public record Inbox(
            // No unsafe default: this must be at least as long as the replay/redelivery window.
            Duration retention
    ) {
    }
}
