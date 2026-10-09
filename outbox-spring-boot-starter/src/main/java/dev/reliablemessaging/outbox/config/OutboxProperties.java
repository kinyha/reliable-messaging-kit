package dev.reliablemessaging.outbox.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import java.time.Duration;

@ConfigurationProperties("outbox")
public record OutboxProperties(
        @DefaultValue Migrations migrations, @DefaultValue Relay relay,
        @DefaultValue Cleanup cleanup, @DefaultValue Metrics metrics, @DefaultValue Inbox inbox) {
    public record Migrations(@DefaultValue("true") boolean enabled) { }

    public record Relay(
            @DefaultValue("true") boolean enabled, @DefaultValue("2") int workers,
            @DefaultValue("128") int batchSize, @DefaultValue("200ms") Duration pollInterval,
            @DefaultValue("30s") Duration leaseDuration, @DefaultValue("10s") Duration sendTimeout,
            @DefaultValue("10s") Duration reaperInterval, @DefaultValue("12") int maxAttempts,
            @DefaultValue("1s") Duration backoffBase, @DefaultValue("5m") Duration backoffMax,
            @DefaultValue("false") boolean virtualThreads) {
        /** Preserve the stage 1 constructor for programmatic configuration. */
        public Relay(boolean enabled,int workers,int batchSize,Duration pollInterval,Duration leaseDuration,
                     Duration sendTimeout,Duration reaperInterval,int maxAttempts,Duration backoffBase,Duration backoffMax) {
            this(enabled,workers,batchSize,pollInterval,leaseDuration,sendTimeout,reaperInterval,maxAttempts,backoffBase,backoffMax,false);
        }
        @org.springframework.boot.context.properties.bind.ConstructorBinding
        public Relay {
            positive(pollInterval, "relay.poll-interval"); positive(leaseDuration, "relay.lease-duration");
            positive(sendTimeout, "relay.send-timeout"); positive(reaperInterval, "relay.reaper-interval");
            positive(backoffBase, "relay.backoff-base"); positive(backoffMax, "relay.backoff-max");
            if (workers < 1 || batchSize < 1 || maxAttempts < 1)
                throw new IllegalArgumentException("outbox.relay workers, batch-size and max-attempts must be positive");
            if (sendTimeout.compareTo(leaseDuration) >= 0)
                throw new IllegalArgumentException("outbox.relay.send-timeout must be shorter than outbox.relay.lease-duration");
            if (backoffBase.compareTo(backoffMax) > 0)
                throw new IllegalArgumentException("outbox.relay.backoff-base must not exceed backoff-max");
            if (sendTimeout.toMillis() < 2 || sendTimeout.toMillis() > Integer.MAX_VALUE)
                throw new IllegalArgumentException("outbox.relay.send-timeout must fit Kafka milliseconds (2..2147483647)");
        }
    }
    public record Cleanup(@DefaultValue("7d") Duration retention,
                          @DefaultValue("1m") Duration interval, @DefaultValue("1000") int batchSize) {
        public Cleanup {
            positive(retention, "cleanup.retention"); positive(interval, "cleanup.interval");
            if (batchSize < 1) throw new IllegalArgumentException("outbox.cleanup.batch-size must be positive");
        }
    }
    public record Metrics(@DefaultValue("5s") Duration refreshInterval) {
        public Metrics { positive(refreshInterval, "metrics.refresh-interval"); }
    }
    /** Retention must cover the entire permitted Kafka redelivery and manual replay window. */
    public record Inbox(Duration retention) {
        public Inbox {
            if (retention == null)
                throw new IllegalArgumentException("outbox.inbox.retention must be configured explicitly");
            positive(retention, "inbox.retention");
        }
    }
    private static void positive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative() || value.toMillis() < 1)
            throw new IllegalArgumentException("outbox." + name + " must be positive and at least 1ms");
    }
}
