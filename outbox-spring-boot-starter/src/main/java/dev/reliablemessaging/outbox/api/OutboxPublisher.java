package dev.reliablemessaging.outbox.api;

import java.util.Map;
import java.util.UUID;

/**
 * Records an event in the outbox as part of the caller's database transaction.
 *
 * <p>The first implementation will reject calls made without an active transaction.</p>
 */
@FunctionalInterface
public interface OutboxPublisher {
    UUID publish(
            String topic,
            Aggregate aggregate,
            Object payload,
            Map<String, String> headers
    );

    default UUID publish(String topic, Aggregate aggregate, Object payload) {
        return publish(topic, aggregate, payload, Map.of());
    }
}
