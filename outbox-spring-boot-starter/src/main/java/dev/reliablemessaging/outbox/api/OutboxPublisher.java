package dev.reliablemessaging.outbox.api;

import java.util.UUID;

/**
 * Records an event in the outbox as part of the caller's database transaction.
 *
 * <p>Calls require an active transaction on the publisher's DataSource.
 * Rollback removes both the event and the caller's database changes.</p>
 */
@FunctionalInterface
public interface OutboxPublisher {
    /** Returns the identity recorded durably with the business transaction. */
    UUID publish(OutboxMessage message);
}
