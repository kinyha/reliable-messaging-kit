package dev.reliablemessaging.outbox.relay;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
public record OutboxRecord(long id, UUID messageId, String aggregateType, String aggregateId,
                           String topic, String partitionKey, String payload, Map<String, String> headers,
                           int attempts, Instant createdAt, UUID claimToken) { }
