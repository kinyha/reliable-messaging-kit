package dev.reliablemessaging.outbox.publisher;

import dev.reliablemessaging.outbox.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import java.util.LinkedHashMap;
import java.util.UUID;

public final class JdbcOutboxPublisher implements OutboxPublisher {
    private final JdbcTemplate jdbc;
    private final PayloadSerializer serializer;
    private final TraceHeadersSupplier traces;
    public JdbcOutboxPublisher(JdbcTemplate jdbc, PayloadSerializer serializer, TraceHeadersSupplier traces) {
        this.jdbc = jdbc; this.serializer = serializer; this.traces = traces;
    }
    @Override public UUID publish(OutboxMessage message) {
        if (!TransactionSynchronizationManager.isActualTransactionActive())
            throw new IllegalTransactionStateException("OutboxPublisher.publish must be called inside an active transaction");
        // An unrelated transaction manager must not silently turn this insert into an autocommit.
        if (!TransactionSynchronizationManager.hasResource(jdbc.getDataSource()))
            throw new IllegalTransactionStateException("OutboxPublisher requires a transaction on its DataSource");
        var id = UUID.randomUUID();
        var headers = new LinkedHashMap<>(message.headers());
        headers.putAll(traces.get());
        jdbc.update("""
                insert into outbox_message
                  (message_id, aggregate_type, aggregate_id, topic, partition_key, payload, headers)
                values (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb)
                """, id, message.aggregate().type(), message.aggregate().id(), message.topic(),
                message.partitionKey(), serializer.toJson(message.payload()), serializer.toJson(headers));
        return id;
    }
}
