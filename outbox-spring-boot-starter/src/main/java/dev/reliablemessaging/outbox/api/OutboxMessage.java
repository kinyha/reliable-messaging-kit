package dev.reliablemessaging.outbox.api;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An event that must be recorded in the outbox together with a business change.
 *
 * <p>The Kafka partition key is always the aggregate identifier. Keeping that
 * invariant inside the message prevents callers from accidentally breaking
 * per-aggregate partitioning.</p>
 */
public final class OutboxMessage {
    private final String topic;
    private final Aggregate aggregate;
    private final Object payload;
    private final Map<String, String> headers;

    private OutboxMessage(Builder builder) {
        topic = requireText(builder.topic, "topic");
        aggregate = Objects.requireNonNull(builder.aggregate, "aggregate must not be null");
        payload = Objects.requireNonNull(builder.payload, "payload must not be null");
        headers = Map.copyOf(builder.headers);
    }

    /** Creates a builder for an immutable event. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the destination Kafka topic. */
    public String topic() {
        return topic;
    }

    /** Returns the producing aggregate. */
    public Aggregate aggregate() {
        return aggregate;
    }

    /** Returns the aggregate identifier used as the Kafka key. */
    public String partitionKey() {
        return aggregate.id();
    }

    /** Returns the payload to serialize as JSON. */
    public Object payload() {
        return payload;
    }

    /** Returns an immutable snapshot of user headers. */
    public Map<String, String> headers() {
        return headers;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    /** Builds an event. The rm- header prefix is reserved. */
    public static final class Builder {
        private String topic;
        private Aggregate aggregate;
        private Object payload;
        private final Map<String, String> headers = new LinkedHashMap<>();

        private Builder() {
        }

        /** Sets the destination Kafka topic. */
        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        /** Sets the producing aggregate. */
        public Builder aggregate(Aggregate aggregate) {
            this.aggregate = aggregate;
            return this;
        }

        /** Sets the non-null JSON payload. */
        public Builder payload(Object payload) {
            this.payload = payload;
            return this;
        }

        /** Replaces user headers; rejects reserved names and null values. */
        public Builder headers(Map<String, String> headers) {
            this.headers.clear();
            Objects.requireNonNull(headers, "headers must not be null").forEach(this::header);
            return this;
        }

        /** Adds a UTF-8 user header; names starting with rm- are rejected. */
        public Builder header(String name, String value) {
            if (requireText(name, "header name").startsWith("rm-")) {
                throw new IllegalArgumentException("header prefix rm- is reserved");
            }
            headers.put(
                    requireText(name, "header name"),
                    Objects.requireNonNull(value, "header value must not be null")
            );
            return this;
        }

        /** Validates fields and creates an immutable event. */
        public OutboxMessage build() {
            return new OutboxMessage(this);
        }
    }
}
