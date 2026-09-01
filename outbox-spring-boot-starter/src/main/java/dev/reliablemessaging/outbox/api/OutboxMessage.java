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

    public static Builder builder() {
        return new Builder();
    }

    public String topic() {
        return topic;
    }

    public Aggregate aggregate() {
        return aggregate;
    }

    public String partitionKey() {
        return aggregate.id();
    }

    public Object payload() {
        return payload;
    }

    public Map<String, String> headers() {
        return headers;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    public static final class Builder {
        private String topic;
        private Aggregate aggregate;
        private Object payload;
        private final Map<String, String> headers = new LinkedHashMap<>();

        private Builder() {
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder aggregate(Aggregate aggregate) {
            this.aggregate = aggregate;
            return this;
        }

        public Builder payload(Object payload) {
            this.payload = payload;
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            this.headers.clear();
            this.headers.putAll(Objects.requireNonNull(headers, "headers must not be null"));
            return this;
        }

        public Builder header(String name, String value) {
            headers.put(
                    requireText(name, "header name"),
                    Objects.requireNonNull(value, "header value must not be null")
            );
            return this;
        }

        public OutboxMessage build() {
            return new OutboxMessage(this);
        }
    }
}
