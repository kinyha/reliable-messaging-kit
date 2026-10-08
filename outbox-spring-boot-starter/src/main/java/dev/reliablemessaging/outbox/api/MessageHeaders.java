package dev.reliablemessaging.outbox.api;

/** Reserved UTF-8 Kafka headers used by the reliable messaging protocol. */
public final class MessageHeaders {
    public static final String MESSAGE_ID = "rm-message-id";
    public static final String AGGREGATE_TYPE = "rm-aggregate-type";
    public static final String AGGREGATE_ID = "rm-aggregate-id";
    public static final String OCCURRED_AT = "rm-occurred-at";
    private MessageHeaders() { }
}
