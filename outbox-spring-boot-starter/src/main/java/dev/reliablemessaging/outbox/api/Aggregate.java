package dev.reliablemessaging.outbox.api;

/** Identifies the domain aggregate that produced an event. */
public record Aggregate(String type, String id) {
}
