package dev.reliablemessaging.outbox.api;

/** Identifies the domain aggregate that produced an event. */
public record Aggregate(String type, String id) {
    public Aggregate {
        type = requireText(type, "type");
        id = requireText(id, "id");
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("aggregate " + field + " must not be blank");
        }
        return value;
    }
}
