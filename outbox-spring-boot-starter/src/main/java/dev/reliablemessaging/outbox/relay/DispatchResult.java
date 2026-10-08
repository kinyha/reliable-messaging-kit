package dev.reliablemessaging.outbox.relay;
public record DispatchResult(long id, boolean success, String error) { }
