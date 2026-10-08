package dev.reliablemessaging.outbox.publisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

public final class PayloadSerializer {
    private final ObjectMapper mapper;
    public PayloadSerializer(ObjectMapper mapper) { this.mapper = mapper; }
    public String toJson(Object payload) {
        try { return mapper.writeValueAsString(payload); }
        catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Cannot serialize " + payload.getClass().getName(), ex);
        }
    }
    public String toJson(Map<String, String> headers) { return toJson((Object) headers); }
}
