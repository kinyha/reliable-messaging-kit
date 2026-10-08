package dev.reliablemessaging.outbox.api;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Message identity, aggregate and Kafka coordinates. No aggregate sequence or delivery order is implied. */
public record MessageMeta(UUID messageId,String aggregateType,String aggregateId,Instant occurredAt,
                          String topic,int partition,long offset,Map<String,String> headers) {
    public MessageMeta {
        if(messageId==null) throw new IllegalArgumentException("Missing required Kafka header rm-message-id");
        headers=Map.copyOf(headers);
    }
}
