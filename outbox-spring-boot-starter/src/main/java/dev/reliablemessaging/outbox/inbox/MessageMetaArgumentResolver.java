package dev.reliablemessaging.outbox.inbox;
import dev.reliablemessaging.outbox.api.*;
import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.kafka.support.KafkaHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

public final class MessageMetaArgumentResolver implements HandlerMethodArgumentResolver {
    @Override public boolean supportsParameter(MethodParameter parameter) {
        return parameter.getParameterType()==MessageMeta.class;
    }
    @Override public Object resolveArgument(MethodParameter parameter,Message<?> message) {
        var headers=new LinkedHashMap<String,String>();
        message.getHeaders().forEach((name,value) -> headers.put(name,text(value)));
        var id=headers.get(MessageHeaders.MESSAGE_ID);
        if(id==null) throw new IllegalArgumentException("Missing required Kafka header rm-message-id");
        var time=headers.get(MessageHeaders.OCCURRED_AT);
        return new MessageMeta(UUID.fromString(id),headers.get(MessageHeaders.AGGREGATE_TYPE),headers.get(MessageHeaders.AGGREGATE_ID),
                time==null ? null : Instant.parse(time),headers.get(KafkaHeaders.RECEIVED_TOPIC),
                number(message.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION)).intValue(),
                number(message.getHeaders().get(KafkaHeaders.OFFSET)).longValue(),headers);
    }
    private static Number number(Object value) { return value instanceof Number n ? n : -1L; }
    private static String text(Object value) {
        return value instanceof byte[] bytes ? new String(bytes,StandardCharsets.UTF_8) : String.valueOf(value);
    }
}
