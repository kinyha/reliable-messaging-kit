package dev.reliablemessaging.demo.payment;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliablemessaging.outbox.api.MessageHeaders;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.util.backoff.FixedBackOff;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
@Configuration(proxyBeanMethods=false)
public class DemoConfiguration {
    @Bean NewTopic ordersTopic() { return new NewTopic("orders.v1",6,(short)1); }
    @Bean NewTopic deadLettersTopic() { return new NewTopic("orders.v1.DLT",6,(short)1); }
    @Bean StringJsonMessageConverter converter(ObjectMapper mapper) { return new StringJsonMessageConverter(mapper); }
    @Bean RecordInterceptor<Object,Object> deliveryLog(JdbcTemplate jdbc) {
        return (record,consumer) -> {
            var header=record.headers().lastHeader(MessageHeaders.MESSAGE_ID);
            if(header!=null) {
                try {
                    var id=UUID.fromString(new String(header.value(),StandardCharsets.UTF_8));
                    jdbc.update("insert into delivery_log(message_id) values (?)",id);
                } catch(IllegalArgumentException ex) {
                    // Preserve malformed records so the listener error handler can route them to the DLT.
                }
            }
            return record;
        };
    }
    @Bean DefaultErrorHandler errorHandler(KafkaTemplate<Object,Object> template) {
        return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template,
                (record,error) -> new TopicPartition("orders.v1.DLT",record.partition())),new FixedBackOff(1000,3));
    }
    @Bean @ConditionalOnProperty(prefix="demo",name="consumer-mode",havingValue="idempotent",matchIfMissing=true)
    PaymentListeners.Idempotent idempotentListener(JdbcTemplate jdbc,io.micrometer.core.instrument.MeterRegistry registry) {
        return new PaymentListeners.Idempotent(jdbc,registry);
    }
    @Bean @ConditionalOnProperty(prefix="demo",name="consumer-mode",havingValue="naive")
    PaymentListeners.Naive naiveListener(JdbcTemplate jdbc) { return new PaymentListeners.Naive(jdbc); }
}
