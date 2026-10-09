package dev.reliablemessaging.demo.order;
import dev.reliablemessaging.outbox.relay.OutboxDispatcher;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.*;
import org.springframework.kafka.core.*;
import java.time.Duration;
import org.springframework.boot.convert.DurationStyle;
import java.util.HashMap;
@Configuration(proxyBeanMethods=false)
public class DemoConfiguration {
    @Bean NewTopic ordersTopic() { return new NewTopic("orders.v1",6,(short)1); }
    @Bean @ConditionalOnProperty(prefix="demo",name="delivery-mode",havingValue="naive")
    DefaultKafkaProducerFactory<String,String> naiveProducerFactory(KafkaProperties properties) {
        var config=new HashMap<>(properties.buildProducerProperties());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG,"1");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,false);
        config.put(ProducerConfig.LINGER_MS_CONFIG,1000);
        return new DefaultKafkaProducerFactory<>(config);
    }
    @Bean("naiveKafkaTemplate") @ConditionalOnProperty(prefix="demo",name="delivery-mode",havingValue="naive")
    KafkaTemplate<String,String> naiveKafkaTemplate(DefaultKafkaProducerFactory<String,String> factory) { return new KafkaTemplate<>(factory); }
    @Bean static BeanPostProcessor delayedDispatcher(@Value("${demo.chaos.publish-delay:0ms}") String configuredDelay) {
        var delay=DurationStyle.detectAndParse(configuredDelay).plusSeconds(2);
        return new BeanPostProcessor() {
            @Override public Object postProcessAfterInitialization(Object bean,String name) {
                if(!(bean instanceof OutboxDispatcher dispatcher) || delay.isZero()) return bean;
                return (OutboxDispatcher) batch -> {
                    // Delay only the first claim: induce an expired lease, then allow recovery to converge.
                    if(batch.stream().anyMatch(row -> row.attempts()==1)) {
                        try { Thread.sleep(delay.toMillis()); }
                        catch(InterruptedException ex) { Thread.currentThread().interrupt(); throw new IllegalStateException("Chaos delay interrupted",ex); }
                    }
                    return dispatcher.dispatch(batch);
                };
            }
        };
    }
}
