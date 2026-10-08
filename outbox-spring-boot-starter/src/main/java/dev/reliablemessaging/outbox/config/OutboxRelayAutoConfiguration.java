package dev.reliablemessaging.outbox.config;
import dev.reliablemessaging.outbox.relay.*;
import dev.reliablemessaging.outbox.metrics.OutboxMetrics;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.boot.autoconfigure.kafka.*;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import javax.sql.DataSource;
import java.util.random.RandomGenerator;

@AutoConfiguration(after={OutboxPublisherAutoConfiguration.class,KafkaAutoConfiguration.class})
@ConditionalOnClass(KafkaTemplate.class)
@ConditionalOnBean(DataSource.class)
@ConditionalOnProperty(prefix="outbox.relay",name="enabled",havingValue="true",matchIfMissing=true)
public class OutboxRelayAutoConfiguration {
    @Bean @ConditionalOnMissingBean
    OutboxDispatcher outboxDispatcher(KafkaProperties kafka, ObjectProvider<SslBundles> ssl, OutboxProperties properties, KafkaConnectionDetails details) {
        return new KafkaOutboxDispatcher(kafka,ssl.getIfAvailable(),properties.relay().sendTimeout(),details);
    }
    @Bean @ConditionalOnMissingBean
    RetryBackoff retryBackoff(OutboxProperties properties) {
        return new RetryBackoff(properties.relay().backoffBase(),properties.relay().backoffMax(),RandomGenerator.getDefault());
    }
    @Bean @ConditionalOnMissingBean
    OutboxRelay outboxRelay(OutboxRepository repository, OutboxDispatcher dispatcher, OutboxProperties properties, RetryBackoff backoff, OutboxMetrics metrics) {
        return new OutboxRelay(repository,dispatcher,properties.relay(),backoff,metrics);
    }
    @Bean @ConditionalOnMissingBean
    LeaseReaper leaseReaper(OutboxRepository repository, OutboxProperties properties, OutboxMetrics metrics) {
        return new LeaseReaper(repository,properties.relay(),metrics);
    }
}
