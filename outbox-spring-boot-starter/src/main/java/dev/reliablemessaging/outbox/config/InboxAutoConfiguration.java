package dev.reliablemessaging.outbox.config;
import dev.reliablemessaging.outbox.inbox.*;
import dev.reliablemessaging.outbox.metrics.OutboxMetrics;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import javax.sql.DataSource;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.context.annotation.Configuration;
@AutoConfiguration(after={OutboxPublisherAutoConfiguration.class,OutboxMetricsAutoConfiguration.class})
@ConditionalOnBean(DataSource.class)
public class InboxAutoConfiguration {
    @Bean @ConditionalOnMissingBean
    InboxRepository inboxRepository(DataSource ds) { return new InboxRepository(new JdbcTemplate(ds)); }
    @Bean @ConditionalOnMissingBean
    IdempotentExecutor idempotentExecutor(InboxRepository repository,PlatformTransactionManager tm,OutboxMetrics metrics) {
        return new IdempotentExecutor(repository,tm,metrics);
    }
    @Configuration(proxyBeanMethods=false)
    @ConditionalOnClass({KafkaListener.class,Aspect.class})
    static class ConsumerConfiguration {
        @Bean @ConditionalOnMissingBean
        IdempotentConsumerAspect idempotentConsumerAspect(IdempotentExecutor executor) { return new IdempotentConsumerAspect(executor); }
        @Bean @ConditionalOnMissingBean
        MessageMetaArgumentResolver messageMetaArgumentResolver() { return new MessageMetaArgumentResolver(); }
        @Bean
        KafkaListenerConfigurer reliableMessagingListenerConfigurer(MessageMetaArgumentResolver resolver) {
            return registrar -> registrar.setCustomMethodArgumentResolvers(resolver);
        }
    }
}
