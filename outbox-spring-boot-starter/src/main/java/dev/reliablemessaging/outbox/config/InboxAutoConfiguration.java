package dev.reliablemessaging.outbox.config;

import dev.reliablemessaging.outbox.inbox.IdempotentConsumerAspect;
import dev.reliablemessaging.outbox.inbox.IdempotentExecutor;
import dev.reliablemessaging.outbox.inbox.InboxRepository;
import dev.reliablemessaging.outbox.inbox.MessageMetaArgumentResolver;
import dev.reliablemessaging.outbox.metrics.OutboxMetrics;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@AutoConfiguration(after = {OutboxPublisherAutoConfiguration.class, OutboxMetricsAutoConfiguration.class})
@ConditionalOnBean(DataSource.class)
public class InboxAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    InboxRepository inboxRepository(DataSource ds) {
        return new InboxRepository(new JdbcTemplate(ds));
    }

    @Bean
    @ConditionalOnMissingBean
    IdempotentExecutor idempotentExecutor(InboxRepository repository, PlatformTransactionManager tm, OutboxMetrics metrics) {
        return new IdempotentExecutor(repository, tm, metrics);
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({KafkaListener.class, Aspect.class})
    static class ConsumerConfiguration {
        @Bean
        @ConditionalOnMissingBean
        IdempotentConsumerAspect idempotentConsumerAspect(IdempotentExecutor executor) {
            return new IdempotentConsumerAspect(executor);
        }

        @Bean
        @ConditionalOnMissingBean
        MessageMetaArgumentResolver messageMetaArgumentResolver() {
            return new MessageMetaArgumentResolver();
        }

        @Bean
        KafkaListenerConfigurer reliableMessagingListenerConfigurer(MessageMetaArgumentResolver resolver) {
            return registrar -> registrar.setCustomMethodArgumentResolvers(resolver);
        }
    }
}
