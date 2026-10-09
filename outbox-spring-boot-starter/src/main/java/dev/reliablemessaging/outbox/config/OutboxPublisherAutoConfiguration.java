package dev.reliablemessaging.outbox.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliablemessaging.outbox.api.OutboxPublisher;
import dev.reliablemessaging.outbox.publisher.JdbcOutboxPublisher;
import dev.reliablemessaging.outbox.publisher.PayloadSerializer;
import dev.reliablemessaging.outbox.publisher.TraceHeadersSupplier;
import dev.reliablemessaging.outbox.relay.OutboxRepository;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.Map;

@AutoConfiguration(after = ReliableMessagingAutoConfiguration.class)
@ConditionalOnBean(DataSource.class)
public class OutboxPublisherAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    OutboxRepository outboxRepository(DataSource ds, PlatformTransactionManager tm,
                                      ObjectProvider<ReliableMessagingSchemaMigrator> migration) {
        migration.getIfAvailable();
        return new OutboxRepository(new JdbcTemplate(ds), tm);
    }

    @Bean
    @ConditionalOnMissingBean
    PayloadSerializer payloadSerializer(ObjectProvider<ObjectMapper> mapper) {
        return new PayloadSerializer(mapper.getIfAvailable(() -> new ObjectMapper().findAndRegisterModules()));
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxPublisher outboxPublisher(DataSource ds, PayloadSerializer serializer, TraceHeadersSupplier traces,
                                    ObjectProvider<ReliableMessagingSchemaMigrator> migration) {
        migration.getIfAvailable();
        return new JdbcOutboxPublisher(new JdbcTemplate(ds), serializer, traces);
    }

    @Bean
    @ConditionalOnMissingBean
    TraceHeadersSupplier noopTraceHeadersSupplier() {
        return Map::of;
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({Tracer.class, Propagator.class})
    static class TracingConfiguration {
        @Bean
        @ConditionalOnMissingBean
        TraceHeadersSupplier traceHeadersSupplier(ObjectProvider<Tracer> tracers, ObjectProvider<Propagator> propagators) {
            return () -> {
                var tracer = tracers.getIfAvailable();
                var propagator = propagators.getIfAvailable();
                if (tracer == null || propagator == null || tracer.currentSpan() == null) return Map.of();
                var headers = new LinkedHashMap<String, String>();
                propagator.inject(tracer.currentSpan().context(), headers, Map::put);
                return headers;
            };
        }
    }
}
