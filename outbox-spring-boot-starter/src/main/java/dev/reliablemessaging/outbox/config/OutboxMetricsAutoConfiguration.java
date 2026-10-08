package dev.reliablemessaging.outbox.config;
import dev.reliablemessaging.outbox.metrics.OutboxMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import javax.sql.DataSource;
@AutoConfiguration(after=OutboxPublisherAutoConfiguration.class,before=OutboxRelayAutoConfiguration.class)
@ConditionalOnBean(DataSource.class)
public class OutboxMetricsAutoConfiguration {
    @Bean @ConditionalOnMissingBean
    OutboxMetrics outboxMetrics(DataSource ds,ObjectProvider<MeterRegistry> registry,OutboxProperties properties,
                               ObjectProvider<ReliableMessagingSchemaMigrator> migration) {
        migration.getIfAvailable();
        return new OutboxMetrics(new JdbcTemplate(ds),registry.getIfAvailable(),properties.metrics().refreshInterval());
    }
}
