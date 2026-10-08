package dev.reliablemessaging.outbox.config;
import dev.reliablemessaging.outbox.cleanup.RetentionCleaner;
import dev.reliablemessaging.outbox.metrics.OutboxMetrics;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import javax.sql.DataSource;
@AutoConfiguration(after=InboxAutoConfiguration.class)
@ConditionalOnBean(DataSource.class)
public class OutboxCleanupAutoConfiguration {
    @Bean @ConditionalOnMissingBean
    RetentionCleaner retentionCleaner(DataSource ds,OutboxProperties properties,OutboxMetrics metrics) {
        return new RetentionCleaner(new JdbcTemplate(ds),properties,metrics);
    }
}
