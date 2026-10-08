package dev.reliablemessaging.outbox.config;

import org.flywaydb.core.Flyway;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.*;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.annotation.Bean;
import javax.sql.DataSource;

@AutoConfiguration(after = {DataSourceAutoConfiguration.class, FlywayAutoConfiguration.class})
@EnableConfigurationProperties(OutboxProperties.class)
public class ReliableMessagingAutoConfiguration {
    @Bean
    @DependsOnDatabaseInitialization
    @ConditionalOnClass(Flyway.class)
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "outbox.migrations", name = "enabled", havingValue = "true", matchIfMissing = true)
    ReliableMessagingSchemaMigrator reliableMessagingSchemaMigrator(DataSource dataSource) {
        return new ReliableMessagingSchemaMigrator(dataSource);
    }
}
