package dev.reliablemessaging.outbox.config;

import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.InitializingBean;
import javax.sql.DataSource;

public final class ReliableMessagingSchemaMigrator implements InitializingBean {
    private final DataSource dataSource;
    public ReliableMessagingSchemaMigrator(DataSource dataSource) { this.dataSource = dataSource; }
    @Override public void afterPropertiesSet() {
        Flyway.configure().dataSource(dataSource).locations("classpath:reliable-messaging/db/migration")
                .table("reliable_messaging_schema_history").baselineOnMigrate(true).baselineVersion("0")
                .load().migrate();
    }
}
