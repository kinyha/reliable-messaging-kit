package dev.reliablemessaging.outbox.config;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.assertj.core.api.Assertions.assertThat;
@IntegrationTest
class SchemaMigrationIT {
    @Autowired JdbcTemplate jdbc;
    @Test void isolatesBothV1Migrations() {
        assertThat(jdbc.queryForObject("select count(*) from flyway_schema_history where success", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from reliable_messaging_schema_history where success", Integer.class)).isGreaterThanOrEqualTo(2);
        assertThat(jdbc.queryForObject("select count(*) from information_schema.columns where table_name='outbox_message' and column_name='claim_token'", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForList("select * from effects")).isEmpty();
    }
}
