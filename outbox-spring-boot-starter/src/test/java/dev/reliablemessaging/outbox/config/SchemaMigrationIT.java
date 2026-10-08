package dev.reliablemessaging.outbox.config;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.assertj.core.api.Assertions.assertThat;
@IntegrationTest
class SchemaMigrationIT implements Containers {
    @Autowired JdbcTemplate jdbc;
    @Test void isolatesBothV1Migrations() {
        assertThat(jdbc.queryForObject("select count(*) from flyway_schema_history where success", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from reliable_messaging_schema_history where success", Integer.class)).isGreaterThanOrEqualTo(2);
        assertThat(jdbc.queryForList("select claim_token from outbox_message")).isEmpty();
        assertThat(jdbc.queryForList("select * from effects")).isEmpty();
    }
}
