package dev.reliablemessaging.outbox.config;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.assertj.core.api.Assertions.assertThat;
// ЧТО ПРОВЕРЯЕМ: миграции стартера и приложения не мешают друг другу.
// У тестового «приложения» своя миграция V1__app.sql (таблица effects), у стартера свои V1 и V2.
// В общей истории Flyway две V1 конфликтовали бы, поэтому стартер ведёт отдельную историю
// в таблице reliable_messaging_schema_history.
// Ожидаем: в flyway_schema_history 1 миграция приложения, в истории стартера не меньше 2,
// колонка claim_token (добавлена в V2) существует.
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
