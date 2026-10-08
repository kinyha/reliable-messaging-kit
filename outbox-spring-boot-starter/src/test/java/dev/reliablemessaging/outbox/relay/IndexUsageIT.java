package dev.reliablemessaging.outbox.relay;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.assertj.core.api.Assertions.*;
@IntegrationTest
class IndexUsageIT {
    @Autowired JdbcTemplate jdbc;
    @Test void plannerChoosesPartialIndexesWithoutDisablingSequentialScans() {
        jdbc.execute("truncate outbox_message");
        jdbc.execute("""
            insert into outbox_message(message_id,aggregate_type,aggregate_id,topic,partition_key,payload,status,locked_until)
            select gen_random_uuid(),'test',i::text,'events',i::text,'{}',
                case when i<=20000 then 'SENT' when i<=20050 then 'NEW' else 'IN_FLIGHT' end,
                case when i>20050 then now()-interval '1 second' end from generate_series(1,20100) i
            """);
        jdbc.execute("analyze outbox_message");
        assertThat(jdbc.queryForObject("""
            explain (format json) select id from outbox_message
            where status in ('NEW','FAILED') and next_attempt_at<=now()
            order by next_attempt_at,id limit 128 for update skip locked
            """,String.class)).contains("outbox_pending_idx");
        assertThat(jdbc.queryForObject("""
            explain (format json) select id from outbox_message
            where status='IN_FLIGHT' and locked_until<now()
            order by locked_until,id limit 1000 for update skip locked
            """,String.class)).contains("outbox_expired_lease_idx");
    }
}
