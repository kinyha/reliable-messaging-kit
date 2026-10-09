package dev.reliablemessaging.outbox.cleanup;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;
// ЧТО ПРОВЕРЯЕМ: фоновую очистку (RetentionCleaner).
// Данные: 1500 SENT старше 8 дней (по умолчанию cleanup.retention = 7d), 1 DEAD и 1 NEW той же давности,
// 1 свежий SENT; в inbox 1500 записей старше 31 дня (retention 30d) и 1 свежая.
// Ожидаем: старые SENT удалены (батчами по 1000, на 1500 нужно два прохода), старый inbox удалён.
// Остаются DEAD (ручной разбор, автоматически не удаляется никогда), NEW (ещё не отправлен) и свежий SENT.
@IntegrationTest
class RetentionCleanerIT {
    @Autowired RetentionCleaner cleaner;
    @Autowired JdbcTemplate jdbc;
    @Test void removesOnlyExpiredSentAndInboxInBatches() {
        jdbc.execute("truncate outbox_message,inbox_message");
        jdbc.execute("""
            insert into outbox_message(message_id,aggregate_type,aggregate_id,topic,partition_key,payload,status,sent_at)
            select gen_random_uuid(),'test',i::text,'events',i::text,'{}',
                case when i<=1500 then 'SENT' when i=1501 then 'DEAD' else 'NEW' end,
                now()-interval '8 days' from generate_series(1,1502) i
            """);
        jdbc.update("""
            insert into outbox_message(message_id,aggregate_type,aggregate_id,topic,partition_key,payload,status,sent_at)
            values (?,'test','fresh','events','fresh','{}','SENT',now())
            """,UUID.randomUUID());
        jdbc.execute("insert into inbox_message(message_id,consumer,processed_at) select gen_random_uuid(),'test',now()-interval '31 days' from generate_series(1,1500)");
        jdbc.update("insert into inbox_message(message_id,consumer) values (?,'test')",UUID.randomUUID());
        cleaner.clean();
        assertThat(jdbc.queryForList("select status from outbox_message",String.class)).containsExactlyInAnyOrder("DEAD","NEW","SENT");
        assertThat(jdbc.queryForObject("select count(*) from inbox_message",Integer.class)).isEqualTo(1);
    }
}
