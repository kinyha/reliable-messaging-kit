package dev.reliablemessaging.outbox.inbox;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.UUID;
public final class InboxRepository {
    private final JdbcTemplate jdbc;
    public InboxRepository(JdbcTemplate jdbc) { this.jdbc=jdbc; }
    public boolean tryRecord(UUID messageId,String consumer) {
        return jdbc.update("insert into inbox_message(message_id,consumer) values (?,?) on conflict do nothing",messageId,consumer)==1;
    }
}
