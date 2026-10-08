package dev.reliablemessaging.outbox.relay;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;

public final class OutboxRepository {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final ObjectMapper mapper = new ObjectMapper();
    public OutboxRepository(JdbcTemplate jdbc, PlatformTransactionManager tm) {
        this.jdbc = jdbc; transaction = new TransactionTemplate(tm);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }
    public List<OutboxRecord> claim(int batchSize, Duration lease, UUID claimToken) {
        return transaction.execute(status -> jdbc.query("""
                with picked as (
                    select id from outbox_message
                    where status in ('NEW', 'FAILED') and next_attempt_at <= now()
                    order by next_attempt_at, id limit ? for update skip locked
                )
                update outbox_message m
                   set status = 'IN_FLIGHT', locked_until = now() + ?::interval,
                       claim_token = ?, attempts = m.attempts + 1
                  from picked where m.id = picked.id returning m.*
                """, this::read, batchSize, interval(lease), claimToken));
    }
    public static String interval(Duration duration) { return duration.toMillis() + " milliseconds"; }
    private OutboxRecord read(ResultSet rs, int row) throws SQLException {
        try {
            Map<String, String> headers = mapper.readValue(rs.getString("headers"), new TypeReference<>() { });
            return new OutboxRecord(rs.getLong("id"), rs.getObject("message_id", UUID.class),
                    rs.getString("aggregate_type"), rs.getString("aggregate_id"), rs.getString("topic"),
                    rs.getString("partition_key"), rs.getString("payload"), Map.copyOf(headers),
                    rs.getInt("attempts"), rs.getTimestamp("created_at").toInstant(), rs.getObject("claim_token", UUID.class));
        } catch (com.fasterxml.jackson.core.JsonProcessingException ex) {
            throw new SQLException("Invalid outbox headers", ex);
        }
    }
}
