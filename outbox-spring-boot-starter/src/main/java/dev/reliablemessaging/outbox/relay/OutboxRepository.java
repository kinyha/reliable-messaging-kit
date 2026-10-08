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
import java.time.Instant;
import java.sql.Timestamp;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.slf4j.LoggerFactory;
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
    public record AckCounts(int sent, int failed, int dead, int fenced) { }
    public record Failure(long id, UUID token, String error, Instant nextAttemptAt, boolean dead) { }
    public int markSent(List<Long> ids, UUID token) {
        return transaction.execute(status -> updateSent(ids, token));
    }
    private int updateSent(List<Long> ids, UUID token) {
        if (ids.isEmpty()) return 0;
        return jdbc.update((PreparedStatementCreator) connection -> {
            var stmt = connection.prepareStatement("""
                    update outbox_message set status='SENT', sent_at=now(), locked_until=null,
                        claim_token=null, last_error=null
                    where id=any(?) and claim_token=? and status='IN_FLIGHT'
                    """);
            stmt.setArray(1, connection.createArrayOf("bigint", ids.toArray()));
            stmt.setObject(2, token); return stmt;
        });
    }
    public int markFailed(long id, UUID token, String error, Instant nextAttemptAt, boolean dead) {
        return transaction.execute(status -> updateFailed(List.of(new Failure(id, token, error, nextAttemptAt, dead)))[0]);
    }
    private int[] updateFailed(List<Failure> failures) {
        return jdbc.batchUpdate("""
                update outbox_message set status=?, next_attempt_at=?, last_error=?, locked_until=null, claim_token=null
                where id=? and claim_token=? and status='IN_FLIGHT'
                """, failures.stream().map(f -> new Object[] {f.dead() ? "DEAD" : "FAILED",
                    Timestamp.from(f.nextAttemptAt()), truncate(f.error()), f.id(), f.token()}).toList());
    }
    public AckCounts acknowledge(List<OutboxRecord> batch, List<DispatchResult> results, UUID token,
                                 int maxAttempts, RetryBackoff backoff) {
        var byId = new HashMap<Long, DispatchResult>();
        results.forEach(result -> byId.put(result.id(), result));
        var sentIds = new ArrayList<Long>(); var failures = new ArrayList<Failure>();
        for (var row : batch) {
            var result = byId.get(row.id());
            if (result != null && result.success()) sentIds.add(row.id());
            else failures.add(new Failure(row.id(), token, result == null ? "dispatcher returned no result" : result.error(),
                    Instant.now().plus(backoff.nextDelay(row.attempts())), row.attempts() >= maxAttempts));
        }
        return transaction.execute(status -> {
            int sent = updateSent(sentIds, token); int failed = 0; int dead = 0;
            int[] counts = updateFailed(failures);
            for (int i = 0; i < counts.length; i++) {
                if (counts[i] == 0) continue;
                if (failures.get(i).dead()) {
                    dead++;
                    long failedId=failures.get(i).id();
                    LoggerFactory.getLogger(OutboxRepository.class).warn("Outbox message id={} became DEAD: {}",
                            batch.stream().filter(row -> row.id()==failedId).findFirst().orElseThrow().messageId(), truncate(failures.get(i).error()));
                } else failed++;
            }
            int fenced = batch.size() - sent - failed - dead;
            if (fenced > 0) LoggerFactory.getLogger(OutboxRepository.class).debug("Fenced {} stale acknowledgements", fenced);
            return new AckCounts(sent, failed, dead, fenced);
        });
    }
    private static String truncate(String error) {
        return error == null ? "unknown error" : error.substring(0, Math.min(2000, error.length()));
    }
    public int reclaimExpired(int maxAttempts) {
        return transaction.execute(status -> jdbc.update("""
                with expired as (
                    select id from outbox_message where status='IN_FLIGHT' and locked_until < now()
                    order by locked_until,id limit 1000 for update skip locked
                )
                update outbox_message m
                   set status=case when m.attempts >= ? then 'DEAD' else 'NEW' end,
                       locked_until=null, claim_token=null, next_attempt_at=now(),
                       last_error=case when m.attempts >= ? then 'lease expired after final attempt' else m.last_error end
                  from expired where m.id=expired.id
                """,maxAttempts,maxAttempts));
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
