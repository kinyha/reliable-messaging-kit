package dev.reliablemessaging.outbox.cleanup;
import dev.reliablemessaging.outbox.config.OutboxProperties;
import dev.reliablemessaging.outbox.metrics.OutboxMetrics;
import dev.reliablemessaging.outbox.relay.*;
import org.springframework.jdbc.core.JdbcTemplate;
public final class RetentionCleaner extends BackgroundTask {
    private final JdbcTemplate jdbc;
    private final OutboxProperties properties;
    private final OutboxMetrics metrics;
    public RetentionCleaner(JdbcTemplate jdbc,OutboxProperties properties,OutboxMetrics metrics) {
        super("outbox-cleanup",properties.cleanup().interval()); this.jdbc=jdbc; this.properties=properties; this.metrics=metrics;
    }
    @Override protected void tick() { clean(); }
    public void clean() {
        deleteBatches("outbox", """
            delete from outbox_message where id in (
                select id from outbox_message where status='SENT' and sent_at<now()-?::interval limit ?)
            """,OutboxRepository.interval(properties.cleanup().retention()));
        deleteBatches("inbox", """
            delete from inbox_message where ctid in (
                select ctid from inbox_message where processed_at<now()-?::interval limit ?)
            """,OutboxRepository.interval(properties.inbox().retention()));
    }
    private void deleteBatches(String table,String sql,String retention) {
        int count;
        do {
            if(Thread.currentThread().isInterrupted()) return;
            count=jdbc.update(sql,retention,properties.cleanup().batchSize());
            metrics.increment("outbox.cleanup.deleted",count,"table",table);
        } while(count==properties.cleanup().batchSize());
    }
}
