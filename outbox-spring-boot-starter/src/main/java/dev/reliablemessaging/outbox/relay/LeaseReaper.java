package dev.reliablemessaging.outbox.relay;
import dev.reliablemessaging.outbox.config.OutboxProperties;
import org.slf4j.LoggerFactory;
import dev.reliablemessaging.outbox.metrics.OutboxMetrics;
public final class LeaseReaper extends BackgroundTask {
    private final OutboxRepository repository;
    private final int maxAttempts;
    private final OutboxMetrics metrics;
    public LeaseReaper(OutboxRepository repository, OutboxProperties.Relay settings) {
        this(repository,settings,OutboxMetrics.noop());
    }
    public LeaseReaper(OutboxRepository repository,OutboxProperties.Relay settings,OutboxMetrics metrics) {
        super("outbox-reaper",settings.reaperInterval()); this.repository=repository; maxAttempts=settings.maxAttempts(); this.metrics=metrics;
    }
    @Override protected void tick() {
        var result=repository.reclaimExpiredWithCounts(maxAttempts);
        int count=result.reclaimed();
        metrics.increment("outbox.relay.published",result.dead(),"result","dead");
        metrics.increment("outbox.relay.reclaimed",count);
        if(count>0) LoggerFactory.getLogger(LeaseReaper.class).warn("Reclaimed {} expired outbox leases",count);
    }
}
