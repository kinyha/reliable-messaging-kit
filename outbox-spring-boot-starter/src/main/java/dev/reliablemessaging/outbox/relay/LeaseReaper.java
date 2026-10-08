package dev.reliablemessaging.outbox.relay;
import dev.reliablemessaging.outbox.config.OutboxProperties;
import org.slf4j.LoggerFactory;
public final class LeaseReaper extends BackgroundTask {
    private final OutboxRepository repository;
    private final int maxAttempts;
    public LeaseReaper(OutboxRepository repository, OutboxProperties.Relay settings) {
        super("outbox-reaper",settings.reaperInterval()); this.repository=repository; maxAttempts=settings.maxAttempts();
    }
    @Override protected void tick() {
        int count=repository.reclaimExpired(maxAttempts);
        if(count>0) LoggerFactory.getLogger(LeaseReaper.class).warn("Reclaimed {} expired outbox leases",count);
    }
}
