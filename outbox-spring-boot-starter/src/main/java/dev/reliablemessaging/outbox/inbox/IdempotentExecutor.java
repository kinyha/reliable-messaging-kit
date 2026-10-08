package dev.reliablemessaging.outbox.inbox;
import dev.reliablemessaging.outbox.metrics.OutboxMetrics;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.*;
import java.util.UUID;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Deduplicates database effects in the same transaction as the inbox insert. External calls are not covered. */
public final class IdempotentExecutor {
    private final InboxRepository repository;
    private final TransactionTemplate transaction;
    private final OutboxMetrics metrics;
    public IdempotentExecutor(InboxRepository repository,PlatformTransactionManager tm,OutboxMetrics metrics) {
        this.repository=repository; transaction=new TransactionTemplate(tm); this.metrics=metrics;
    }
    public boolean execute(String consumer,UUID messageId,Runnable action) {
        if(consumer==null || consumer.isBlank()) throw new IllegalArgumentException("consumer name must not be blank");
        Objects.requireNonNull(messageId,"messageId"); Objects.requireNonNull(action,"action");
        var reported=new AtomicBoolean();
        try {
            return Boolean.TRUE.equals(transaction.execute(status -> {
                if(!repository.tryRecord(messageId,consumer)) {
                    metrics.increment("inbox.messages",1,"consumer",consumer,"result","duplicate");
                    return false;
                }
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override public void afterCompletion(int completion) {
                        if(reported.compareAndSet(false,true)) metrics.increment("inbox.messages",1,"consumer",consumer,
                                "result",completion==STATUS_COMMITTED ? "processed" : "failed");
                    }
                });
                action.run(); return true;
            }));
        } catch(RuntimeException | Error ex) {
            if(reported.compareAndSet(false,true)) metrics.increment("inbox.messages",1,"consumer",consumer,"result","failed");
            throw ex;
        }
    }
}
