package dev.reliablemessaging.outbox.relay;
import dev.reliablemessaging.outbox.config.OutboxProperties;
import org.slf4j.LoggerFactory;
import dev.reliablemessaging.outbox.metrics.OutboxMetrics;
import org.springframework.context.SmartLifecycle;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class OutboxRelay implements SmartLifecycle {
    private final OutboxRepository repository;
    private final OutboxDispatcher dispatcher;
    private final OutboxProperties.Relay settings;
    private final RetryBackoff backoff;
    private final OutboxMetrics metrics;
    private volatile boolean running;
    private ExecutorService executor;
    public OutboxRelay(OutboxRepository repository, OutboxDispatcher dispatcher,
                       OutboxProperties.Relay settings, RetryBackoff backoff) {
        this(repository,dispatcher,settings,backoff,OutboxMetrics.noop());
    }
    public OutboxRelay(OutboxRepository repository,OutboxDispatcher dispatcher,
                       OutboxProperties.Relay settings,RetryBackoff backoff,OutboxMetrics metrics) {
        this.repository=repository; this.dispatcher=dispatcher; this.settings=settings; this.backoff=backoff; this.metrics=metrics;
    }
    @Override public synchronized void start() {
        if(running) return;
        running=true;
        executor=Executors.newFixedThreadPool(settings.workers(),namedFactory(settings.virtualThreads()));
        LoggerFactory.getLogger(OutboxRelay.class).info("Starting relay: workers={}, virtualThreads={}, batchSize={}, pollInterval={}",
                settings.workers(),settings.virtualThreads(),settings.batchSize(),settings.pollInterval());
        for(int i=0;i<settings.workers();i++) executor.submit(this::work);
    }
    static ThreadFactory namedFactory(boolean virtual) {
        if(virtual) return Thread.ofVirtual().name("outbox-relay-",1).factory();
        var number=new AtomicInteger();
        return action -> new Thread(action,"outbox-relay-" + number.incrementAndGet());
    }
    private void work() {
        while(running && !Thread.currentThread().isInterrupted()) {
            try {
                var token=UUID.randomUUID();
                var batch=repository.claim(settings.batchSize(),settings.leaseDuration(),token);
                if(!batch.isEmpty()) {
                    metrics.increment("outbox.relay.claimed",batch.size());
                    var results=metrics.dispatch(() -> dispatcher.dispatch(batch));
                    metrics.acknowledged(repository.acknowledge(batch,results,token,settings.maxAttempts(),backoff));
                }
                if(batch.size()<settings.batchSize()) pause();
            } catch(InterruptedException ex) { Thread.currentThread().interrupt(); break; }
            catch(Exception ex) {
                LoggerFactory.getLogger(OutboxRelay.class).warn("Relay iteration failed",ex);
                try { pause(); } catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); break; }
            }
        }
    }
    private void pause() throws InterruptedException { Thread.sleep(settings.pollInterval().toMillis()); }
    @Override public synchronized void stop() {
        running=false;
        if(executor==null) return;
        executor.shutdown();
        try {
            if(!executor.awaitTermination(settings.sendTimeout().toMillis()*2,TimeUnit.MILLISECONDS)) executor.shutdownNow();
        } catch(InterruptedException ex) { executor.shutdownNow(); Thread.currentThread().interrupt(); }
    }
    @Override public boolean isRunning() { return running; }
    @Override public int getPhase() { return Integer.MAX_VALUE-100; }
}
