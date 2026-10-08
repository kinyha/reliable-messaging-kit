package dev.reliablemessaging.outbox.relay;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import java.time.Duration;
import java.util.concurrent.*;

/** Runs a recoverable maintenance operation with bounded shutdown. */
public abstract class BackgroundTask implements SmartLifecycle {
    private final Duration interval;
    private final String name;
    private volatile boolean running;
    private ScheduledExecutorService executor;
    protected BackgroundTask(String name, Duration interval) { this.name=name; this.interval=interval; }
    protected abstract void tick();
    @Override public synchronized void start() {
        if (running) return;
        running=true;
        executor=Executors.newSingleThreadScheduledExecutor(r -> new Thread(r,name));
        executor.scheduleWithFixedDelay(() -> {
            try { tick(); }
            catch (Exception ex) { LoggerFactory.getLogger(getClass()).warn("Maintenance task failed: " + name,ex); }
        },0,interval.toMillis(),TimeUnit.MILLISECONDS);
    }
    @Override public synchronized void stop() {
        running=false;
        if(executor!=null) executor.shutdownNow();
    }
    @Override public boolean isRunning() { return running; }
    @Override public int getPhase() { return Integer.MAX_VALUE-101; }
}
