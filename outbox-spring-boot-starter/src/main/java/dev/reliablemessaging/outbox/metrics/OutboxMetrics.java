package dev.reliablemessaging.outbox.metrics;
import dev.reliablemessaging.outbox.relay.*;
import io.micrometer.core.instrument.*;
import org.springframework.jdbc.core.JdbcTemplate;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public final class OutboxMetrics extends BackgroundTask {
    private record Snapshot(long pending,long inFlight,long dead,double age) { }
    private final AtomicReference<Snapshot> snapshot=new AtomicReference<>(new Snapshot(0,0,0,0));
    private final MeterRegistry registry;
    private final JdbcTemplate jdbc;
    public OutboxMetrics(JdbcTemplate jdbc, MeterRegistry registry, Duration interval) {
        super("outbox-metrics",interval); this.jdbc=jdbc; this.registry=registry;
        if(registry!=null) {
            Gauge.builder("outbox.messages.pending",snapshot,s -> s.get().pending()).register(registry);
            Gauge.builder("outbox.messages.in_flight",snapshot,s -> s.get().inFlight()).register(registry);
            Gauge.builder("outbox.messages.dead",snapshot,s -> s.get().dead()).register(registry);
            Gauge.builder("outbox.oldest_pending.age",snapshot,s -> s.get().age()).baseUnit("seconds").register(registry);
            for(var result : new String[]{"sent","failed","dead"}) increment("outbox.relay.published",0,"result",result);
            for(var name : new String[]{"claimed","reclaimed","fenced"}) increment("outbox.relay."+name,0);
            for(var table : new String[]{"outbox","inbox"}) increment("outbox.cleanup.deleted",0,"table",table);
        }
    }
    public static OutboxMetrics noop() { return new OutboxMetrics(null,null,Duration.ofSeconds(5)); }
    @Override protected void tick() { refresh(); }
    public void refresh() {
        if(registry==null) return;
        snapshot.set(jdbc.queryForObject("""
            select count(*) filter(where status in ('NEW','FAILED')) as pending,
                   count(*) filter(where status='IN_FLIGHT') as in_flight,
                   count(*) filter(where status='DEAD') as dead,
                   coalesce(extract(epoch from now()-min(created_at) filter(where status in ('NEW','FAILED'))),0) as age
            from outbox_message
            """,(rs,n) -> new Snapshot(rs.getLong("pending"),rs.getLong("in_flight"),rs.getLong("dead"),rs.getDouble("age"))));
    }
    public void increment(String name,double count,String... tags) {
        if(registry!=null) registry.counter(name,tags).increment(count);
    }
    public <T> T dispatch(Supplier<T> action) {
        return registry==null ? action.get() : registry.timer("outbox.relay.dispatch").record(action);
    }
    public void acknowledged(OutboxRepository.AckCounts counts) {
        increment("outbox.relay.published",counts.sent(),"result","sent");
        increment("outbox.relay.published",counts.failed(),"result","failed");
        increment("outbox.relay.published",counts.dead(),"result","dead");
        increment("outbox.relay.fenced",counts.fenced());
    }
}
