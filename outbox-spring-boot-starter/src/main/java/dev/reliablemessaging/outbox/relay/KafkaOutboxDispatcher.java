package dev.reliablemessaging.outbox.relay;

import dev.reliablemessaging.outbox.api.MessageHeaders;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.kafka.core.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public final class KafkaOutboxDispatcher implements OutboxDispatcher, DisposableBean {
    private final DefaultKafkaProducerFactory<String, String> factory;
    private final KafkaTemplate<String, String> template;
    private final Duration timeout;
    public KafkaOutboxDispatcher(KafkaProperties properties, SslBundles bundles, Duration timeout) {
        this.timeout = timeout;
        var config = new HashMap<>(properties.buildProducerProperties(bundles));
        int millis = Math.toIntExact(timeout.toMillis());
        int linger = Math.min(5, millis - 1);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        config.put(ProducerConfig.LINGER_MS_CONFIG, linger);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, millis);
        // Kafka requires delivery.timeout.ms >= request.timeout.ms + linger.ms.
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.min(millis - linger, 30_000));
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (long) millis);
        factory = new DefaultKafkaProducerFactory<>(config);
        template = new KafkaTemplate<>(factory);
    }
    @Override public List<DispatchResult> dispatch(List<OutboxRecord> batch) {
        long deadline = System.nanoTime() + timeout.toNanos();
        var futures = new LinkedHashMap<Long, CompletableFuture<?>>();
        for (var row : batch) {
            if (System.nanoTime() >= deadline) {
                futures.put(row.id(), CompletableFuture.failedFuture(new TimeoutException("send timeout")));
                continue;
            }
            var record = new ProducerRecord<String, String>(row.topic(), row.partitionKey(), row.payload());
            row.headers().forEach((name, value) -> header(record, name, value));
            header(record, MessageHeaders.MESSAGE_ID, row.messageId().toString());
            header(record, MessageHeaders.AGGREGATE_TYPE, row.aggregateType());
            header(record, MessageHeaders.AGGREGATE_ID, row.aggregateId());
            header(record, MessageHeaders.OCCURRED_AT, row.createdAt().toString());
            try { futures.put(row.id(), template.send(record)); }
            catch (Exception ex) { futures.put(row.id(), CompletableFuture.failedFuture(ex)); }
        }
        try {
            long left = deadline - System.nanoTime();
            if (left > 0) CompletableFuture.allOf(futures.values().toArray(CompletableFuture[]::new)).get(left, TimeUnit.NANOSECONDS);
        } catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
        catch (ExecutionException | TimeoutException ex) { /* Resolve each record independently below. */ }
        var results = new ArrayList<DispatchResult>(batch.size());
        futures.forEach((id, future) -> {
            if (!future.isDone()) { results.add(new DispatchResult(id, false, "send timeout")); return; }
            try { future.join(); results.add(new DispatchResult(id, true, null)); }
            catch (CompletionException | CancellationException ex) {
                var cause = ex.getCause() == null ? ex : ex.getCause();
                results.add(new DispatchResult(id, false, cause instanceof TimeoutException ? "send timeout" : cause.toString()));
            }
        });
        return results;
    }
    private static void header(ProducerRecord<?, ?> record, String name, String value) {
        record.headers().remove(name); record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }
    @Override public void destroy() { factory.destroy(); }
}
