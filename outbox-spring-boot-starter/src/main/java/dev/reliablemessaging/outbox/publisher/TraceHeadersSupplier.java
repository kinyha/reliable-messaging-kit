package dev.reliablemessaging.outbox.publisher;
import java.util.Map;
@FunctionalInterface
public interface TraceHeadersSupplier {
    Map<String, String> get();
}
