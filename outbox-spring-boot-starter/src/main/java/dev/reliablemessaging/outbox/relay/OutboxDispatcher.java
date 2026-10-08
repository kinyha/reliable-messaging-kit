package dev.reliablemessaging.outbox.relay;
import java.util.List;
/** Dispatches outside database transactions; implementations may be replaced by the application. */
@FunctionalInterface
public interface OutboxDispatcher {
    List<DispatchResult> dispatch(List<OutboxRecord> batch);
}
