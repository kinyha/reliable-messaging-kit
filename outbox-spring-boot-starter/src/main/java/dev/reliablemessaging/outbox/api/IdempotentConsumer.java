package dev.reliablemessaging.outbox.api;
import java.lang.annotation.*;

/**
 * Deduplicates the annotated handler using its stable consumer name and MessageMeta identity.
 * The method must accept MessageMeta. Only effects in the same database transaction are covered.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface IdempotentConsumer {
    /** Stable logical consumer name; changing it permits old messages to be processed again. */
    String name();
}
