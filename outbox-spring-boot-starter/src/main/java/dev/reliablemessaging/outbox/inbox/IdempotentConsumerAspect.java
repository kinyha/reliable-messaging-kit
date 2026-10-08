package dev.reliablemessaging.outbox.inbox;
import dev.reliablemessaging.outbox.api.*;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

@Aspect
public final class IdempotentConsumerAspect {
    private final IdempotentExecutor executor;
    public IdempotentConsumerAspect(IdempotentExecutor executor) { this.executor=executor; }
    @Around(value="@annotation(consumer)",argNames="joinPoint,consumer")
    public Object consume(ProceedingJoinPoint joinPoint,IdempotentConsumer consumer) throws Throwable {
        var meta=Arrays.stream(joinPoint.getArgs()).filter(MessageMeta.class::isInstance)
                .map(MessageMeta.class::cast).findFirst().orElseThrow(() -> new IllegalStateException(
                        "A method annotated with @IdempotentConsumer must accept MessageMeta"));
        var result=new AtomicReference<Object>();
        try {
            executor.execute(consumer.name(),meta.messageId(),() -> {
                try { result.set(joinPoint.proceed()); } catch(Throwable ex) { throw new ActionFailure(ex); }
            });
            return result.get();
        } catch(ActionFailure ex) { throw ex.getCause(); }
    }
    private static final class ActionFailure extends RuntimeException {
        private ActionFailure(Throwable cause) { super(cause); }
    }
}
