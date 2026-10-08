package dev.reliablemessaging.outbox.relay;
import java.time.Duration;
import java.util.random.RandomGenerator;
public final class RetryBackoff {
    private final Duration base;
    private final Duration max;
    private final RandomGenerator random;
    public RetryBackoff(Duration base, Duration max, RandomGenerator random) {
        this.base = base; this.max = max; this.random = random;
    }
    public Duration nextDelay(int attempts) {
        long ceiling = base.toMillis();
        for (int i = 1; i < attempts && ceiling < max.toMillis(); i++)
            ceiling = ceiling > max.toMillis() / 2 ? max.toMillis() : ceiling * 2;
        ceiling = Math.min(ceiling, max.toMillis());
        long floor = ceiling / 2;
        return Duration.ofMillis(floor + random.nextLong(ceiling - floor + 1));
    }
}
