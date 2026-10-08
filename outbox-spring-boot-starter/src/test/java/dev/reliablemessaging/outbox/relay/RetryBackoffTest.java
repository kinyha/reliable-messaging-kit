package dev.reliablemessaging.outbox.relay;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.Random;
import static org.assertj.core.api.Assertions.*;
class RetryBackoffTest {
    @Test void staysWithinExponentialEqualJitterBoundsAndHandlesHugeAttempts() {
        var backoff = new RetryBackoff(Duration.ofSeconds(2), Duration.ofMinutes(5), new Random(17));
        for (int attempt=1; attempt<=15; attempt++) {
            long exp = Math.min(300_000, 2000L << (attempt-1));
            for (int i=0; i<100; i++) assertThat(backoff.nextDelay(attempt).toMillis()).isBetween(exp/2, exp);
        }
        assertThat(backoff.nextDelay(Integer.MAX_VALUE).toMillis()).isBetween(150_000L,300_000L);
    }
    @Test void spreadsRetriesAcrossSeconds() {
        var backoff = new RetryBackoff(Duration.ofSeconds(2), Duration.ofMinutes(5), new Random(42));
        var buckets = new int[40];
        for (int i=0;i<10_000;i++) buckets[(int)backoff.nextDelay(5).toSeconds()]++;
        for (int n : buckets) assertThat(n).isLessThan(1000);
    }
}
