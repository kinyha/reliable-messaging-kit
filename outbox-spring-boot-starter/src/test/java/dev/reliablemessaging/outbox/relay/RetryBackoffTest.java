package dev.reliablemessaging.outbox.relay;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.Random;
import static org.assertj.core.api.Assertions.*;
// ЧТО ПРОВЕРЯЕМ: расчёт паузы перед повтором (RetryBackoff). Чистый юнит-тест, без базы.
// Формула: потолок = min(max, base * 2^(attempt-1)), пауза = случайное число в [потолок/2, потолок].
// Это «equal jitter»: не меньше половины потолка, чтобы не долбить сразу, и со случайностью,
// чтобы повторы разных сообщений не совпадали по времени.
class RetryBackoffTest {
    // Для попыток 1..15 пауза всегда в своих границах. При огромном номере попытки нет переполнения:
    // пауза упирается в max (5 минут).
    @Test void staysWithinExponentialEqualJitterBoundsAndHandlesHugeAttempts() {
        var backoff = new RetryBackoff(Duration.ofSeconds(2), Duration.ofMinutes(5), new Random(17));
        for (int attempt=1; attempt<=15; attempt++) {
            long exp = Math.min(300_000, 2000L << (attempt-1));
            for (int i=0; i<100; i++) assertThat(backoff.nextDelay(attempt).toMillis()).isBetween(exp/2, exp);
        }
        assertThat(backoff.nextDelay(Integer.MAX_VALUE).toMillis()).isBetween(150_000L,300_000L);
    }
    // 10 000 повторов одной и той же 5-й попытки: ни в одну секундную «корзину» не попадает 10 % и больше.
    // То есть повторы размазаны по времени, а не бьют брокер одной волной.
    @Test void spreadsRetriesAcrossSeconds() {
        var backoff = new RetryBackoff(Duration.ofSeconds(2), Duration.ofMinutes(5), new Random(42));
        var buckets = new int[40];
        for (int i=0;i<10_000;i++) buckets[(int)backoff.nextDelay(5).toSeconds()]++;
        for (int n : buckets) assertThat(n).isLessThan(1000);
    }
}
