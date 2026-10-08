package dev.reliablemessaging.outbox.inbox;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;
@IntegrationTest
class IdempotentExecutorIT {
    @Autowired IdempotentExecutor executor;
    @Autowired JdbcTemplate jdbc;
    @BeforeEach void clean() { jdbc.execute("truncate inbox_message,effects"); }
    @Test void sameIdIsDeduplicatedPerConsumer() {
        var id=UUID.randomUUID(); var effects=new AtomicInteger();
        assertThat(executor.execute("one",id,effects::incrementAndGet)).isTrue();
        assertThat(executor.execute("one",id,effects::incrementAndGet)).isFalse();
        assertThat(executor.execute("two",id,effects::incrementAndGet)).isTrue();
        assertThat(effects.get()).isEqualTo(2);
    }
    @Test void failedActionRollsBackInboxAndDatabaseEffect() {
        var id=UUID.randomUUID();
        assertThatThrownBy(() -> executor.execute("one",id,() -> {
            jdbc.update("insert into effects values (?)",id); throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("select count(*) from inbox_message",Integer.class)).isZero();
        assertThat(jdbc.queryForObject("select count(*) from effects",Integer.class)).isZero();
        assertThat(executor.execute("one",id,() -> jdbc.update("insert into effects values (?)",id))).isTrue();
    }
    @Test void eightConcurrentDeliveriesApplyOneDatabaseEffect() throws Exception {
        var id=UUID.randomUUID(); var barrier=new CyclicBarrier(8);
        try(var pool=Executors.newFixedThreadPool(8)) {
            var futures=new ArrayList<Future<Boolean>>();
            for(int i=0;i<8;i++) futures.add(pool.submit(() -> {
                barrier.await(); return executor.execute("one",id,() -> jdbc.update("insert into effects values (?)",id));
            }));
            int applied=0; for(var future:futures) if(future.get(30,TimeUnit.SECONDS)) applied++;
            assertThat(applied).isEqualTo(1);
        }
        assertThat(jdbc.queryForObject("select count(*) from effects",Integer.class)).isEqualTo(1);
    }
}
