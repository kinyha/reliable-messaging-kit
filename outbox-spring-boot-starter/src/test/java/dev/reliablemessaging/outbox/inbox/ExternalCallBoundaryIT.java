package dev.reliablemessaging.outbox.inbox;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;
@IntegrationTest
class ExternalCallBoundaryIT {
    @Autowired IdempotentExecutor executor;
    @Autowired JdbcTemplate jdbc;
    @Test void externalCallsSurviveRollbackAndCanRepeat() {
        jdbc.execute("truncate inbox_message,effects");
        var calls=new AtomicInteger(); var id=UUID.randomUUID();
        Runnable action=() -> {
            // Models a remote HTTP effect outside this database transaction.
            int attempt=calls.incrementAndGet(); jdbc.update("insert into effects values (?)",id);
            if(attempt==1) throw new IllegalStateException("rollback after external call");
        };
        assertThatThrownBy(() -> executor.execute("external",id,action)).isInstanceOf(IllegalStateException.class);
        executor.execute("external",id,action);
        assertThat(calls.get()).isEqualTo(2);
        assertThat(jdbc.queryForObject("select count(*) from effects",Integer.class)).isEqualTo(1);
    }
}
