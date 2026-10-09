package dev.reliablemessaging.outbox.inbox;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;
// ЧТО ПРОВЕРЯЕМ: ГРАНИЦУ гарантии. Это не баг, а зафиксированное ограничение (design, раздел 7).
// Однократен только эффект в той же транзакции Postgres, что и запись в inbox.
// Внешний вызов (HTTP к платёжному провайдеру, письмо) транзакция не откатывает.
@IntegrationTest
class ExternalCallBoundaryIT {
    @Autowired IdempotentExecutor executor;
    @Autowired JdbcTemplate jdbc;
    // Сценарий: обработчик делает «внешний вызов» (счётчик calls), пишет эффект в базу и на первой попытке падает.
    // Первая попытка: транзакция откатилась, эффекта и записи в inbox нет, но внешний вызов УЖЕ случился.
    // Вторая попытка (повторная доставка) проходит успешно.
    // Итог: эффект в базе 1 (гарантия держится), внешних вызовов 2 (гарантия на них не распространяется).
    // Вывод для пользователя: у внешнего сервиса должен быть свой ключ идемпотентности.
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
