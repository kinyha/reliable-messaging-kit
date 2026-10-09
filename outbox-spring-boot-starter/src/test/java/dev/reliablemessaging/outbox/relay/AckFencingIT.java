package dev.reliablemessaging.outbox.relay;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import java.time.*;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
// ЧТО ПРОВЕРЯЕМ: фенсинг подтверждения через claim_token.
// Ситуация из прода: под A захватил строку и «уснул» (GC-пауза, сеть) дольше аренды. Reaper вернул строку,
// её захватил под B. Потом A просыпается и пытается записать свой результат.
// A не должен перезаписать статус строки, которая теперь принадлежит B.
@IntegrationTest
class AckFencingIT {
    @Autowired JdbcTemplate jdbc;
    @Autowired OutboxRepository repository;
    @BeforeEach void clean() { jdbc.execute("truncate outbox_message"); }
    // Сценарий: A захватил строку, затем мы руками подменяем токен на B (имитация перехвата другим подом).
    // Ожидаем: и markSent, и markFailed от A обновляют 0 строк, строка остаётся IN_FLIGHT с токеном B,
    // а подтверждение от B проходит (1 строка).
    @Test void staleSuccessAndFailureCannotOverwriteAnotherClaim() {
        jdbc.update("insert into outbox_message(message_id,aggregate_type,aggregate_id,topic,partition_key,payload) values (?,'test','1','events','1','{}')", UUID.randomUUID());
        var a = UUID.randomUUID(); var b = UUID.randomUUID();
        var row = repository.claim(1, Duration.ofSeconds(30), a).getFirst();
        // имитация: строку «перехватил» под B
        jdbc.update("update outbox_message set claim_token=? where id=?", b, row.id());
        assertThat(repository.markSent(List.of(row.id()),a)).isZero();
        assertThat(repository.markFailed(row.id(),a,"boom",Instant.now(),true)).isZero();
        assertThat(jdbc.queryForMap("select status,claim_token from outbox_message where id=?",row.id()))
                .containsEntry("status","IN_FLIGHT").containsEntry("claim_token",b);
        assertThat(repository.markSent(List.of(row.id()),b)).isEqualTo(1);
    }
    // Ошибка длиной 3000 символов обрезается до 2000 в last_error: диагностика не раздувает таблицу.
    // Заодно видно, что markFailed с dead=true переводит строку в DEAD.
    @Test void failedAcknowledgementLimitsDiagnosticLength() {
        jdbc.update("insert into outbox_message(message_id,aggregate_type,aggregate_id,topic,partition_key,payload) values (?,'test','1','events','1','{}')", UUID.randomUUID());
        var token=UUID.randomUUID(); var row=repository.claim(1,Duration.ofSeconds(30),token).getFirst();
        assertThat(repository.markFailed(row.id(),token,"x".repeat(3000),Instant.now(),true)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select length(last_error) from outbox_message",Integer.class)).isEqualTo(2000);
        assertThat(jdbc.queryForObject("select status from outbox_message",String.class)).isEqualTo("DEAD");
    }
}
