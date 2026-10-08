package dev.reliablemessaging.outbox.relay;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import java.time.*;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
@IntegrationTest
class AckFencingIT {
    @Autowired JdbcTemplate jdbc;
    @Autowired OutboxRepository repository;
    @BeforeEach void clean() { jdbc.execute("truncate outbox_message"); }
    @Test void staleSuccessAndFailureCannotOverwriteAnotherClaim() {
        jdbc.update("insert into outbox_message(message_id,aggregate_type,aggregate_id,topic,partition_key,payload) values (?,'test','1','events','1','{}')", UUID.randomUUID());
        var a = UUID.randomUUID(); var b = UUID.randomUUID();
        var row = repository.claim(1, Duration.ofSeconds(30), a).getFirst();
        jdbc.update("update outbox_message set claim_token=? where id=?", b, row.id());
        assertThat(repository.markSent(List.of(row.id()),a)).isZero();
        assertThat(repository.markFailed(row.id(),a,"boom",Instant.now(),true)).isZero();
        assertThat(jdbc.queryForMap("select status,claim_token from outbox_message where id=?",row.id()))
                .containsEntry("status","IN_FLIGHT").containsEntry("claim_token",b);
        assertThat(repository.markSent(List.of(row.id()),b)).isEqualTo(1);
    }
    @Test void failedAcknowledgementLimitsDiagnosticLength() {
        jdbc.update("insert into outbox_message(message_id,aggregate_type,aggregate_id,topic,partition_key,payload) values (?,'test','1','events','1','{}')", UUID.randomUUID());
        var token=UUID.randomUUID(); var row=repository.claim(1,Duration.ofSeconds(30),token).getFirst();
        assertThat(repository.markFailed(row.id(),token,"x".repeat(3000),Instant.now(),true)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select length(last_error) from outbox_message",Integer.class)).isEqualTo(2000);
        assertThat(jdbc.queryForObject("select status from outbox_message",String.class)).isEqualTo("DEAD");
    }
}
