package dev.reliablemessaging.outbox.relay;

import dev.reliablemessaging.outbox.support.IntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

@IntegrationTest
class HeadersIT {
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager tm;
    @BeforeEach void clean() { jdbc.execute("truncate outbox_message"); }

    private void insert(String headers) {
        jdbc.update("""
                insert into outbox_message(message_id,aggregate_type,aggregate_id,topic,partition_key,payload,headers)
                values (?,'test','a','events','a','{}'::jsonb,?::jsonb)
                """, UUID.randomUUID(), headers);
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void readsEmptyHeadersAndPreservesTheClaimProtocol(boolean fast) {
        insert(" { } "); // PostgreSQL JSONB normalizes this to exactly {}.
        var token=UUID.randomUUID();
        var repository=new OutboxRepository(jdbc,tm,fast);
        var rows=repository.claim(10,Duration.ofSeconds(30),token);
        assertThat(rows).hasSize(1);
        var row=rows.getFirst();
        assertThat(row.headers()).isEmpty();
        assertThatThrownBy(() -> row.headers().put("new","value")).isInstanceOf(UnsupportedOperationException.class);
        assertThat(row.claimToken()).isEqualTo(token);
        assertThat(row.attempts()).isEqualTo(1);
        assertThat(repository.markSent(java.util.List.of(row.id()),token)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select status from outbox_message",String.class)).isEqualTo("SENT");
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void retainsUnicodeTraceAndEscapedHeaderValues(boolean fast) {
        insert("""
                {"traceparent":"00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
                 "имя":"значение", "escaped":"quote\\\" and newline\\n", "empty":""}
                """);
        var rows=new OutboxRepository(jdbc,tm,fast).claim(10,Duration.ofSeconds(30),UUID.randomUUID());
        assertThat(rows.getFirst().headers()).isEqualTo(Map.of(
                "traceparent","00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
                "имя","значение","escaped","quote\" and newline\n","empty",""));
        assertThatThrownBy(() -> rows.getFirst().headers().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void rejectsNonObjectHeadersAndRollsBackTheWholeClaim(boolean fast) {
        insert("{}"); insert("[]");
        assertThatThrownBy(() -> new OutboxRepository(jdbc,tm,fast).claim(10,Duration.ofSeconds(30),UUID.randomUUID()))
                .hasStackTraceContaining("Invalid outbox headers");
        assertThat(jdbc.queryForList("select status from outbox_message",String.class)).containsOnly("NEW");
        assertThat(jdbc.queryForList("select attempts from outbox_message",Integer.class)).containsOnly(0);
        assertThat(jdbc.queryForObject("select count(*) from outbox_message where claim_token is not null",Integer.class)).isZero();
    }
}
