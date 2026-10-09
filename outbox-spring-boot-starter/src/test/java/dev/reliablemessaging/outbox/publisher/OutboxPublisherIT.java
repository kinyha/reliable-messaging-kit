package dev.reliablemessaging.outbox.publisher;
import dev.reliablemessaging.outbox.api.*;
import dev.reliablemessaging.outbox.support.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.IllegalTransactionStateException;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;
// ЧТО ПРОВЕРЯЕМ: шаг 1 протокола, запись события в outbox (JdbcOutboxPublisher).
// Главное свойство паттерна: событие пишется В ТОЙ ЖЕ транзакции, что и бизнес-данные.
// Коммит: в базе и бизнес-строка, и событие. Откат: ни того, ни другого.
// Отправки в Kafka здесь нет вообще (релэй выключен). Проверяем только «положили письмо в ящик исходящих».
@IntegrationTest
class OutboxPublisherIT {
    @Autowired OutboxPublisher publisher;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager tm;
    // Перед каждым тестом очищаем outbox, чтобы считать строки с нуля.
    @BeforeEach void clean() { jdbc.execute("truncate outbox_message"); }
    private OutboxMessage event() {
        return OutboxMessage.builder().topic("test-events").aggregate(new Aggregate("test", "a-1"))
                .payload(Map.of("value", 42)).header("custom", "yes").build();
    }
    // Сценарий: в одной транзакции дважды вызываем publish.
    // Ожидаем: две строки NEW, у каждой свой message_id (UUID). Payload и пользовательские заголовки
    // лежат как jsonb (поэтому их можно читать SQL-оператором ->>), partition_key равен id агрегата.
    @Test void writesJsonAndDistinctIdsInCallingTransaction() {
        new TransactionTemplate(tm).executeWithoutResult(status -> {
            assertThat(publisher.publish(event())).isNotEqualTo(publisher.publish(event()));
        });
        assertThat(jdbc.queryForList("select status, partition_key, payload->>'value' as value, headers->>'custom' as custom from outbox_message"))
                .hasSize(2).allSatisfy(row -> {
                    assertThat(row).containsEntry("status", "NEW").containsEntry("partition_key", "a-1")
                            .containsEntry("value", "42").containsEntry("custom", "yes");
                });
    }
    // Сценарий: публикуем событие, пишем «бизнес-эффект» в effects, затем откатываем транзакцию.
    // Ожидаем: обе таблицы пусты. Это и есть «нет события без заказа и нет заказа без события».
    @Test void rollbackRemovesBusinessChangeAndEvent() {
        new TransactionTemplate(tm).executeWithoutResult(status -> {
            var id = publisher.publish(event());
            jdbc.update("insert into effects values (?)", id);
            status.setRollbackOnly();
        });
        assertThat(jdbc.queryForObject("select count(*) from outbox_message", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("select count(*) from effects", Integer.class)).isZero();
    }
    // Сценарий: вызываем publish без транзакции.
    // Ожидаем: исключение сразу, строка не записана. Без этой проверки INSERT ушёл бы в autocommit
    // отдельно от бизнес-данных. Это тот самый dual-write, от которого мы защищаемся, только тихий.
    @Test void rejectsAutocommit() {
        assertThatThrownBy(() -> publisher.publish(event())).isInstanceOf(IllegalTransactionStateException.class);
        assertThat(jdbc.queryForObject("select count(*) from outbox_message", Integer.class)).isZero();
    }
}
