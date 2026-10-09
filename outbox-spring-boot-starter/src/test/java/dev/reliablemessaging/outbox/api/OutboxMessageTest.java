package dev.reliablemessaging.outbox.api;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// ЧТО ПРОВЕРЯЕМ: объект события OutboxMessage. Юнит-тесты без базы.
class OutboxMessageTest {
    // Ключ партиции Kafka всегда равен id агрегата. Вызывающий код не может задать другой,
    // поэтому все события одного заказа попадут в одну партицию.
    @Test
    void usesAggregateIdAsPartitionKey() {
        var message = OutboxMessage.builder()
                .topic("orders.v1")
                .aggregate(new Aggregate("order", "order-42"))
                .payload(new TestEvent("order-42"))
                .header("trace-id", "trace-7")
                .build();

        assertThat(message.partitionKey()).isEqualTo("order-42");
        assertThat(message.headers()).containsEntry("trace-id", "trace-7");
    }

    // Заголовки копируются в момент build(): изменение исходной Map потом ни на что не влияет,
    // а Map внутри сообщения неизменяемая.
    @Test
    void snapshotsHeadersWhenBuilt() {
        var headers = new LinkedHashMap<String, String>();
        headers.put("trace-id", "trace-7");

        var message = OutboxMessage.builder()
                .topic("orders.v1")
                .aggregate(new Aggregate("order", "order-42"))
                .payload(new TestEvent("order-42"))
                .headers(headers)
                .build();

        headers.put("trace-id", "changed");

        assertThat(message.headers()).containsEntry("trace-id", "trace-7");
        assertThatThrownBy(() -> message.headers().put("new", "value"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // Обязательные поля (topic, aggregate, payload) проверяются сразу при сборке.
    @Test
    void rejectsMissingRequiredFields() {
        assertThatThrownBy(() -> OutboxMessage.builder()
                .topic("orders.v1")
                .aggregate(new Aggregate("order", "order-42"))
                .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessage("payload must not be null");
    }

    private record TestEvent(String orderId) {
    }
    // Префикс rm- зарезервирован под служебные заголовки стартера (rm-message-id и другие).
    // Пользователь не может их подменить ни через header(), ни через headers().
    @org.junit.jupiter.api.Test
    void rejectsReservedHeadersThroughBothBuilderMethods() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> OutboxMessage.builder().header("rm-message-id", "x"))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> OutboxMessage.builder().headers(java.util.Map.of("rm-any", "x")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
