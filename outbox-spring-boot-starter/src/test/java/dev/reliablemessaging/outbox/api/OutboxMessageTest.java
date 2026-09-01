package dev.reliablemessaging.outbox.api;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxMessageTest {
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
}
