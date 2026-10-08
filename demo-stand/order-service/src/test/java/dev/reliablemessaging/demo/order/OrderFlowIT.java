package dev.reliablemessaging.demo.order;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import java.math.BigDecimal;
import static org.assertj.core.api.Assertions.*;
@SpringBootTest(properties={"outbox.relay.enabled=false","spring.kafka.admin.auto-create=false","management.tracing.enabled=false"})
@Import(OrderFlowIT.Database.class)
class OrderFlowIT {
    @Autowired Orders orders;
    @Autowired JdbcTemplate jdbc;
    @Test void committedOrderAndEventShareTheSameTransaction() {
        var id=orders.place("c-1",new BigDecimal("42.50"),false);
        assertThat(jdbc.queryForObject("select count(*) from orders where id=?",Integer.class,id)).isEqualTo(1);
        assertThat(jdbc.queryForMap("select aggregate_id,status,payload->>'orderId' as event_id from outbox_message where aggregate_id=?",id.toString()))
            .containsEntry("aggregate_id",id.toString()).containsEntry("event_id",id.toString()).containsEntry("status","NEW");
        int before=jdbc.queryForObject("select count(*) from orders",Integer.class);
        assertThatThrownBy(() -> orders.place("c-2",BigDecimal.TEN,true)).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("select count(*) from orders",Integer.class)).isEqualTo(before);
        assertThat(jdbc.queryForObject("select count(*) from outbox_message",Integer.class)).isEqualTo(before);
    }
    @TestConfiguration(proxyBeanMethods=false)
    static class Database {
        @Bean @ServiceConnection PostgreSQLContainer<?> postgres() { return new PostgreSQLContainer<>("postgres:16.15-alpine"); }
    }
}
