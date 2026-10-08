package dev.reliablemessaging.outbox.support;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.List;
import java.util.Map;
public record OutboxTestRows(JdbcTemplate jdbc) {
    public List<Map<String, Object>> withStatus(String status) {
        return jdbc.queryForList("select * from outbox_message where status = ? order by id", status);
    }
}
