package dev.reliablemessaging.outbox.support;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testcontainers.kafka.KafkaContainer;
import java.time.Duration;
import java.util.*;
import static org.awaitility.Awaitility.await;
// Хелперы для работы с Kafka в тестах.
public final class KafkaTestSupport {
    // Каждый тест создаёт свой уникальный топик на 3 партиции, поэтому тесты не видят сообщения друг друга.
    public static String topic(KafkaContainer kafka) throws Exception {
        var topic = "test-" + UUID.randomUUID();
        try (var admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1))).all().get();
        }
        return topic;
    }
    // Читает топик с начала новым потребителем (случайный group.id), пока не наберёт count СЫРЫХ записей.
    public static List<ConsumerRecord<String, String>> read(KafkaContainer kafka, String topic, int count) {
        return readRecords(kafka,topic,count,false);
    }
    // Ждёт count УНИКАЛЬНЫХ rm-message-id. Нужен там, где дубли допустимы (at-least-once):
    // если считать сырые записи, первые N штук могут оказаться с повторами, и тест ошибочно решит,
    // что часть событий потерялась.
    public static List<ConsumerRecord<String,String>> readUnique(KafkaContainer kafka,String topic,int count) {
        return readRecords(kafka,topic,count,true);
    }
    private static List<ConsumerRecord<String,String>> readRecords(KafkaContainer kafka,String topic,int count,boolean unique) {
        var properties = new HashMap<String, Object>();
        properties.put("bootstrap.servers", kafka.getBootstrapServers());
        properties.put("group.id", UUID.randomUUID().toString());
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);
        var records = new ArrayList<ConsumerRecord<String, String>>();
        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            consumer.subscribe(List.of(topic));
            await().atMost(Duration.ofSeconds(40)).until(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(records::add);
                return unique ? records.stream().map(r -> new String(r.headers().lastHeader(
                    dev.reliablemessaging.outbox.api.MessageHeaders.MESSAGE_ID).value(),java.nio.charset.StandardCharsets.UTF_8))
                    .distinct().count() >= count : records.size() >= count;
            });
        }
        return records;
    }
}
