package dev.reliablemessaging.demo.order;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.reliablemessaging.outbox.api.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.*;
import java.nio.charset.StandardCharsets;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Service
public class Orders {
    private final JdbcTemplate jdbc;
    private final OutboxPublisher outbox;
    private final ObjectProvider<KafkaTemplate<String,String>> naiveKafka;
    private final ObjectMapper mapper;
    private final String mode;
    public Orders(JdbcTemplate jdbc,OutboxPublisher outbox,
                  @Qualifier("naiveKafkaTemplate") ObjectProvider<KafkaTemplate<String,String>> naiveKafka,
                  ObjectMapper mapper,@Value("${demo.delivery-mode:outbox}") String mode) {
        if(!mode.equals("outbox") && !mode.equals("naive")) throw new IllegalArgumentException("demo.delivery-mode must be outbox or naive");
        this.jdbc=jdbc; this.outbox=outbox; this.naiveKafka=naiveKafka; this.mapper=mapper; this.mode=mode;
    }
    @Transactional
    public UUID place(String customerId,BigDecimal total,boolean fail) {
        var id=UUID.randomUUID(); var event=new OrderPlaced(id,customerId,total,Instant.now());
        jdbc.update("insert into orders(id,customer_id,total) values (?,?,?)",id,customerId,total);
        if(mode.equals("outbox")) outbox.publish(OutboxMessage.builder().topic("orders.v1")
                .aggregate(new Aggregate("order",id.toString())).payload(event).build());
        else {
            final String payload;
            try { payload=mapper.writeValueAsString(event); }
            catch(JsonProcessingException ex) { throw new IllegalArgumentException("Cannot serialize OrderPlaced",ex); }
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override public void afterCommit() {
                    var record=new ProducerRecord<String,String>("orders.v1",id.toString(),payload);
                    record.headers().add(MessageHeaders.MESSAGE_ID,UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
                    naiveKafka.getObject().send(record);
                }
            });
        }
        if(fail) throw new IllegalStateException("Intentional failure after event recording");
        return id;
    }
}
