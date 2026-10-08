package dev.reliablemessaging.demo.order;

import org.springframework.boot.SpringApplication;
import org.springframework.core.env.MapPropertySource;
import java.util.Map;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OrderServiceApplication {
    public static void main(String[] args) {
        var app=new SpringApplication(OrderServiceApplication.class);
        app.addInitializers(context -> {
            if("naive".equals(context.getEnvironment().getProperty("demo.delivery-mode")))
                context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("naiveDeliveryMode",
                        Map.of("outbox.relay.enabled","false")));
        });
        app.run(args);
    }
}
