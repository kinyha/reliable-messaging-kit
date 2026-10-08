package dev.reliablemessaging.demo.order;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
public record OrderPlaced(UUID orderId,String customerId,BigDecimal total,Instant placedAt) { }
