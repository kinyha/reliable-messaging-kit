package dev.reliablemessaging.demo.payment;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
public record OrderPlaced(UUID orderId,String customerId,BigDecimal total,Instant placedAt) { }
