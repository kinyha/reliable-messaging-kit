package dev.reliablemessaging.demo.order;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import java.math.BigDecimal;
import java.util.UUID;
@RestController
public class OrderController {
    private final Orders orders;
    public OrderController(Orders orders) { this.orders=orders; }
    public record Request(@NotBlank String customerId,@NotNull @DecimalMin("0.01") @Digits(integer=10,fraction=2) BigDecimal total) { }
    public record Created(UUID orderId) { }
    @PostMapping("/orders")
    public ResponseEntity<Created> place(@Valid @RequestBody Request request,@RequestParam(defaultValue="false") boolean fail) {
        return ResponseEntity.status(HttpStatus.CREATED).body(new Created(orders.place(request.customerId(),request.total(),fail)));
    }
}
