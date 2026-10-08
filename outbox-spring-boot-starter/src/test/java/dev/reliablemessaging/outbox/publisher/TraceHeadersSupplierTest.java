package dev.reliablemessaging.outbox.publisher;
import dev.reliablemessaging.outbox.config.*;
import io.micrometer.tracing.*;
import io.micrometer.tracing.propagation.Propagator;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.transaction.PlatformTransactionManager;
import javax.sql.DataSource;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
class TraceHeadersSupplierTest {
    @Test void propagatesCurrentContextWhenOptionalTracingBeansExist() {
        var tracer=mock(Tracer.class); var span=mock(Span.class); var context=mock(TraceContext.class);
        when(tracer.currentSpan()).thenReturn(span); when(span.context()).thenReturn(context);
        var propagator=mock(Propagator.class);
        doAnswer(invocation -> {
            Map<String,String> headers=invocation.getArgument(1);
            headers.put("traceparent","00-01234567890123456789012345678901-0123456789012345-01");
            return null;
        }).when(propagator).inject(eq(context),any(),any());
        new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(ReliableMessagingAutoConfiguration.class,
                OutboxPublisherAutoConfiguration.class))
            .withPropertyValues("outbox.inbox.retention=30d","outbox.migrations.enabled=false")
            .withBean(DataSource.class,() -> mock(DataSource.class))
            .withBean(PlatformTransactionManager.class,() -> mock(PlatformTransactionManager.class))
            .withBean(Tracer.class,() -> tracer).withBean(Propagator.class,() -> propagator)
            .run(c -> assertThat(c.getBean(TraceHeadersSupplier.class).get()).containsKey("traceparent"));
    }
}
