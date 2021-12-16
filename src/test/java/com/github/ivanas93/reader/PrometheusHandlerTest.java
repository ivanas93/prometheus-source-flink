package com.github.ivanas93.reader;

import com.github.ivanas93.reader.model.TimeSerie;
import com.github.ivanas93.reader.test.SnappyContentUtil;
import com.github.ivanas93.reader.test.TestSourceContext;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import jakarta.servlet.http.HttpServletResponse;
import lombok.SneakyThrows;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.ivanas93.reader.configuration.RemoteReadHeader.CONTENT_ENCODING;
import static com.github.ivanas93.reader.configuration.RemoteReadHeader.CONTENT_ENCODING_VALUE;
import static com.github.ivanas93.reader.configuration.RemoteReadHeader.PROMETHEUS_MEDIA_TYPE;
import static com.github.ivanas93.reader.configuration.RemoteReadHeader.PROMETHEUS_REMOTE_WRITE_VALUE;
import static com.github.ivanas93.reader.configuration.RemoteReadHeader.PROMETHEUS_REMOTE_WRITE_VERSION;
import static org.apache.flink.shaded.akka.org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;

class PrometheusHandlerTest {

    private HttpExchange exchange;

    private Headers headers;

    private AtomicInteger code;

    PrometheusHandler prometheusHandler;

    TestSourceContext<TimeSerie> context;

    @BeforeEach
    @SneakyThrows
    void setUp() {

        code = new AtomicInteger();
        exchange = Mockito.mock(HttpExchange.class);
        Mockito.doAnswer(a -> {
            Integer codeReceived = a.getArgument(0);
            code.set(codeReceived);
            return null;
        }).when(exchange).sendResponseHeaders(anyInt(), anyLong());

        headers = new Headers();
        Mockito.when(exchange.getRequestHeaders()).thenReturn(headers);
        context = new TestSourceContext<>();
        prometheusHandler = new PrometheusHandler(context);
    }

    @Test
    @SneakyThrows
    void shouldHandleARequest() {
        //Given
        byte[] content = SnappyContentUtil.snappyTimeSeriesBuilder()
                .add("my_metric_name", Map.of("name", "my_name"), 1.0f, Instant.now().getEpochSecond())
                .add("my_metric_name", Map.of("name", "my_name_2"), 1.0f, Instant.now().getEpochSecond())
                .toSnappy();

        headers.add(CONTENT_ENCODING, CONTENT_ENCODING_VALUE);
        headers.add(PROMETHEUS_REMOTE_WRITE_VERSION, PROMETHEUS_REMOTE_WRITE_VALUE);
        headers.add(CONTENT_TYPE, PROMETHEUS_MEDIA_TYPE);

        Mockito.when(exchange.getRequestMethod()).thenReturn("POST");
        Mockito.when(exchange.getRequestBody()).thenReturn(new ByteArrayInputStream(content));

        //When
        prometheusHandler.handle(exchange);

        //Then
        Assertions.assertThat(context.iterator()).hasSize(2);
        Assertions.assertThat(code.get()).isEqualTo(HttpServletResponse.SC_OK);
    }

    @Test
    @SneakyThrows
    void shouldFailOfHeaders() {

        //Given
        headers.add(CONTENT_ENCODING, "not-encoding-in-right-format");
        headers.add(PROMETHEUS_REMOTE_WRITE_VERSION, PROMETHEUS_REMOTE_WRITE_VALUE);

        //When
        prometheusHandler.handle(exchange);

        //Then
        Assertions.assertThat(context.iterator()).hasSize(0);
        Assertions.assertThat(code.get()).isEqualTo(HttpServletResponse.SC_PRECONDITION_FAILED);
    }

    @Test
    @SneakyThrows
    void shouldFailOfMethod() {
        //Given
        headers.add(CONTENT_ENCODING, CONTENT_ENCODING_VALUE);
        headers.add(PROMETHEUS_REMOTE_WRITE_VERSION, PROMETHEUS_REMOTE_WRITE_VALUE);

        Mockito.when(exchange.getRequestMethod()).thenReturn("GET");

        //When
        prometheusHandler.handle(exchange);

        //Then
        Assertions.assertThat(context.iterator()).hasSize(0);
        Assertions.assertThat(code.get()).isEqualTo(HttpServletResponse.SC_PRECONDITION_FAILED);
    }

    @Test
    @SneakyThrows
    void shouldFailOfMediaType() {

        //Given
        headers.add(CONTENT_ENCODING, CONTENT_ENCODING_VALUE);
        headers.add(PROMETHEUS_REMOTE_WRITE_VERSION, PROMETHEUS_REMOTE_WRITE_VALUE);
        headers.add(CONTENT_TYPE, PROMETHEUS_MEDIA_TYPE);

        Mockito.when(exchange.getRequestMethod()).thenReturn("POST");

        //When
        prometheusHandler.handle(exchange);

        //Then
        Assertions.assertThat(context.iterator()).hasSize(0);
        Assertions.assertThat(code.get()).isEqualTo(HttpServletResponse.SC_BAD_REQUEST);
    }

}

