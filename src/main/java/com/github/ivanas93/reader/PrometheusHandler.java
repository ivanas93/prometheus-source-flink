package com.github.ivanas93.reader;

import com.github.ivanas93.reader.model.Label;
import com.github.ivanas93.reader.model.Sample;
import com.github.ivanas93.reader.model.TimeSerie;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import jakarta.servlet.http.HttpServletResponse;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.xerial.snappy.SnappyInputStream;
import prometheus.Remote.WriteRequest;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.github.ivanas93.reader.configuration.RemoteReadHeader.CONTENT_ENCODING;
import static com.github.ivanas93.reader.configuration.RemoteReadHeader.CONTENT_ENCODING_VALUE;
import static com.github.ivanas93.reader.configuration.RemoteReadHeader.PROMETHEUS_MEDIA_TYPE;
import static com.github.ivanas93.reader.configuration.RemoteReadHeader.PROMETHEUS_REMOTE_WRITE_VALUE;
import static com.github.ivanas93.reader.configuration.RemoteReadHeader.PROMETHEUS_REMOTE_WRITE_VERSION;
import static org.apache.flink.shaded.akka.org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;

@Slf4j
@Getter
public class PrometheusHandler implements HttpHandler {

    private final SourceContext<TimeSerie> context;

    public PrometheusHandler(final SourceContext<TimeSerie> context) {
        this.context = context;
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        if (!validateHeaders(exchange) || !validateHttpMethod(exchange)) {
            exchange.sendResponseHeaders(HttpServletResponse.SC_PRECONDITION_FAILED, 0);
            return;
        }

        if (!validateMediaType(exchange)) {
            exchange.sendResponseHeaders(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE, 0);
            return;
        }

        InputStream body = exchange.getRequestBody();
        try {
            SnappyInputStream snappyInputStream = new SnappyInputStream(body);
            deserializeSnappyRequest(snappyInputStream, context::collect);
            exchange.sendResponseHeaders(HttpServletResponse.SC_OK, 0);
        } catch (Exception exception) {
            exchange.sendResponseHeaders(HttpServletResponse.SC_BAD_REQUEST, 0);
        }
    }

    private boolean validateHeaders(final HttpExchange exchange) {
        Headers headers = exchange.getRequestHeaders();
        List<String> encodings = headers.getOrDefault(CONTENT_ENCODING, List.of());
        List<String> remoteValues = headers.getOrDefault(PROMETHEUS_REMOTE_WRITE_VERSION, List.of());

        return encodings.stream().anyMatch(s -> s.equalsIgnoreCase(CONTENT_ENCODING_VALUE))
                && remoteValues.stream().anyMatch(s -> s.equalsIgnoreCase(PROMETHEUS_REMOTE_WRITE_VALUE));
    }

    private boolean validateHttpMethod(final HttpExchange exchange) {
        String method = exchange.getRequestMethod();
        if (!method.equalsIgnoreCase("post")) {
            log.error("requested endpoint but method is not a POST");
            return false;
        }
        return true;
    }

    private boolean validateMediaType(final HttpExchange exchange) {
        return exchange.getRequestHeaders().getOrDefault(CONTENT_TYPE, List.of())
                .stream()
                .anyMatch(s -> s.equalsIgnoreCase(PROMETHEUS_MEDIA_TYPE));
    }

    private void deserializeSnappyRequest(final SnappyInputStream inputStream,
                                          final Consumer<TimeSerie> timeSeriesConsumer) {

        try {
            WriteRequest writeRequest = WriteRequest.parseFrom(inputStream);

            writeRequest.getTimeseriesList().forEach(timeSeries -> {
                var labels = new ArrayList<Label>();
                var samples = new ArrayList<Sample>();
                timeSeries.getLabelsList()
                        .forEach(label -> labels.add(Label.builder()
                                .value(label.getValue())
                                .name(label.getName())
                                .build()));

                timeSeries.getSamplesList()
                        .forEach(sample -> samples.add(Sample.builder()
                                .timestamp(sample.getTimestamp())
                                .sample(sample.getValue())
                                .build()));
                timeSeriesConsumer.accept(TimeSerie.builder().samples(samples).labels(labels).build());
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
