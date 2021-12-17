package com.github.ivanas93.reader;

import com.github.ivanas93.reader.model.Label;
import com.github.ivanas93.reader.model.Sample;
import com.github.ivanas93.reader.model.TimeSeries;
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
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.github.ivanas93.reader.configuration.RemoteReadHeader.CONTENT_ENCODING;
import static com.github.ivanas93.reader.configuration.RemoteReadHeader.CONTENT_ENCODING_VALUE;
import static com.github.ivanas93.reader.configuration.RemoteReadHeader.PROMETHEUS_MEDIA_TYPE;
import static com.github.ivanas93.reader.configuration.RemoteReadHeader.PROMETHEUS_REMOTE_WRITE_VALUE;
import static com.github.ivanas93.reader.configuration.RemoteReadHeader.PROMETHEUS_REMOTE_WRITE_VERSION;
import static org.apache.flink.shaded.akka.org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;

@Slf4j
@Getter
public class PrometheusHandler implements HttpHandler {

    private final SourceContext<TimeSeries> context;

    public PrometheusHandler(final SourceContext<TimeSeries> context) {
        this.context = context;
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        if (!validateHeaders(exchange) || !validateHttpMethod(exchange)) {
            exchange.sendResponseHeaders(HttpServletResponse.SC_PRECONDITION_FAILED, 0);
        } else if (!validateMediaType(exchange)) {
            exchange.sendResponseHeaders(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE, 0);
        } else {
            InputStream body = exchange.getRequestBody();
            try {
                SnappyInputStream snappyInputStream = new SnappyInputStream(body);
                deserializeSnappyRequest(snappyInputStream, context::collect);
                exchange.sendResponseHeaders(HttpServletResponse.SC_OK, 0);
            } catch (Exception exception) {
                exchange.sendResponseHeaders(HttpServletResponse.SC_BAD_REQUEST, 0);
            }
        }
        exchange.getResponseBody().flush();
        exchange.getResponseBody().close();
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
                                          final Consumer<TimeSeries> timeSeriesConsumer) {

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

                Map<String, String> labelsMap = labels.stream()
                        .filter(l -> !l.getName().equals("__name__"))
                        .collect(Collectors.toMap(Label::getName, Label::getValue));

                String metricName = labels.stream()
                        .filter(l -> l.getName().equals("__name__"))
                        .findFirst().map(Label::getName).orElseThrow();

                timeSeries.getSamplesList()
                        .forEach(sample -> samples.add(Sample.builder()
                                .labels(labelsMap)
                                .metricName(metricName)
                                .timestamp(sample.getTimestamp())
                                .sample(sample.getValue())
                                .build()));

                timeSeriesConsumer.accept(TimeSeries.builder().samples(samples).labels(labels).build());
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
