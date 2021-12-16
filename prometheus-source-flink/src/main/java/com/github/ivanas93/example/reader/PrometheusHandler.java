package com.github.ivanas93.example.reader;

import com.github.ivanas93.example.reader.model.Label;
import com.github.ivanas93.example.reader.model.Sample;
import com.github.ivanas93.example.reader.model.TimeSerie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.xerial.snappy.SnappyInputStream;
import prometheus.Remote.WriteRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;

import static com.github.ivanas93.example.reader.configuration.PrometheusHeader.CONTENT_ENCODING;
import static com.github.ivanas93.example.reader.configuration.PrometheusHeader.CONTENT_ENCODING_VALUE;
import static com.github.ivanas93.example.reader.configuration.PrometheusHeader.PROMETHEUS_MEDIA_TYPE;
import static com.github.ivanas93.example.reader.configuration.PrometheusHeader.PROMETHEUS_REMOTE_WRITE_VALUE;
import static com.github.ivanas93.example.reader.configuration.PrometheusHeader.PROMETHEUS_REMOTE_WRITE_VERSION;

@Slf4j
@Setter(AccessLevel.PACKAGE)
@RequiredArgsConstructor
public class PrometheusHandler extends AbstractHandler {

    private SourceFunction.SourceContext<TimeSerie> sourceFlinkContext;

    @Override
    public void handle(final String target, final Request request, final HttpServletRequest httpServletRequest,
                       final HttpServletResponse httpServletResponse) throws IOException {

        var outputStream = httpServletResponse.getOutputStream();

        if (!validatePrometheusHeader(httpServletRequest)) {
            httpServletResponse.setStatus(HttpServletResponse.SC_PRECONDITION_FAILED);

        } else if (!httpServletRequest.getContentType().equalsIgnoreCase(PROMETHEUS_MEDIA_TYPE)) {
            httpServletResponse.setStatus(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE);

        } else {
            try {
                SnappyInputStream inputStream = new SnappyInputStream(request.getInputStream());
                deserializeSnappyRequest(inputStream, httpServletResponse);
            } catch (Exception exception) {
                log.error(exception.toString());
                httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            }

        }
        outputStream.flush();
    }

    private boolean validatePrometheusHeader(final HttpServletRequest httpServletRequest) {
        var encodingValue = Optional.ofNullable(httpServletRequest.getHeader(CONTENT_ENCODING));
        var remoteValue = Optional.ofNullable(httpServletRequest.getHeader(PROMETHEUS_REMOTE_WRITE_VERSION));

        if (encodingValue.isPresent() && remoteValue.isPresent()) {
            return encodingValue.map(s -> s.equalsIgnoreCase(CONTENT_ENCODING_VALUE)).orElse(false)
                    && remoteValue.map(s -> s.equalsIgnoreCase(PROMETHEUS_REMOTE_WRITE_VALUE)).orElse(false);
        } else {
            return false;
        }
    }

    private void deserializeSnappyRequest(final SnappyInputStream inputStream,
                                          final HttpServletResponse httpServletResponse) throws IOException {
        WriteRequest writeRequest = WriteRequest.parseFrom(inputStream);

        writeRequest.getTimeseriesList()
                .forEach(timeSeries -> {
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

                    TimeSerie timeSerie = TimeSerie.builder()
                            .samples(samples)
                            .labels(labels)
                            .build();

                    sourceFlinkContext.collect(timeSerie);

                    httpServletResponse.setStatus(HttpServletResponse.SC_OK);
                });
    }
}
