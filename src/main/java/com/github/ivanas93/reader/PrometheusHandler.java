package com.github.ivanas93.reader;

import com.github.ivanas93.reader.model.Label;
import com.github.ivanas93.reader.model.Sample;
import com.github.ivanas93.reader.model.TimeSerie;
import io.vavr.control.Try;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.xerial.snappy.SnappyInputStream;
import prometheus.Remote.WriteRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.github.ivanas93.reader.configuration.RemoteReadHeader.CONTENT_ENCODING;
import static com.github.ivanas93.reader.configuration.RemoteReadHeader.CONTENT_ENCODING_VALUE;
import static com.github.ivanas93.reader.configuration.RemoteReadHeader.PROMETHEUS_MEDIA_TYPE;
import static com.github.ivanas93.reader.configuration.RemoteReadHeader.PROMETHEUS_REMOTE_WRITE_VALUE;
import static com.github.ivanas93.reader.configuration.RemoteReadHeader.PROMETHEUS_REMOTE_WRITE_VERSION;

@Slf4j
@Getter
@RequiredArgsConstructor
public class PrometheusHandler extends AbstractHandler {

    protected static final BlockingQueue<TimeSerie> TIME_SERIES = new ArrayBlockingQueue<>(
            Runtime.getRuntime().availableProcessors());

    @Override
    public void handle(final String target, final Request request, final HttpServletRequest httpServletRequest,
                       final HttpServletResponse httpServletResponse) throws IOException {

        var outputStream = httpServletResponse.getOutputStream();

        if (!validatePrometheusHeader(httpServletRequest)) {
            httpServletResponse.setStatus(HttpServletResponse.SC_PRECONDITION_FAILED);

        } else if (!httpServletRequest.getContentType().equalsIgnoreCase(PROMETHEUS_MEDIA_TYPE)) {
            httpServletResponse.setStatus(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE);

        } else {

            Try.of(() -> new SnappyInputStream(request.getInputStream()))
                    .onFailure(throwable -> httpServletResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST))
                    .onSuccess(inputStream -> deserializeSnappyRequest(inputStream, httpServletResponse));
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
                                          final HttpServletResponse httpServletResponse) {

        Try.of(() -> WriteRequest.parseFrom(inputStream))
                .onSuccess(writeRequest -> writeRequest.getTimeseriesList()
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

                            TIME_SERIES.add(TimeSerie.builder()
                                    .samples(samples)
                                    .labels(labels)
                                    .build());

                            httpServletResponse.setStatus(HttpServletResponse.SC_OK);
                        }));

    }

}
