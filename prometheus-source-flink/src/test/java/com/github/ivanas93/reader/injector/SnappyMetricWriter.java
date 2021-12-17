package com.github.ivanas93.reader.injector;

import com.github.ivanas93.reader.test.SnappyContentUtil;
import lombok.SneakyThrows;
import okhttp3.ConnectionPool;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.github.ivanas93.reader.configuration.PrometheusHeader.CONTENT_ENCODING;
import static com.github.ivanas93.reader.configuration.PrometheusHeader.CONTENT_ENCODING_VALUE;
import static com.github.ivanas93.reader.configuration.PrometheusHeader.PROMETHEUS_REMOTE_WRITE_VALUE;
import static com.github.ivanas93.reader.configuration.PrometheusHeader.PROMETHEUS_REMOTE_WRITE_VERSION;
import static java.time.Instant.now;

public class SnappyMetricWriter {

    @SneakyThrows
    public void write() {

        OkHttpClient httpClient = (new OkHttpClient.Builder()).connectionPool(new ConnectionPool())
                .connectTimeout(10000, TimeUnit.MILLISECONDS)
                .writeTimeout(10000, TimeUnit.MILLISECONDS)
                .readTimeout(100_000, TimeUnit.MILLISECONDS)
                .build();

        byte[] content = SnappyContentUtil.snappyTimeSeriesBuilder()
                .add("my_metric_name", Map.of("name", "my_name"), 1.0f, now().getEpochSecond())
                .toSnappy();
        httpClient.newCall(new Request.Builder()
                        .post(RequestBody.create(content, MediaType.get("application/x-protobuf")))
                        .addHeader(CONTENT_ENCODING, CONTENT_ENCODING_VALUE)
                        .addHeader(PROMETHEUS_REMOTE_WRITE_VERSION, PROMETHEUS_REMOTE_WRITE_VALUE)
                        .url("http://localhost:9000/api/v1/remote_write/")
                        .build())
                .execute();
    }

    public static void main(final String[] args) {
        SnappyMetricWriter snappyMetricWriter = new SnappyMetricWriter();
        snappyMetricWriter.write();

    }
}
