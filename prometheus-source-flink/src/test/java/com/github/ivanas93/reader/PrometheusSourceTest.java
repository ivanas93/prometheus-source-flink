package com.github.ivanas93.reader;

import com.github.ivanas93.reader.configuration.PrometheusConfiguration;
import com.github.ivanas93.reader.model.TimeSerie;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import okhttp3.ConnectionPool;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;
import prometheus.Remote;
import prometheus.Types;

import java.net.ConnectException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.github.ivanas93.reader.configuration.PrometheusHeader.CONTENT_ENCODING;
import static com.github.ivanas93.reader.configuration.PrometheusHeader.CONTENT_ENCODING_VALUE;
import static com.github.ivanas93.reader.configuration.PrometheusHeader.PROMETHEUS_REMOTE_WRITE_VALUE;
import static com.github.ivanas93.reader.configuration.PrometheusHeader.PROMETHEUS_REMOTE_WRITE_VERSION;
import static org.awaitility.Awaitility.given;

class PrometheusSourceTest {

    Map<String, String> configuration;
    static volatile AtomicBoolean result = new AtomicBoolean(false);

    OkHttpClient httpClient;

    @BeforeEach
    void startUp() {
        configuration = Map.of("app.port", "8080", "app.path", "/api/v1/write");

        httpClient = (new OkHttpClient.Builder()).connectionPool(new ConnectionPool())
                .connectTimeout(10000, TimeUnit.MILLISECONDS)
                .writeTimeout(10000, TimeUnit.MILLISECONDS)
                .readTimeout(10000, TimeUnit.MILLISECONDS)
                .build();
    }

    @Test
    @SneakyThrows
    void shouldAcceptRequest() {
        //Given-When
        Executors.newSingleThreadExecutor()
                .execute(() -> {
                    StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
                    environment.getConfig().setGlobalJobParameters(Configuration.fromMap(configuration));

                    environment.addSource(new PrometheusSource(
                                    new PrometheusConfiguration("app", configuration)))
                            .addSink(new FakeSink());

                    try {
                        environment.execute("run_cluster_test");
                    } catch (final Exception e) {
                        Assertions.fail();
                    }

                });

        given().ignoreException(ConnectException.class)
                .await()
                .until(() -> {
                    while (!PrometheusSourceTest.result.get()) {
                        httpClient.newCall(new Request.Builder()
                                        .post(RequestBody.create(getTimeSeriesByteArray(),
                                                MediaType.get("application/x-protobuf")))
                                        .addHeader(CONTENT_ENCODING, CONTENT_ENCODING_VALUE)
                                        .addHeader(PROMETHEUS_REMOTE_WRITE_VERSION, PROMETHEUS_REMOTE_WRITE_VALUE)
                                        .url("http://localhost:8080/api/v1/write/")
                                        .build())
                                .execute();
                    }
                    return result.get();
                });
    }

    @SneakyThrows
    public byte[] getTimeSeriesByteArray() {
        Remote.WriteRequest.Builder writeRequestBuilder = Remote.WriteRequest.newBuilder();

        Types.TimeSeries.Builder timeSeriesBuilder = Types.TimeSeries.newBuilder();
        Types.Label.Builder labelBuilder = Types.Label.newBuilder();
        Types.Sample.Builder sampleBuilder = Types.Sample.newBuilder();


        labelBuilder.setName("name1")
                .setValue("value1");

        labelBuilder.setName("name2")
                .setValue("value2");

        Types.Label l = labelBuilder.build();

        sampleBuilder.setValue(1)
                .setTimestamp(1111111111L);

        Types.Sample s = sampleBuilder.build();

        timeSeriesBuilder.addAllLabels(List.of(l));
        timeSeriesBuilder.addAllSamples(List.of(s));

        Types.TimeSeries t = timeSeriesBuilder.build();
        writeRequestBuilder.addAllTimeseries(List.of(t));

        Remote.WriteRequest message = writeRequestBuilder.build();

        return Snappy.compress(message.toByteArray());
    }


    @AllArgsConstructor
    static class FakeSink implements SinkFunction<TimeSerie> {
        @Override
        public void invoke(final TimeSerie value, final Context context) throws Exception {
            SinkFunction.super.invoke(value, context);
            Optional.ofNullable(value).ifPresent(timeSerie -> PrometheusSourceTest.result.set(true));
        }
    }
}