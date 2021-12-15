package com.github.ivanas93.reader;

import com.github.ivanas93.reader.configuration.RemoteReadConfiguration;
import com.github.ivanas93.reader.model.TimeSerie;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.given;

class RemoteReadSourceTest {

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
    @Disabled
    void shouldAcceptRequest() {
        //Given-When
        Executors.newSingleThreadExecutor()
                .execute(() -> {
                    StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
                    environment.getConfig().setGlobalJobParameters(Configuration.fromMap(configuration));

                    environment.addSource(new RemoteReadSource(
                                    new RemoteReadConfiguration("app", configuration)))
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
                    while (!RemoteReadSourceTest.result.get()) {
                        httpClient.newCall(new Request.Builder()
                                        .post(RequestBody.create("".getBytes(StandardCharsets.UTF_8)))
                                        .url("http://localhost:8080/api/v1/write/")
                                        .build())
                                .execute();
                    }
                    return result.get();
                });
    }

    @AllArgsConstructor
    static class FakeSink implements SinkFunction<TimeSerie> {
        @Override
        public void invoke(final TimeSerie value, final Context context) throws Exception {
            SinkFunction.super.invoke(value, context);
            Optional.ofNullable(value).ifPresent(timeSerie -> RemoteReadSourceTest.result.set(true));
        }
    }
}