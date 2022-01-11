package com.github.ivanas93.reader;

import com.github.ivanas93.reader.configuration.PrometheusConfiguration;
import com.github.ivanas93.reader.model.TimeSeries;
import com.sun.net.httpserver.HttpServer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class PrometheusSource extends RichSourceFunction<TimeSeries> {


    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private transient HttpServer server;
    private final PrometheusConfiguration prometheusConfiguration;


    @Override
    public void open(final Configuration parameters) throws RuntimeException {
        try {
            super.open(parameters);
            server = HttpServer.create(new InetSocketAddress(prometheusConfiguration.getPort()), 0);
            server.setExecutor(Executors.newSingleThreadExecutor());
            server.start();
            isRunning.set(true);
        } catch (final Exception exception) {
            isRunning.set(false);
            throw new RuntimeException("Error during open operator PrometheusSource: " + exception.getMessage());
        }
    }

    public PrometheusSource(final String prefix, final Map<String, String> params) {
        this(new PrometheusConfiguration(prefix, params));
    }

    public PrometheusSource(final PrometheusConfiguration prometheusConfiguration) {
        this.prometheusConfiguration = prometheusConfiguration;
    }

    @Override
    @SneakyThrows
    public void run(final SourceContext<TimeSeries> ctx) {
        server.createContext(prometheusConfiguration.getPath(), new PrometheusHandler(ctx));
        while (isRunning.get()) {
            TimeUnit.SECONDS.sleep(prometheusConfiguration.getWaitingInitialization());
        }
    }

    @SneakyThrows
    @Override
    public void cancel() {
        isRunning.set(false);
        if (server != null) {
            server.stop(prometheusConfiguration.getStopDelay());
        }
    }

}
