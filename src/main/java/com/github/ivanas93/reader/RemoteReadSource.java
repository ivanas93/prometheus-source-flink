package com.github.ivanas93.reader;

import com.github.ivanas93.reader.configuration.RemoteReadConfiguration;
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
public class RemoteReadSource extends RichSourceFunction<TimeSeries> {


    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private transient HttpServer server;
    private final RemoteReadConfiguration remoteReadConfiguration;


    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);

        server = HttpServer.create(new InetSocketAddress(remoteReadConfiguration.getPort()), 0);
        server.setExecutor(Executors.newSingleThreadExecutor());
        server.start();
    }

    @SneakyThrows
    public RemoteReadSource(final String prefix, final Map<String, String> params) {
        this(new RemoteReadConfiguration(prefix, params));
    }

    @SneakyThrows
    public RemoteReadSource(final RemoteReadConfiguration remoteReadConfiguration) {
        this.remoteReadConfiguration = remoteReadConfiguration;
    }

    @Override
    @SneakyThrows
    public void run(final SourceContext<TimeSeries> ctx) {
        server.createContext(remoteReadConfiguration.getPath(), new PrometheusHandler(ctx));
        while (isRunning.get()) {
            TimeUnit.SECONDS.sleep(5_000);
        }
    }

    @SneakyThrows
    @Override
    public void cancel() {
        isRunning.set(false);
        if (server != null) {
            server.stop(3);
        }
    }

}
