package com.github.ivanas93.example.reader;

import com.github.ivanas93.example.reader.configuration.PrometheusConfiguration;
import com.github.ivanas93.example.reader.model.TimeSerie;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;

@Slf4j
public class PrometheusSource extends RichSourceFunction<TimeSerie> {

    private transient Server server;
    private transient PrometheusHandler prometheusHandler;
    private final PrometheusConfiguration remoteReadConfiguration;

    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);

        this.prometheusHandler = new PrometheusHandler();
        this.server = new Server(remoteReadConfiguration.getPort());

        var context = new ContextHandler();
        context.setContextPath(remoteReadConfiguration.getPath());
        context.setAllowNullPathInfo(true);
        context.setHandler(prometheusHandler);

        this.server.setHandler(context);

        this.startUp();
    }

    @SneakyThrows
    public PrometheusSource(final PrometheusConfiguration remoteReadConfiguration) {
        this.remoteReadConfiguration = remoteReadConfiguration;
    }

    @Override
    public void run(final SourceContext<TimeSerie> ctx) {
        prometheusHandler.setSourceFlinkContext(ctx);
        while (server.isRunning() && prometheusHandler.isRunning()) {
            log.info("Server is alive");
        }
    }

    @SneakyThrows
    @Override
    public void cancel() {
        server.stop();
    }

    @SneakyThrows
    private void startUp() {
        server.join();
        server.start();
    }
}
