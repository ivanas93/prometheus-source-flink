package com.github.ivanas93.example;

import com.github.ivanas93.example.reader.PrometheusSource;
import com.github.ivanas93.example.reader.configuration.PrometheusConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new PrometheusSource(new PrometheusConfiguration("app.source.remote-write-server",
                        env.getConfig().getGlobalJobParameters().toMap())))
                .name("source" + Thread.currentThread())
                .print();

        env.execute("run_flink_cluster");
    }
}
