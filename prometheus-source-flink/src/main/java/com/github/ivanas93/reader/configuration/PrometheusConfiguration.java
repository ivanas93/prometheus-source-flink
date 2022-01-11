package com.github.ivanas93.reader.configuration;

import lombok.Getter;

import java.io.Serializable;
import java.util.Map;

@Getter
public class PrometheusConfiguration implements Serializable {

    private final String path;
    private final Integer port;
    private final Long waitingInitialization;
    private final Integer stopDelay;

    public PrometheusConfiguration(final String prefix, final Map<String, String> configuration) {
        path = configuration.getOrDefault(prefix + ".path", "/api/v1/write");
        port = Integer.valueOf(configuration.getOrDefault(prefix + ".port", "9090"));
        waitingInitialization = Long.valueOf(configuration.getOrDefault(prefix + ".waiting-initialization", "5000"));
        stopDelay = Integer.valueOf(configuration.getOrDefault(prefix + ".stop-delay", "3"));
    }

}
