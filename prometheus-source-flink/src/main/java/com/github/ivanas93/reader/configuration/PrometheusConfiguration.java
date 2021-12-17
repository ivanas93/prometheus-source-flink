package com.github.ivanas93.reader.configuration;

import lombok.Getter;

import java.io.Serializable;
import java.util.Map;

@Getter
public class PrometheusConfiguration implements Serializable {

    private final String path;
    private final Integer port;

    public PrometheusConfiguration(final String prefix, final Map<String, String> configuration) {
        path = configuration.getOrDefault(prefix + ".path", "/api/v1/write");
        port = Integer.valueOf(configuration.getOrDefault(prefix + ".port", "9090"));
    }

}
