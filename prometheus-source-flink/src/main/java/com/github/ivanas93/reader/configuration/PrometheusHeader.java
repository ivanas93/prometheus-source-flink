package com.github.ivanas93.reader.configuration;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PrometheusHeader {
    public static final String CONTENT_ENCODING = "Content-Encoding";
    public static final String CONTENT_ENCODING_VALUE = "snappy";

    public static final String PROMETHEUS_REMOTE_WRITE_VERSION = "X-Prometheus-Remote-Write-Version";
    public static final String PROMETHEUS_REMOTE_WRITE_VALUE = "0.1.0";

    public static final String PROMETHEUS_MEDIA_TYPE = "application/x-protobuf";
}
