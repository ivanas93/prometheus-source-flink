package com.github.ivanas93.reader.model;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

@Getter
@Builder
@ToString
public class Sample implements Serializable {

    private String metricName;
    private Map<String, String> labels;
    private Double sample;
    private long timestamp;
}
