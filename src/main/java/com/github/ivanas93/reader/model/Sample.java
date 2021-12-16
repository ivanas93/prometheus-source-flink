package com.github.ivanas93.reader.model;

import lombok.Builder;
import lombok.ToString;

import java.io.Serializable;

@Builder
@ToString
public class Sample implements Serializable {
    private Double sample;
    private long timestamp;
}
