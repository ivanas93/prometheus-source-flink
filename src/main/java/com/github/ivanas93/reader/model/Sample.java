package com.github.ivanas93.reader.model;

import lombok.Builder;

import java.io.Serializable;

@Builder
public class Sample implements Serializable {
    public Double sample;
    public long timestamp;
}
