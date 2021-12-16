package com.github.ivanas93.example.reader.model;

import lombok.Builder;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@Builder
@ToString
public class TimeSerie implements Serializable {
    private List<Label> labels;
    private List<Sample> samples;
}
