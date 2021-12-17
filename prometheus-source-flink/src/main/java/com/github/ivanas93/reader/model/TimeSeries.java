package com.github.ivanas93.reader.model;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@Getter
@Builder
@ToString
public class TimeSeries implements Serializable {
    private List<Label> labels;
    private List<Sample> samples;
}
