package com.github.ivanas93.reader.model;

import lombok.Builder;

import java.io.Serializable;
import java.util.List;

@Builder
public class TimeSerie implements Serializable {
    List<Label> labels;
    List<Sample> samples;
}
