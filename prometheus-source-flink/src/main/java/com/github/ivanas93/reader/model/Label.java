package com.github.ivanas93.reader.model;

import lombok.Builder;
import lombok.ToString;

import java.io.Serializable;

@Builder
@ToString
public class Label implements Serializable {
    private String name;
    private String value;
}
