package com.github.ivanas93.reader.model;

import lombok.Builder;

import java.io.Serializable;

@Builder
public class Label implements Serializable {
    public String name;
    public String value;
}
