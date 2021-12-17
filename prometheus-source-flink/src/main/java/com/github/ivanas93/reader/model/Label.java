package com.github.ivanas93.reader.model;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Builder
@ToString
public class Label implements Serializable {
    private String name;
    private String value;
}
