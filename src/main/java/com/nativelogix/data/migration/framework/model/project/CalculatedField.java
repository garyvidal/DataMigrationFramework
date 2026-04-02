package com.nativelogix.data.migration.framework.model.project;


import lombok.Data;

@Data
public class CalculatedField {
    String name;
    TargetType targetType;
    String expression;
    boolean onCondition;
    String condition;
}
