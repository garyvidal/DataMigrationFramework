package com.nativelogix.data.migration.framework.model.project;

import lombok.Data;

@Data
public class Target {
    String name;
    boolean included;
    boolean nullIncluded;
    TargetType dataType;
}
