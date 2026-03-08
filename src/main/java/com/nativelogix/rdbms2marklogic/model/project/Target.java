package com.nativelogix.rdbms2marklogic.model.project;

import lombok.Data;

@Data
public class Target {
    String name;
    boolean included;
    boolean nullIncluded;
    TargetType dataType;
}
