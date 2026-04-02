package com.nativelogix.rdbms2marklogic.model.diagrams;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Connection line types matching the @xyflow/react ConnectionLineType enum values.
 */
public enum ConnectionLineType {
    BEZIER("default"),
    SMOOTH_STEP("smoothstep"),
    STEP("step"),
    STRAIGHT("straight"),
    SIMPLE_BEZIER("simplebezier");

    private final String value;

    ConnectionLineType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static ConnectionLineType fromValue(String value) {
        for (ConnectionLineType type : values()) {
            if (type.value.equals(value)) return type;
        }
        throw new IllegalArgumentException("Unknown ConnectionLineType: " + value);
    }
}
