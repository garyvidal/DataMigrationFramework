package com.nativelogix.rdbms2marklogic.model.project;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Join predicate operators for a {@link JoinCondition}.
 * Values are serialized as camelCase strings to match the TypeScript {@code JoinType} union.
 */
public enum JoinType {

    EQUALS("equals"),
    NOT_EQUALS("notEquals"),
    LESS_THAN("lessThan"),
    LESS_THAN_OR_EQUAL("lessThanOrEqual"),
    GREATER_THAN("greaterThan"),
    GREATER_THAN_OR_EQUAL("greaterThanOrEqual"),
    LIKE("like");

    private final String value;

    JoinType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static JoinType fromValue(String value) {
        if (value == null) return null;
        for (JoinType type : values()) {
            if (type.value.equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown JoinType value: '" + value + "'");
    }
}
