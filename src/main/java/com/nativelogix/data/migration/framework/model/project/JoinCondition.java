package com.nativelogix.data.migration.framework.model.project;

import lombok.Data;

/**
 * A single column-level predicate within a {@link SyntheticJoin}.
 */
@Data
public class JoinCondition {
    String sourceColumn;
    JoinType joinType;
    String targetColumn;
}
