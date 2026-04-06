package com.nativelogix.data.migration.framework.model.marklogic;

import lombok.Data;

import java.util.List;

@Data
public class MarkLogicPermission {
    /** MarkLogic role name, e.g. "data-reader", "admin". */
    private String roleName;
    /** Capabilities granted to the role: "read", "update", "insert", "execute", "node-update". */
    private List<String> capabilities;
}
