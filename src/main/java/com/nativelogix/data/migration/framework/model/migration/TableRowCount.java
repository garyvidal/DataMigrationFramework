package com.nativelogix.data.migration.framework.model.migration;

public class TableRowCount {

    private String schema;
    private String tableName;
    private String role; // "root" or "child"
    private long rowCount;
    /** WHERE clause applied when counting, null if no filter. */
    private String whereClause;

    public TableRowCount() {}

    public TableRowCount(String schema, String tableName, String role, long rowCount, String whereClause) {
        this.schema = schema;
        this.tableName = tableName;
        this.role = role;
        this.rowCount = rowCount;
        this.whereClause = whereClause;
    }

    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }

    public long getRowCount() { return rowCount; }
    public void setRowCount(long rowCount) { this.rowCount = rowCount; }

    public String getWhereClause() { return whereClause; }
    public void setWhereClause(String whereClause) { this.whereClause = whereClause; }
}
