package com.nativelogix.data.migration.framework.model.migration;

import java.util.List;

public class MigrationPreviewResult {

    private List<TableRowCount> tables;
    private long totalRows;

    public MigrationPreviewResult() {}

    public MigrationPreviewResult(List<TableRowCount> tables, long totalRows) {
        this.tables = tables;
        this.totalRows = totalRows;
    }

    public List<TableRowCount> getTables() { return tables; }
    public void setTables(List<TableRowCount> tables) { this.tables = tables; }

    public long getTotalRows() { return totalRows; }
    public void setTotalRows(long totalRows) { this.totalRows = totalRows; }
}
