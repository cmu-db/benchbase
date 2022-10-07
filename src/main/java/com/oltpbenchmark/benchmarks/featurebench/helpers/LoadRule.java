package com.oltpbenchmark.benchmarks.featurebench.helpers;

public class LoadRule {
    private final TableInfo tableInfo;

    public LoadRule(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }
}
