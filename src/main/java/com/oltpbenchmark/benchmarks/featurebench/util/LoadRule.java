package com.oltpbenchmark.benchmarks.featurebench.util;

public class LoadRule {
    private TableInfo tableInfo;

    public LoadRule(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }
}

