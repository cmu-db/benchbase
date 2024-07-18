package com.oltpbenchmark.benchmarks.dataloader;

public class PrimaryKey {
    private String columnName;
    private String dataType;

    // Getters and Setters
    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    @Override
    public String toString() {
        return "PrimaryKey{" +
            "columnName='" + columnName + '\'' +
            ", dataType='" + dataType + '\'' +
            '}';
    }
}
