package com.oltpbenchmark.benchmarks.dataloader;

import java.util.List;

public class ForeignKey {
    private String schemaName;
    private String tableName;
    private String columnName;
    private String columnDataType;
    private String foreignTableSchema;
    private String foreignTableName;
    private String foreignColumnName;
    private List<Object> distinctValues;  // Add this line

    // Getters and Setters
    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnDataType() {
        return columnDataType;
    }

    public void setColumnDataType(String columnDataType) {
        this.columnDataType = columnDataType;
    }

    public String getForeignTableSchema() {
        return foreignTableSchema;
    }

    public void setForeignTableSchema(String foreignTableSchema) {
        this.foreignTableSchema = foreignTableSchema;
    }

    public String getForeignTableName() {
        return foreignTableName;
    }

    public void setForeignTableName(String foreignTableName) {
        this.foreignTableName = foreignTableName;
    }

    public String getForeignColumnName() {
        return foreignColumnName;
    }

    public void setForeignColumnName(String foreignColumnName) {
        this.foreignColumnName = foreignColumnName;
    }

    public List<Object> getDistinctValues() {
        return distinctValues;
    }

    public void setDistinctValues(List<Object> distinctValues) {
        this.distinctValues = distinctValues;
    }

    @Override
    public String toString() {
        return "ForeignKey{" +
            "schemaName='" + schemaName + '\'' +
            ", tableName='" + tableName + '\'' +
            ", columnName='" + columnName + '\'' +
            ", columnDataType='" + columnDataType + '\'' +
            ", foreignTableSchema='" + foreignTableSchema + '\'' +
            ", foreignTableName='" + foreignTableName + '\'' +
            ", foreignColumnName='" + foreignColumnName + '\'' +
            ", distinctValues=" + distinctValues +
            '}';
    }
}
