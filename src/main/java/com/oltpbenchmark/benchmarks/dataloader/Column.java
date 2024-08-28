package com.oltpbenchmark.benchmarks.dataloader;

public class Column {
    private String columnName;
    private String dataType;
    private String baseDataType;
    private Integer characterMaximumLength;
    private Boolean isIdentity;
    private Boolean nullable;

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

    public Integer getCharacterMaximumLength() {
        return characterMaximumLength;
    }

    public void setCharacterMaximumLength(Integer characterMaximumLength) {
        this.characterMaximumLength = characterMaximumLength;
    }

    public String getBaseDataType() {
        return baseDataType;
    }

    public void setBaseDataType(String baseDataType) {
        this.baseDataType = baseDataType;
    }

    public Boolean getIdentity() {
        return isIdentity;
    }

    public void setIdentity(Boolean identity) {
        isIdentity = identity;
    }

    public Boolean getIsIdentity() {
        return isIdentity;
    }

    public void setIsIdentity(Boolean isIdentity) {
        this.isIdentity = isIdentity;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    @Override
    public String toString() {
        return "Column{" +
            "columnName='" + columnName + '\'' +
            ", dataType='" + dataType + '\'' +
            ", baseDataType='" + baseDataType + '\'' +
            ", characterMaximumLength=" + characterMaximumLength +
            ", isIdentity=" + isIdentity +
            ", nullable=" + nullable +
            '}';
    }
}
