package com.oltpbenchmark.benchmarks.dataloader;

public class Column {
    private String columnName;
    private String dataType;
    private Integer characterMaximumLength;
    private Boolean isIdentity;

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

    public Boolean getIsIdentity() {
        return isIdentity;
    }

    public void setIsIdentity(Boolean isIdentity) {
        this.isIdentity = isIdentity;
    }

    @Override
    public String toString() {
        return "Column{" +
            "columnName='" + columnName + '\'' +
            ", dataType='" + dataType + '\'' +
            ", characterMaximumLength=" + characterMaximumLength +
            ", isIdentity=" + isIdentity +
            '}';
    }
}
