package com.oltpbenchmark.benchmarks.featurebench.helpers;

import java.util.ArrayList;

public class TableInfo {

    private final int noOfRows;
    private final String tableName;
    private final ArrayList<ColumnsDetails> columnDet;

    public TableInfo(int noOfRows, String tableName, ArrayList<ColumnsDetails> column_det) {

        this.noOfRows = noOfRows;
        this.tableName = tableName;
        this.columnDet = column_det;
    }


    public int getNoOfRows() {
        return noOfRows;
    }

    public String get_table_name() {
        return tableName;
    }

    public ArrayList<ColumnsDetails> getColumnDet() {
        return columnDet;
    }
}
