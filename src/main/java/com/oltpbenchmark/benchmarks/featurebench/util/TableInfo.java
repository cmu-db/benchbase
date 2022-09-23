package com.oltpbenchmark.benchmarks.featurebench.util;

import java.util.ArrayList;

public class TableInfo {

    private final int no_of_rows;
    private final String table_name;
    private final ArrayList<ColumnsDetails> column_Det;

    public TableInfo(int no_of_rows, String table_name, ArrayList<ColumnsDetails> column_det) {

        this.no_of_rows = no_of_rows;
        this.table_name = table_name;
        this.column_Det = column_det;
    }


    public int getNo_of_rows() {
        return no_of_rows;
    }

    public String get_table_name() {
        return table_name;
    }

    public ArrayList<ColumnsDetails> getColumn_Det() {
        return column_Det;
    }
}
