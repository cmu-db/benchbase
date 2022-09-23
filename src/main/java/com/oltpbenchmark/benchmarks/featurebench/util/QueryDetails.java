package com.oltpbenchmark.benchmarks.featurebench.util;

import java.util.ArrayList;

public class QueryDetails {
    private final String query;
    private final ArrayList<BindParams> bindParams;

    public QueryDetails(String query, ArrayList<BindParams> bindParams) {
        this.query = query;
        this.bindParams = bindParams;
    }

    public ArrayList<BindParams> getBindParams() {
        return bindParams;
    }

    public String getQuery() {
        return query;
    }
}
