package com.oltpbenchmark.benchmarks.featurebench.workerhelpers;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.oltpbenchmark.benchmarks.featurebench.helpers.UtilToMethod;

import java.util.List;

public class Query {
    @JsonProperty("query")
    public String query;
    public int count = 1;
    public List<UtilToMethod> baseUtils;
    public boolean isSelectQuery = false;

    public List<UtilToMethod> getBaseUtils() {
        return baseUtils;
    }

    public void setBaseUtils(List<UtilToMethod> baseUtils) {
        this.baseUtils = baseUtils;
    }

    public String getQuery() {
        return query;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public boolean isSelectQuery() {
        return isSelectQuery;
    }

    public void setSelectQuery(boolean selectQuery) {
        isSelectQuery = selectQuery;
    }
}