package com.oltpbenchmark.benchmarks.featurebench.workerhelpers;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.oltpbenchmark.benchmarks.featurebench.helpers.UtilToMethod;

import java.util.List;

public class Query {
    @JsonProperty("query")
    public String query;

    public List<UtilToMethod> baseUtils;


    public List<UtilToMethod> getBaseUtils() {
        return baseUtils;
    }

    public void setBaseUtils(List<UtilToMethod> baseUtils) {
        this.baseUtils = baseUtils;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

}
