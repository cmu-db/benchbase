package com.oltpbenchmark.benchmarks.featurebench.workerhelpers;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ExecuteRule {
    @JsonProperty("name")
    public String name;

    @JsonProperty("weight")
    public double weight;

    @JsonProperty("queries")
    public List<Query> queries;


//    public ExecuteRule(String name, double weight, List<Queries> queries) {
//        this.name = name;
//        this.weight = weight;
//        this.queries = queries;
//    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public List<Query> getQueries() {
        return queries;
    }

    public void setQueries(List<Query> queries) {
        this.queries = queries;
    }
}
