package com.oltpbenchmark.benchmarks.featurebench.helpers;

import java.util.ArrayList;

public class TransactionDetails {
    private final String name;
    private final int weightTransactionType;
    private ArrayList<QueryDetails> query;

    public TransactionDetails(String name, int weightTransactionType, ArrayList<QueryDetails> query) {
        this.name = name;
        this.weightTransactionType = weightTransactionType;
        this.query = query;
    }

    public ArrayList<QueryDetails> getQuery() {
        return query;
    }

    public void setQuery(ArrayList<QueryDetails> query) {
        this.query = query;
    }

    public int getWeightTransactionType() {
        return weightTransactionType;
    }

    public String getName() {
        return name;
    }
}
