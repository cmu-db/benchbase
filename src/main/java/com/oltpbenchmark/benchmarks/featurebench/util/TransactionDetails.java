package com.oltpbenchmark.benchmarks.featurebench.util;

import java.util.ArrayList;

public class TransactionDetails {
    private final String name;
    private final int weight_transaction_type;
    private ArrayList<QueryDetails> query;

    public TransactionDetails(String name, int weight_transaction_type, ArrayList<QueryDetails> query) {
        this.name = name;
        this.weight_transaction_type = weight_transaction_type;
        this.query = query;
    }

    public ArrayList<QueryDetails> getQuery() {
        return query;
    }

    public void setQuery(ArrayList<QueryDetails> query) {
        this.query = query;
    }

    public int getWeight_transaction_type() {
        return weight_transaction_type;
    }

    public String getName() {
        return name;
    }
}
