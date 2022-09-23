package com.oltpbenchmark.benchmarks.featurebench.util;


public class ExecuteRule {
    private TransactionDetails transactionDetails;

    public ExecuteRule(TransactionDetails transactionDetails) {
        this.transactionDetails = transactionDetails;
    }

    public TransactionDetails getTransactionDetails() {
        return transactionDetails;
    }

    public void setTransactionDetails(TransactionDetails transactionDetails) {
        this.transactionDetails = transactionDetails;
    }
}
