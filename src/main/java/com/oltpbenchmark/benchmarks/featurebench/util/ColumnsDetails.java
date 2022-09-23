package com.oltpbenchmark.benchmarks.featurebench.util;

public class ColumnsDetails {
    private final String name;
    private final UtilityFunc utilFunc;

    public ColumnsDetails(String name, UtilityFunc utilFunc) {
        this.name = name;
        this.utilFunc = utilFunc;
    }

    public UtilityFunc getUtilFunc() {
        return utilFunc;
    }

    public String getName() {
        return name;
    }
}
