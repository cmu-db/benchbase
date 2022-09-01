package com.oltpbenchmark.benchmarks.featurebench.util;

import java.util.ArrayList;

public class BindParams {
    private final ArrayList<UtilityFunc> utilFunc;

    public BindParams(ArrayList<UtilityFunc> utilFunc) {
        this.utilFunc = utilFunc;
    }

    public ArrayList<UtilityFunc> getUtilFunc() {
        return utilFunc;
    }
}
