package com.oltpbenchmark.benchmarks.featurebench.helpers;

import java.util.ArrayList;

public class UtilityFunc {

    private final String name;
    private final ArrayList<ParamsForUtilFunc> params;

    public UtilityFunc(String name, ArrayList<ParamsForUtilFunc> params) {
        this.name = name;

        this.params = params;
    }

    public ArrayList<ParamsForUtilFunc> getParams() {
        return params;
    }

    public String getName() {
        return name;
    }
}
