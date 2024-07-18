package com.oltpbenchmark.benchmarks.dataloader;

import java.util.List;

public class PropertyMapping {
//    public String key;
    public String className;
    public int numParams;
    public List<Object> params;


    public PropertyMapping(String className, int numParams, List<Object> params) {
//        this.key = key;
        this.className = className;
        this.numParams = numParams;
        this.params = params;
    }

}
