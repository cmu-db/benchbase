package com.oltpbenchmark.benchmarks.dataloader;

import java.util.ArrayList;
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

    public PropertyMapping(PropertyMapping other) {
        this.className = other.className;
        this.numParams = other.numParams;
        this.params = new ArrayList<>();
        for (Object param : other.params) {
            this.params.add(clone(param));
        }
    }

    private Object clone(Object obj) {
        if (obj instanceof Cloneable) {
            try {
                // Use reflection to call the clone method
                return obj.getClass().getMethod("clone").invoke(obj);
            } catch (Exception e) {
                throw new RuntimeException("Clone failed", e);
            }
        }
        // Fallback to simple object copy (might not work for all types)
        return obj;
    }
}
