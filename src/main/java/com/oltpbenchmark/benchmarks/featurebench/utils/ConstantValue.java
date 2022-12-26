package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class ConstantValue implements BaseUtil {

    Object constantVal;

    public ConstantValue(List<Object> values) {
        if (values.size() != 1) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.constantVal = values.get(0);
    }

    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return constantVal;
    }
}
