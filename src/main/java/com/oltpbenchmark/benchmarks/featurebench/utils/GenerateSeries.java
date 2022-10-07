/*
package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class GenerateSeries implements BaseUtil {

    protected int jumpValue;
    protected int startValue;
    protected int endValue;

    protected int currentVal;

    public GenerateSeries(List<Object> values) {
        if (values.size() != 3) {
            throw new RuntimeException("Incorrect number of parameters");
        }
        this.startValue = (int) values.get(0);
        this.endValue = (int) values.get(1);
        this.jumpValue = (int) values.get(2);
        this.currentVal = startValue - jumpValue;
        if (startValue >= endValue || jumpValue <= 0) {
            throw new RuntimeException("Please enter correct lower and upper range and jumpValue");
        }
    }

    private int findNextHigherValue() {
        currentVal += jumpValue;
        if (currentVal > endValue) {
            return -1;
        }
        return currentVal;
    }

    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        currentVal += jumpValue;
        if (currentVal > endValue) {
            return null;
        }
        return currentVal;
    }
}
*/
