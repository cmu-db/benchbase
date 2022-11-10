package com.oltpbenchmark.benchmarks.featurebench.utils;


import java.text.SimpleDateFormat;
import java.util.List;

/*
  Returns the CurrentTime in yyyyMMddHHmmss format
  Params:- Empty list
  Return type:- String :- Eg:- 20221006080333
 */

public class CurrentTimeString14 implements BaseUtil {

    protected static SimpleDateFormat DATE_FORMAT_14;

    public CurrentTimeString14(List<Object> values) {
        if (values.size() != 0) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        DATE_FORMAT_14 = new SimpleDateFormat("yyyyMMddHHmmss");
    }
    public CurrentTimeString14(List<Object> values,int workerId, int totalWorkers) {
        if (values.size() != 0) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        DATE_FORMAT_14 = new SimpleDateFormat("yyyyMMddHHmmss");
    }

    public Object run() {
        return CurrentTimeString14.DATE_FORMAT_14.format(new java.util.Date());
    }
}
