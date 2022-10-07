package com.oltpbenchmark.benchmarks.featurebench.utils;


import java.text.SimpleDateFormat;
import java.util.List;

/*
  Returns the CurrentTime in yyyy-MM-dd_HH-mm-ss format
  Params:- Empty list
  Return type:- String :- Eg:- 2022-10-06_07-58-08
 */

public class CurrentTimeString implements BaseUtil {

    protected static SimpleDateFormat DATE_FORMAT;

    public CurrentTimeString(List<Object> values) {
        if (values.size() != 0) {
            throw new RuntimeException("Incorrect number of parameters for util function");
        }
        DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
    }

    public Object run() {
        return CurrentTimeString.DATE_FORMAT.format(new java.util.Date());
    }
}