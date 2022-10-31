package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.sql.Timestamp;
import java.util.List;

/* Description :-
  SQL TIMESTAMP value.
  It adds the ability to hold the SQL TIMESTAMP fractional seconds value, by allowing the specification of fractional seconds to a precision of nanoseconds.
  Constructs a Timestamp object using a milliseconds time value.(Can be used for DATE in SQL schema)
  Params:- Empty list
  Returns :- year-month-date  hour:minutes:seconds.nanoseconds
  Eg: 2022-10-06 07:41:21.887
 */

public class CurrentTime implements BaseUtil {

    public CurrentTime(List<Object> values) {
        if (values.size() != 0) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
    }

    public Object run() {
        return new Timestamp(System.currentTimeMillis());
    }
}