package com.oltpbenchmark.benchmarks.featurebench.utils;


import java.util.List;


/*
Description :- Cyclic Sequential Integer Generator between a range.
Params :
1.int: lowerRange (values[0]) :- Lower Range.
2.int: upperRange (values[1]) :- Upper Range.

Eg:-
lowerRange:- 1, upperRange: 10
Return type (Integer) :- All values between 1 and 10 including these bounds and repeats the sequence in cyclic way.
Sample output : 1 2 3 4 5 6 7 8 9 10 1 2 3 4 ...
*/

public class CyclicSeqIntGen implements BaseUtil {
    private final int upperRange;
    private final int lowerRange;
    private int currentValue;

    public CyclicSeqIntGen(List<Object> values) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.currentValue = ((Number) values.get(0)).intValue() - 1;
        this.upperRange = ((Number) values.get(1)).intValue();
        this.lowerRange = ((Number) values.get(0)).intValue();
        if (upperRange < lowerRange) {
            throw new RuntimeException("Upper bound less than lower bound");
        }
    }

    public CyclicSeqIntGen(List<Object> values, int workerId, int totalWorkers) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        int divide = (((Number) values.get(1)).intValue() - ((Number) values.get(0)).intValue()) / totalWorkers;
        this.currentValue = ((Number) values.get(0)).intValue() - 1 + divide * workerId;
        int upperRangeTemp = (((Number) values.get(0)).intValue() + (divide) * (workerId + 1) + (workerId == 0 ? 0 : 1));
        this.upperRange = Math.min(upperRangeTemp, ((Number) values.get(1)).intValue());
        this.lowerRange = ((Number) values.get(0)).intValue() + divide * (workerId) + (workerId == 0 ? 0 : 1);
        if (upperRange < lowerRange) {
            throw new RuntimeException("Upper bound less than lower bound");
        }

    }

    private int findNextHigherValue() {
        currentValue++;
        //The current value reinitializes to lowerRange once it reaches upperRange
        if(currentValue>upperRange)
        {
            currentValue=lowerRange;
        }
        return currentValue;
    }

    @Override
    public Object run() {
        currentValue = findNextHigherValue();
        return currentValue;
    }
}
