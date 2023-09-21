package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.time.LocalDate;
import java.util.List;
import java.util.Random;

/*
Description :- Generate Sequential date from 01-01-2023
Start Date :- 01-01-2023
Params :
1.int: numberofDays (values[0]) :- Number of days
2 int: offset (values[1])

Eg:-
numberofDays:- 10
Return type : (String):- 03-01-2023
*/
public class PrimaryDateGen implements BaseUtil {
    private final int numberofDays;

    private int offSet = 1;

    private int currentPlus = 0;
    LocalDate startDate =  LocalDate.of(2023, 1, 1);

    private final Random rd = new Random((int) System.nanoTime());


    public PrimaryDateGen(List<Object> values) {
        if (values.isEmpty()) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }

        this.numberofDays = values.get(0) instanceof String? Integer.parseInt((String) values.get(0)):  ((Number) values.get(0)).intValue();
        if(values.size() == 2){
           this.offSet = values.get(1) instanceof String? Integer.parseInt((String) values.get(1)):  ((Number) values.get(1)).intValue();
           this.currentPlus = this.offSet;
        }

        if (numberofDays <0)
            throw new RuntimeException("Please enter positive number of days");

    }

    public PrimaryDateGen(List<Object> values, int workerId, int totalWorkers) {

        if (values.isEmpty()) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }

        this.numberofDays = values.get(0) instanceof String? Integer.parseInt((String) values.get(0)):  ((Number) values.get(0)).intValue();
        if(values.size() == 2){
            this.offSet = values.get(1) instanceof String? Integer.parseInt((String) values.get(1)):  ((Number) values.get(1)).intValue();
            this.currentPlus = this.offSet;
        }
        int divide =  this.numberofDays / totalWorkers;
        this.currentPlus = this.currentPlus + divide * workerId ;

        System.out.println(this.currentPlus);

        if (numberofDays <0)
            throw new RuntimeException("Please enter positive number of days");

    }


    private int findNextHigherValue() {
        return currentPlus++;
    }

    @Override
    public Object run() {
        int vv = findNextHigherValue();
        return  startDate.plusDays(vv);
    }
}
