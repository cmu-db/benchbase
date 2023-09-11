package com.oltpbenchmark.benchmarks.featurebench.utils;

import com.oltpbenchmark.benchmarks.featurebench.helpers.MD5hash;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/*

Params :
1.int startNumber (values[0]) :- starting number for numeric string Primary Key.
2.int desiredLength (values[1]) :- desired length for numeric string Primary key.(extra characters appended by 'a').

Eg:-
startNumber=0, desiredLength: 5
String Numeric Primary keys generated :- "0aaaa","1aaaa","2aaaa","3aaaa",......
Return type :- String (Numeric)
*/

public class HashedPrimaryStringGen implements BaseUtil {
    private final int desiredLength;
    private final int startNumber;
    private int currentValue;
    private String key;

    public HashedPrimaryStringGen(List<Object> values) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.startNumber = ((Number) values.get(0)).intValue();
        this.currentValue = startNumber - 1;
        this.desiredLength = ((Number) values.get(1)).intValue();
        if (desiredLength <= 0) {
            throw new RuntimeException("Please use positive desired length for string primary keys");
        }
    }

    public HashedPrimaryStringGen(List<Object> values, int workerId, int totalWorkers) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        int divide = (((Number) values.get(1)).intValue() - ((Number) values.get(0)).intValue()) / totalWorkers;
        this.startNumber = ((Number) values.get(0)).intValue() - 1 + divide * workerId;
        this.currentValue = startNumber - 1;
        int upperRangeTemp = (((Number) values.get(0)).intValue() + (divide) * (workerId + 1) + (workerId == 0 ? 0 : 1));
        this.desiredLength = Math.min(upperRangeTemp, ((Number) values.get(1)).intValue());
        if (desiredLength <= 0) {
            throw new RuntimeException("Please use positive desired length for string primary keys");
        }
    }

    public String numberToIdString() {
        String md5HASHString= MD5hash.getMd5(String.valueOf(currentValue));
        int repeatCount = (int) Math.ceil((double) desiredLength /32);
        String baseNumberStr = StringUtils.repeat(md5HASHString, repeatCount);
        return baseNumberStr.length() == desiredLength ? baseNumberStr : baseNumberStr.substring(0,desiredLength);
    }

    @Override
    public Object run() {
        currentValue++;
        key = numberToIdString();
        return key;
    }
}