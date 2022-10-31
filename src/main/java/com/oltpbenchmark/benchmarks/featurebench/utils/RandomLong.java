package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Random;

/**
 * Returns a random long value between minimum and maximum (inclusive)
 *
 * @param minimum
 * @param maximum
 * @return
 */

public class RandomLong extends Random implements BaseUtil {

    private long minimum;
    private long maximum;

    public RandomLong(List<Object> values) {
        super((int) System.nanoTime());
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.minimum = ((Number) values.get(0)).longValue();
        this.maximum = ((Number) values.get(1)).longValue();
        if (maximum < minimum)
            throw new RuntimeException("Please enter correct values for min and max");
    }

    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException,
        InstantiationException, IllegalAccessException {
        long range_size = (maximum - minimum) + 1;

        // error checking and 2^x checking removed for simplicity.
        long bits, val;
        do {
            bits = (this.nextLong() << 1) >>> 1;
            val = bits % range_size;
        } while (bits - val + range_size < 0L);
        val += minimum;

        return val;
    }
}
