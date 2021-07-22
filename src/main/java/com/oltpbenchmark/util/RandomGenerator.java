/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package com.oltpbenchmark.util;

import java.util.Random;

public class RandomGenerator extends Random {

    /**
     * Constructor
     *
     * @param seed
     */
    public RandomGenerator(int seed) {
        super(seed);
    }

    /**
     * Returns a random int value between minimum and maximum (inclusive)
     *
     * @param minimum
     * @param maximum
     * @returns a int in the range [minimum, maximum]. Note that this is inclusive.
     */
    public int number(int minimum, int maximum) {

        int range_size = maximum - minimum + 1;
        int value = this.nextInt(range_size);
        value += minimum;

        return value;
    }

    /**
     * Returns a random long value between minimum and maximum (inclusive)
     *
     * @param minimum
     * @param maximum
     * @return
     */
    public long number(long minimum, long maximum) {

        long range_size = (maximum - minimum) + 1;

        // error checking and 2^x checking removed for simplicity.
        long bits, val;
        do {
            bits = (this.nextLong() << 1) >>> 1;
            val = bits % range_size;
        }
        while (bits - val + range_size < 0L);
        val += minimum;


        return val;
    }

    /**
     * @param decimal_places
     * @param minimum
     * @param maximum
     * @return
     */
    public double fixedPoint(int decimal_places, double minimum, double maximum) {


        int multiplier = 1;
        for (int i = 0; i < decimal_places; ++i) {
            multiplier *= 10;
        }

        int int_min = (int) (minimum * multiplier + 0.5);
        int int_max = (int) (maximum * multiplier + 0.5);

        return (double) this.number(int_min, int_max) / (double) multiplier;
    }

    /**
     * @returns a random alphabetic string with length in range [minimum_length, maximum_length].
     */
    public String astring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, 'a', 26);
    }


    /**
     * @returns a random numeric string with length in range [minimum_length, maximum_length].
     */
    public String nstring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, '0', 10);
    }

    /**
     * @param minimum_length
     * @param maximum_length
     * @param base
     * @param numCharacters
     * @return
     */
    private String randomString(int minimum_length, int maximum_length, char base, int numCharacters) {
        int length = number(minimum_length, maximum_length);
        byte baseByte = (byte) base;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = (byte) (baseByte + number(0, numCharacters - 1));
        }
        return new String(bytes);
    }
}