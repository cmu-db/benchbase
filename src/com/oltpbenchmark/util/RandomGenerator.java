/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.util;

import java.util.*;

@SuppressWarnings("serial")
public class RandomGenerator extends Random {

    /**
     * Constructor
     * @param rand
     */
    public RandomGenerator(int seed) {
        super(seed);
    }
    
    public Set<Integer> getRandomIntSet(int cnt, int max) {
        assert(cnt <= max);
        Set<Integer> ret = new HashSet<Integer>();
        do { 
            ret.add(this.nextInt(max));
        } while (ret.size() < cnt);
        return (ret);
    }

    /**
     * Returns a random int value between minimum and maximum (inclusive)
     * @param minimum
     * @param maximum
     * @returns a int in the range [minimum, maximum]. Note that this is inclusive.
     */
    public int number(int minimum, int maximum) {
        assert minimum <= maximum : String.format("%d <= %d", minimum, maximum);
        int range_size = maximum - minimum + 1;
        int value = this.nextInt(range_size);
        value += minimum;
        assert minimum <= value && value <= maximum;
        return value;
    }
    
    /**
     * Returns a random long value between minimum and maximum (inclusive)
     * @param minimum
     * @param maximum
     * @return
     */
    public long number(long minimum, long maximum) {
        assert minimum <= maximum : String.format("%d <= %d", minimum, maximum);
        long range_size = (maximum - minimum) + 1;
        
        // error checking and 2^x checking removed for simplicity.
        long bits, val;
        do {
            bits = (this.nextLong() << 1) >>> 1;
            val = bits % range_size;
        } while (bits - val + range_size < 0L);
        val += minimum;
        assert(val >= minimum);
        assert(val <= maximum);
        return val;
    }
    
    /**
     * 
     * @param minimum
     * @param maximum
     * @param excluding
     * @returns an int in the range [minimum, maximum], excluding excluding.
     */
    public int numberExcluding(int minimum, int maximum, int excluding) {
        assert minimum < maximum;
        assert minimum <= excluding && excluding <= maximum;

        // Generate 1 less number than the range
        int num = number(minimum, maximum-1);

        // Adjust the numbers to remove excluding
        if (num >= excluding) {
            num += 1;
        }
        assert minimum <= num && num <= maximum && num != excluding;
        return num;
    }
    
    /**
     * Returns a random int in a skewed gaussian distribution of the range
     * Note that the range is inclusive
     * A skew factor of 0.0 means that it's a uniform distribution
     * The greater the skew factor the higher the probability the selected random
     * value will be closer to the mean of the range
     *  
     * @param minimum the minimum random number
     * @param maximum the maximum random number
     * @param skewFactor the factor to skew the stddev of the gaussian distribution
     */
    public int numberSkewed(int minimum, int maximum, double skewFactor) {
        // Calling number() when the skewFactor is zero will likely be faster
        // than using our Gaussian distribution method below 
        if (skewFactor == 0) return (this.number(minimum, maximum));

        assert minimum <= maximum;
        int range_size = maximum - minimum + 1;
        int mean = range_size / 2;
        double stddev = range_size - ((range_size / 1.1) * skewFactor);
        int value = -1;
        while (value < 0 || value >= range_size) {
            value = (int) Math.round(this.nextGaussian() * stddev) + mean;
        }
        value += minimum;
        assert minimum <= value && value <= maximum;
        return value;
    }

    /**
     * 
     * @param decimal_places
     * @param minimum
     * @param maximum
     * @return
     */
    public double fixedPoint(int decimal_places, double minimum, double maximum) {
        assert decimal_places > 0;
        assert minimum < maximum : String.format("%f < %f", minimum, maximum);

        int multiplier = 1;
        for (int i = 0; i < decimal_places; ++i) {
            multiplier *= 10;
        }

        int int_min = (int)(minimum * multiplier + 0.5);
        int int_max = (int)(maximum * multiplier + 0.5);
        
        return (double)this.number(int_min, int_max) / (double) multiplier;
    }
    
    /** @returns a random alphabetic string with length in range [minimum_length, maximum_length].
     */
    public String astring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, 'a', 26);
    }


    /** @returns a random numeric string with length in range [minimum_length, maximum_length].
     */
    public String nstring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, '0', 10);
    }

    /**
     * 
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
            bytes[i] = (byte)(baseByte + number(0, numCharacters-1));
        }
        return new String(bytes);
    }
}