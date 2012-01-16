/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:    Carlo Curino <carlo.curino@gmail.com>
 *              Evan Jones <ej@evanjones.ca>
 *              DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 *              Andy Pavlo <pavlo@cs.brown.edu>
 *              CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *                  Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *                                                                         *
 *  Modifications by:                                                      *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package com.oltpbenchmark.benchmarks.tatp;

import java.util.Random;

public abstract class TATPUtil {
    
    public static final Random rand = new Random();

    public static byte isActive() {
        return (byte)(number(1,100) < number(86,100) ? 1 : 0);
    }

    public static Long getSubscriberId(long subscriberSize) {
        return (TATPUtil.number(1, subscriberSize));
    }
    
    // modified from tpcc.RandomGenerator
    /**
     * @returns a random alphabetic string with length in range [minimum_length, maximum_length].
     */
    public static String astring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, 'A', 26);
    }

    // taken from tpcc.RandomGenerator
    /**
     * @returns a random numeric string with length in range [minimum_length, maximum_length].
     */
    public static String nstring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, '0', 10);
    }

    // taken from tpcc.RandomGenerator 
    public static String randomString(int minimum_length, int maximum_length, char base, int numCharacters) {
        int length = number(minimum_length, maximum_length).intValue();
        byte baseByte = (byte) base;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = (byte)(baseByte + number(0, numCharacters-1));
        }
        return new String(bytes);
    }

    // taken from tpcc.RandomGenerator
    public static Long number(long minimum, long maximum) {
        assert minimum <= maximum;
        long value = Math.abs( rand.nextLong() ) % ( maximum - minimum + 1 ) + minimum;
        assert minimum <= value && value <= maximum;
        return value;
    }
    
    public static String padWithZero(long n) {
        String str = Long.toString(n);
        char[] zeros = new char[TATPConstants.SUB_NBR_PADDING_SIZE - str.length()];
        for (int i = 0; i < zeros.length; i++)
            zeros[i] = '0';
        return (new String(zeros) + str);
    }

    /**
     * Returns sub array of arr, with length in range [min_len, max_len].
     * Each element in arr appears at most once in sub array.
     */
    public static int[] subArr(int arr[], int min_len, int max_len) {
        assert min_len <= max_len && min_len >= 0;
        int sub_len = number(min_len, max_len).intValue();
        int arr_len = arr.length;

        assert sub_len <= arr_len;

        int sub[] = new int[sub_len];
        for (int i = 0; i < sub_len; i++) {
            int j = number(0, arr_len - 1).intValue();
            sub[i] = arr[j];
            //arr[j] put to tail
            int tmp = arr[j];
            arr[j] = arr[arr_len - 1];
            arr[arr_len - 1] = tmp;

            arr_len--;
        } // FOR

        return sub;
    }
    
}
