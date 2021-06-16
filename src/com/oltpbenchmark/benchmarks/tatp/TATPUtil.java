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


package com.oltpbenchmark.benchmarks.tatp;

import java.util.Random;

public abstract class TATPUtil {

    public static final Random rand = new Random();

    public static byte isActive() {
        return (byte) (number(1, 100) < number(86, 100) ? 1 : 0);
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
            bytes[i] = (byte) (baseByte + number(0, numCharacters - 1));
        }
        return new String(bytes);
    }

    // taken from tpcc.RandomGenerator
    public static Long number(long minimum, long maximum) {

        return Math.abs(rand.nextLong()) % (maximum - minimum + 1) + minimum;
    }

    public static String padWithZero(long n) {
        String str = Long.toString(n);
        char[] zeros = new char[TATPConstants.SUB_NBR_PADDING_SIZE - str.length()];
        for (int i = 0; i < zeros.length; i++) {
            zeros[i] = '0';
        }
        return (new String(zeros) + str);
    }

    /**
     * Returns sub array of arr, with length in range [min_len, max_len].
     * Each element in arr appears at most once in sub array.
     */
    public static int[] subArr(int[] arr, int min_len, int max_len) {

        int sub_len = number(min_len, max_len).intValue();
        int arr_len = arr.length;


        int[] sub = new int[sub_len];
        for (int i = 0; i < sub_len; i++) {
            int j = number(0, arr_len - 1).intValue();
            sub[i] = arr[j];
            //arr[j] put to tail
            int tmp = arr[j];
            arr[j] = arr[arr_len - 1];
            arr[arr_len - 1] = tmp;

            arr_len--;
        }

        return sub;
    }

}
