package com.oltpbenchmark.benchmarks.featurebench.util;

public abstract class UtilGenerators  {
    private static int counter;
    private static int starter_for_string_counter = 0;
    private static int upper_range_for_primary_int_keys;
    private static int lower_range_for_primary_int_keys;
    private static int desired_length_string_pkeys;


    public static int getUpper_range_for_primary_int_keys() {
        return upper_range_for_primary_int_keys;
    }

    public  static void setUpper_range_for_primary_int_keys(int upper_range_for_primary_int_keys) {
        UtilGenerators.upper_range_for_primary_int_keys = upper_range_for_primary_int_keys;
    }

    public static int getLower_range_for_primary_int_keys() {
        return lower_range_for_primary_int_keys;
    }

    public static void setLower_range_for_primary_int_keys(int lower_range_for_primary_int_keys) {
        UtilGenerators.lower_range_for_primary_int_keys = lower_range_for_primary_int_keys;
        counter = lower_range_for_primary_int_keys;
    }

    public static int getDesired_length_string_pkeys() {
        return desired_length_string_pkeys;
    }

    public static void setDesired_length_string_pkeys(int desired_length_string_pkeys) {
        UtilGenerators.desired_length_string_pkeys = desired_length_string_pkeys;
    }

    public static int get_int_primary_key() {
        if (counter > upper_range_for_primary_int_keys) {
            throw new RuntimeException("Index out of range");
        } else {
            counter++;
            return counter;
        }
    }

    public static String numberToIdString() {
        StringBuilder baseNumberStr = new StringBuilder(String.valueOf(starter_for_string_counter));
        while (baseNumberStr.length() < desired_length_string_pkeys) {
            baseNumberStr.append('a');
        }
        starter_for_string_counter++;
        return baseNumberStr.toString();
    }

}
