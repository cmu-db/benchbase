package com.oltpbenchmark.benchmarks.indexjungle;

public class IndexJungleUtil {

    /**
     *
     * @param offset
     * @return
     */
    public static String generateUUID(long offset) {
        StringBuilder sb = new StringBuilder();

        // time low
        sb.append(new StringBuilder(String.format("%08d", offset % 10000)).reverse());
        sb.append("-");

        // time_mid
        sb.append(String.format("%04d", offset % 1000));
        sb.append("-");

        // time_hi
        sb.append(String.format("%04d", (offset*33) % 1000));
        sb.append("-");

        // clock_seq
        sb.append(String.format("%04d", (offset*99) % 1000));
        sb.append("-");

        // node
        sb.append(String.format("%08d", offset));

        return (sb.toString());
    }
}
