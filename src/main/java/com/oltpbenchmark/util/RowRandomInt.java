/*
 * Copyright 2020 Trino
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
 */
package com.oltpbenchmark.util;

public class RowRandomInt {
    private static final long MULTIPLIER = 16807;
    private static final long MODULUS = 2147483647;
    private static final long DEFAULT_SEED = 19620718;

    private final int seedsPerRow;

    private long seed;
    private int usage;

    /**
     * Creates a new random number generator with the specified seed and
     * specified number of random values per row.
     */
    public RowRandomInt(long seed, int seedsPerRow) {
        this.seed = seed;
        this.seedsPerRow = seedsPerRow;
    }

    /**
     * Creates a new random generator with specified column number
     * and specified number of random values per row.
     * Seed is base + colnum * Constant.
     */
    public RowRandomInt(int globalColumnNumber, int seedsPerRow) {
        this(globalColumnNumber, DEFAULT_SEED, seedsPerRow);
    }

    /**
     * Creates a new random generator with specified column number
     * and specified number of random values per row.
     * Seed is base + colnum * Constant.
     */
    public RowRandomInt(int globalColumnNumber, long seed, int seedsPerRow) {
        this.seed = seed + globalColumnNumber * (MODULUS / 799);
        this.seedsPerRow = seedsPerRow;
    }

    /**
     * Get a random value between lowValue (inclusive) and highValue (inclusive).
     */
    public int nextInt(int lowValue, int highValue) {
        nextRand();

        // This will result in overflow when high is max int and low is 0,
        // which is a bug since you will get a value outside of the
        // specified range. There is code that relies on this bug.
        int intRange = highValue - lowValue + 1;
        double doubleRange = (double) intRange;
        int valueInRange = (int) ((1.0 * seed / MODULUS) * doubleRange);

        return lowValue + valueInRange;
    }

    public long nextRand() {
        seed = (seed * MULTIPLIER) % MODULUS;
        usage++;
        return seed;
    }

    /**
     * Advances the random number generator to the start of the sequence for
     * the next row. Each row uses a specified number of random values, so the
     * random number generator can be quickly advanced for partitioned data
     * sets.
     */
    public void rowFinished() {
        advanceSeed(seedsPerRow - usage);
        usage = 0;
    }

    /**
     * Advance the specified number of rows. Advancing to a specific row is
     * needed for partitioned data sets.
     */
    public void advanceRows(long rowCount) {
        // finish the current row
        if (usage != 0) {
            rowFinished();
        }

        // advance the seed
        advanceSeed(seedsPerRow * rowCount);
    }

    private void advanceSeed(long count) {
        long multiplier = MULTIPLIER;
        while (count > 0) {
            if (count % 2 != 0) {
                seed = (multiplier * seed) % MODULUS;
            }
            // integer division, truncates
            count = count / 2;
            multiplier = (multiplier * multiplier) % MODULUS;
        }
    }
}
