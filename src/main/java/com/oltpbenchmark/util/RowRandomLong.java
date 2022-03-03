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

public class RowRandomLong {
    private static final long MULTIPLIER = 6364136223846793005L;
    private static final long INCREMENT = 1;

    private final int seedsPerRow;

    private long seed;
    private int usage;

    /**
     * Creates a new random number generator with the specified seed and
     * specified number of random values per row.
     */
    public RowRandomLong(long seed, int seedsPerRow) {
        this.seed = seed;
        this.seedsPerRow = seedsPerRow;
    }

    /**
     * Get a random value between lowValue (inclusive) and highValue (inclusive).
     */
    protected long nextLong(long lowValue, long highValue) {
        nextRand();

        long valueInRange = Math.abs(seed) % (highValue - lowValue + 1);

        return lowValue + valueInRange;
    }

    protected long nextRand() {
        seed = (seed * MULTIPLIER) + INCREMENT;
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
        advanceSeed32(seedsPerRow - usage);
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
        advanceSeed32(seedsPerRow * rowCount);
    }

    private static final long MULTIPLIER_32 = 16807;
    private static final long MODULUS_32 = 2147483647;

    private void advanceSeed32(long count) {
        long multiplier = MULTIPLIER_32;
        while (count > 0) {
            // testing for oddness, this seems portable
            if (count % 2 != 0) {
                seed = (multiplier * seed) % MODULUS_32;
            }
            // integer division, truncates
            count = count / 2;
            multiplier = (multiplier * multiplier) % MODULUS_32;
        }
    }
}
