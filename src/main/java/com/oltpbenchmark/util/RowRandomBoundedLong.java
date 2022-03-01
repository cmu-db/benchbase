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

public class RowRandomBoundedLong {
    private final RowRandomLong randomLong;
    private final RowRandomInt randomInt;

    private final long lowValue;
    private final long highValue;

    public RowRandomBoundedLong(long seed, boolean use64Bits, long lowValue, long highValue) {
        this(seed, use64Bits, lowValue, highValue, 1);
    }

    public RowRandomBoundedLong(long seed, boolean use64Bits, long lowValue, long highValue, int seedsPerRow) {
        if (use64Bits) {
            this.randomLong = new RowRandomLong(seed, seedsPerRow);
            this.randomInt = null;
        } else {
            this.randomLong = null;
            this.randomInt = new RowRandomInt(seed, seedsPerRow);
        }

        this.lowValue = lowValue;
        this.highValue = highValue;
    }

    public long nextValue() {
        if (randomLong != null) {
            return randomLong.nextLong(lowValue, highValue);
        }
        return randomInt.nextInt((int) lowValue, (int) highValue);
    }

    public void rowFinished() {
        if (randomLong != null) {
            randomLong.rowFinished();
        } else {
            randomInt.rowFinished();
        }
    }

    public void advanceRows(long rowCount) {
        if (randomLong != null) {
            randomLong.advanceRows(rowCount);
        } else {
            randomInt.advanceRows(rowCount);
        }
    }
}
