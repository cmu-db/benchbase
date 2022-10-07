package com.oltpbenchmark.benchmarks.featurebench.utils;

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

import java.util.List;


public class RowRandomBoundedLong implements BaseUtil {
    private final RowRandomLong randomLong;
    private final RowRandomInt randomInt;

    private final long lowValue;
    private final long highValue;

    public RowRandomBoundedLong(List<Object> values) {
        if (values.size() != 4) {
            throw new RuntimeException("Incorrect number of parameters for util function");
        }
        if ((boolean) values.get(1)) {
            this.randomLong = new RowRandomLong((long) values.get(0), 1);
            this.randomInt = null;
        } else {
            this.randomLong = null;
            this.randomInt = new RowRandomInt((long) values.get(0), 1);
        }
        this.lowValue = ((Number) (int) values.get(2)).longValue();
        this.highValue = ((Number) (int) values.get(3)).longValue();
        if (lowValue > highValue)
            throw new RuntimeException("Please enter correct value for max and min value");
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

    @Override
    public Object run() {
        if (randomLong != null) {
            return randomLong.nextLong(lowValue, highValue);
        }
        return randomInt.nextInt((int) lowValue, (int) highValue);
    }
}
