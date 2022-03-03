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
package com.oltpbenchmark.benchmarks.tpch.util;

import com.oltpbenchmark.util.StringUtil;
import com.oltpbenchmark.util.RowRandomInt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TPCHRandomStringSequence
        extends RowRandomInt {
    private final int count;
    private final Distribution distribution;

    public TPCHRandomStringSequence(long seed, int count, Distribution distribution) {
        this(seed, count, distribution, 1);
    }

    public TPCHRandomStringSequence(long seed, int count, Distribution distribution, int seedsPerRow) {
        super(seed, distribution.size() * seedsPerRow);
        this.count = count;
        this.distribution = distribution;
    }

    public String nextValue() {
        List<String> values = new ArrayList<>(distribution.getValues());

        // randomize first 'count' elements of the string
        for (int currentPosition = 0; currentPosition < count; currentPosition++) {
            int swapPosition = nextInt(currentPosition, values.size() - 1);
            Collections.swap(values, currentPosition, swapPosition);
        }

        // join random words
        String result = StringUtil.join(" ", values.subList(0, count));
        return result;
    }
}
