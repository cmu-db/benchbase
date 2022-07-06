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

import com.oltpbenchmark.util.RowRandomInt;

public class TPCHRandomString
        extends RowRandomInt {
    private final Distribution distribution;

    public TPCHRandomString(long seed, Distribution distribution) {
        this(seed, distribution, 1);
    }

    public TPCHRandomString(long seed, Distribution distribution, int seedsPerRow) {
        super(seed, seedsPerRow);
        this.distribution = distribution;
    }

    public String nextValue() {
        return distribution.randomValue(this);
    }
}
