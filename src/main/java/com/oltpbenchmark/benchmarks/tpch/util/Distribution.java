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

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import com.oltpbenchmark.util.RowRandomInt;

import static java.util.Objects.requireNonNull;

public class Distribution {
    private final String name;
    private final List<String> values;
    private final int[] weights;
    private final String[] distribution;
    private final int maxWeight;

    public Distribution(String name, Map<String, Integer> distribution) {
        this.name = requireNonNull(name, "name is null");
        requireNonNull(distribution, "distribution is null");

        List<String> values = new ArrayList<>();
        this.weights = new int[distribution.size()];

        int runningWeight = 0;
        int index = 0;
        boolean isValidDistribution = true;
        for (Entry<String, Integer> entry : distribution.entrySet()) {
            values.add(entry.getKey());

            runningWeight += entry.getValue();
            weights[index] = runningWeight;

            isValidDistribution &= entry.getValue() > 0;

            index++;
        }
        this.values = values;

        // "nations" is hack and not a valid distribution so we need to skip it
        if (isValidDistribution) {
            this.maxWeight = weights[weights.length - 1];
            this.distribution = new String[maxWeight];

            index = 0;
            for (String value : this.values) {
                int count = distribution.get(value);
                for (int i = 0; i < count; i++) {
                    this.distribution[index] = value;
                    index++;
                }
            }
        } else {
            this.maxWeight = -1;
            this.distribution = null;
        }
    }

    public String getValue(int index) {
        return values.get(index);
    }

    public List<String> getValues() {
        return values;
    }

    public int getWeight(int index) {
        return weights[index];
    }

    public int size() {
        return values.size();
    }

    public String randomValue(RowRandomInt randomInt) {
        int randomValue = randomInt.nextInt(0, maxWeight - 1);
        return distribution[randomValue];
    }
}
