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

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import static java.util.Objects.requireNonNull;

public class NationGenerator
        implements Iterable<List<Object>> {
    private static final int COMMENT_AVERAGE_LENGTH = 72;

    private final Distributions distributions;
    private final TextPool textPool;

    public NationGenerator() {
        this(Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool());
    }

    public NationGenerator(Distributions distributions, TextPool textPool) {
        this.distributions = requireNonNull(distributions, "distributions is null");
        this.textPool = requireNonNull(textPool, "textPool is null");
    }

    @Override
    public Iterator<List<Object>> iterator() {
        return new NationGeneratorIterator(distributions.getNations(), textPool);
    }

    private static class NationGeneratorIterator
            implements Iterator<List<Object>> {
        private final Distribution nations;
        private final TPCHRandomText commentRandom;

        private int index;

        private NationGeneratorIterator(Distribution nations, TextPool textPool) {
            this.nations = nations;
            this.commentRandom = new TPCHRandomText(606179079L, textPool, COMMENT_AVERAGE_LENGTH);
        }

        @Override
        public boolean hasNext() {
            return index < nations.size();
        }

        @Override
        public List<Object> next() {
            List<Object> nation = new ArrayList<>();
            nation.add((long) index);
            nation.add(nations.getValue(index));
            nation.add((long) nations.getWeight(index));
            nation.add(commentRandom.nextValue());

            commentRandom.rowFinished();
            index++;

            return nation;
        }
    }
}
