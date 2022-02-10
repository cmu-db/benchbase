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

import com.oltpbenchmark.util.RowRandomBoundedInt;

import static com.oltpbenchmark.benchmarks.tpch.util.GenerateUtils.calculateRowCount;
import static com.oltpbenchmark.benchmarks.tpch.util.GenerateUtils.calculateStartIndex;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PartGenerator
        implements Iterable<List<Object>> {
    public static final int SCALE_BASE = 200_000;

    private static final int NAME_WORDS = 5;
    private static final int MANUFACTURER_MIN = 1;
    private static final int MANUFACTURER_MAX = 5;
    private static final int BRAND_MIN = 1;
    private static final int BRAND_MAX = 5;
    private static final int SIZE_MIN = 1;
    private static final int SIZE_MAX = 50;
    private static final int COMMENT_AVERAGE_LENGTH = 14;

    private final double scaleFactor;
    private final int part;
    private final int partCount;

    private final Distributions distributions;
    private final TextPool textPool;

    public PartGenerator(double scaleFactor, int part, int partCount) {
        this(scaleFactor, part, partCount, Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool());
    }

    public PartGenerator(double scaleFactor, int part, int partCount, Distributions distributions, TextPool textPool) {
        this.scaleFactor = scaleFactor;
        this.part = part;
        this.partCount = partCount;

        this.distributions = requireNonNull(distributions, "distributions is null");
        this.textPool = requireNonNull(textPool, "textPool is null");
    }

    @Override
    public Iterator<List<Object>> iterator() {
        return new PartGeneratorIterator(
                distributions,
                textPool,
                calculateStartIndex(SCALE_BASE, scaleFactor, part, partCount),
                calculateRowCount(SCALE_BASE, scaleFactor, part, partCount));
    }

    private static class PartGeneratorIterator
            implements Iterator<List<Object>> {
        private final TPCHRandomStringSequence nameRandom;
        private final RowRandomBoundedInt manufacturerRandom;
        private final RowRandomBoundedInt brandRandom;
        private final TPCHRandomString typeRandom;
        private final RowRandomBoundedInt sizeRandom;
        private final TPCHRandomString containerRandom;
        private final TPCHRandomText commentRandom;

        private final long startIndex;
        private final long rowCount;

        private long index;

        private PartGeneratorIterator(Distributions distributions, TextPool textPool, long startIndex, long rowCount) {
            this.startIndex = startIndex;
            this.rowCount = rowCount;

            nameRandom = new TPCHRandomStringSequence(709314158L, NAME_WORDS, distributions.getPartColors());
            manufacturerRandom = new RowRandomBoundedInt(1L, MANUFACTURER_MIN, MANUFACTURER_MAX);
            brandRandom = new RowRandomBoundedInt(46831694L, BRAND_MIN, BRAND_MAX);
            typeRandom = new TPCHRandomString(1841581359L, distributions.getPartTypes());
            sizeRandom = new RowRandomBoundedInt(1193163244L, SIZE_MIN, SIZE_MAX);
            containerRandom = new TPCHRandomString(727633698L, distributions.getPartContainers());
            commentRandom = new TPCHRandomText(804159733L, textPool, COMMENT_AVERAGE_LENGTH);

            nameRandom.advanceRows(startIndex);
            manufacturerRandom.advanceRows(startIndex);
            brandRandom.advanceRows(startIndex);
            typeRandom.advanceRows(startIndex);
            sizeRandom.advanceRows(startIndex);
            containerRandom.advanceRows(startIndex);
            commentRandom.advanceRows(startIndex);
        }

        @Override
        public boolean hasNext() {
            return index < rowCount;
        }

        @Override
        public List<Object> next() {
            List<Object> part = makePart(startIndex + index + 1);

            nameRandom.rowFinished();
            manufacturerRandom.rowFinished();
            brandRandom.rowFinished();
            typeRandom.rowFinished();
            sizeRandom.rowFinished();
            containerRandom.rowFinished();
            commentRandom.rowFinished();

            index++;

            return part;
        }

        private List<Object> makePart(long partKey) {
            String name = nameRandom.nextValue();

            int manufacturer = manufacturerRandom.nextValue();
            int brand = manufacturer * 10 + brandRandom.nextValue();

            List<Object> part = new ArrayList<Object>();
            part.add(partKey);
            part.add(name);
            part.add(String.format(ENGLISH, "Manufacturer#%d", manufacturer));
            part.add(String.format(ENGLISH, "Brand#%d", brand));
            part.add(typeRandom.nextValue());
            part.add((long) sizeRandom.nextValue());
            part.add(containerRandom.nextValue());
            part.add((double) calculatePartPrice(partKey) / 100.);
            part.add(commentRandom.nextValue());

            return part;
        }
    }

    static long calculatePartPrice(long p) {
        long price = 90000;

        // limit contribution to $200
        price += (p / 10) % 20001;
        price += (p % 1000) * 100;

        return (price);
    }
}
