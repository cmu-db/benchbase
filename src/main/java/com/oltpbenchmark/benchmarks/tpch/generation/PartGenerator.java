/*
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
package com.oltpbenchmark.benchmarks.tpch.generation;

import com.google.common.collect.AbstractIterator;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import static com.google.common.base.Preconditions.checkArgument;
import static com.oltpbenchmark.benchmarks.tpch.generation.GenerateUtils.calculateRowCount;
import static com.oltpbenchmark.benchmarks.tpch.generation.GenerateUtils.calculateStartIndex;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PartGenerator
        implements Iterable<List<Object>>
{
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

    public PartGenerator(double scaleFactor, int part, int partCount)
    {
        this(scaleFactor, part, partCount, Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool());
    }

    public PartGenerator(double scaleFactor, int part, int partCount, Distributions distributions, TextPool textPool)
    {
        checkArgument(scaleFactor > 0, "scaleFactor must be greater than 0");
        checkArgument(part >= 1, "part must be at least 1");
        checkArgument(part <= partCount, "part must be less than or equal to part count");

        this.scaleFactor = scaleFactor;
        this.part = part;
        this.partCount = partCount;

        this.distributions = requireNonNull(distributions, "distributions is null");
        this.textPool = requireNonNull(textPool, "textPool is null");
    }

    @Override
    public Iterator<List<Object>> iterator()
    {
        return new PartGeneratorIterator(
                distributions,
                textPool,
                calculateStartIndex(SCALE_BASE, scaleFactor, part, partCount),
                calculateRowCount(SCALE_BASE, scaleFactor, part, partCount));
    }

    private static class PartGeneratorIterator
            extends AbstractIterator<List<Object>>
    {
        private final RandomStringSequence nameRandom;
        private final RandomBoundedInt manufacturerRandom;
        private final RandomBoundedInt brandRandom;
        private final RandomString typeRandom;
        private final RandomBoundedInt sizeRandom;
        private final RandomString containerRandom;
        private final RandomText commentRandom;

        private final long startIndex;
        private final long rowCount;

        private long index;

        private PartGeneratorIterator(Distributions distributions, TextPool textPool, long startIndex, long rowCount)
        {
            this.startIndex = startIndex;
            this.rowCount = rowCount;

            nameRandom = new RandomStringSequence(709314158, NAME_WORDS, distributions.getPartColors());
            manufacturerRandom = new RandomBoundedInt(1, MANUFACTURER_MIN, MANUFACTURER_MAX);
            brandRandom = new RandomBoundedInt(46831694, BRAND_MIN, BRAND_MAX);
            typeRandom = new RandomString(1841581359, distributions.getPartTypes());
            sizeRandom = new RandomBoundedInt(1193163244, SIZE_MIN, SIZE_MAX);
            containerRandom = new RandomString(727633698, distributions.getPartContainers());
            commentRandom = new RandomText(804159733, textPool, COMMENT_AVERAGE_LENGTH);

            nameRandom.advanceRows(startIndex);
            manufacturerRandom.advanceRows(startIndex);
            brandRandom.advanceRows(startIndex);
            typeRandom.advanceRows(startIndex);
            sizeRandom.advanceRows(startIndex);
            containerRandom.advanceRows(startIndex);
            commentRandom.advanceRows(startIndex);
        }

        @Override
        protected List<Object> computeNext()
        {
            if (index >= rowCount) {
                return endOfData();
            }

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

        private List<Object> makePart(long partKey)
        {
            String name = nameRandom.nextValue();

            int manufacturer = manufacturerRandom.nextValue();
            int brand = manufacturer * 10 + brandRandom.nextValue();

            List<Object> part = new ArrayList<Object>();
            part.add(partKey);
            part.add(name);
            part.add(String.format(ENGLISH, "Manufacturer#%d", manufacturer));
            part.add(String.format(ENGLISH, "Brand#%d", brand));
            part.add(typeRandom.nextValue());
            part.add((long)sizeRandom.nextValue());
            part.add(containerRandom.nextValue());
            part.add((double)calculatePartPrice(partKey) / 100.);
            part.add(commentRandom.nextValue());

            return part;
        }
    }

    static long calculatePartPrice(long p)
    {
        long price = 90000;

        // limit contribution to $200
        price += (p / 10) % 20001;
        price += (p % 1000) * 100;

        return (price);
    }
}
