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

public class CustomerGenerator
        implements Iterable<List<Object>>
{
    public static final int SCALE_BASE = 150_000;
    private static final int ACCOUNT_BALANCE_MIN = -99999;
    private static final int ACCOUNT_BALANCE_MAX = 999999;
    private static final int ADDRESS_AVERAGE_LENGTH = 25;
    private static final int COMMENT_AVERAGE_LENGTH = 73;

    private final double scaleFactor;
    private final int part;
    private final int partCount;

    private final Distributions distributions;
    private final TextPool textPool;

    public CustomerGenerator(double scaleFactor, int part, int partCount)
    {
        this(scaleFactor, part, partCount, Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool());
    }

    public CustomerGenerator(double scaleFactor, int part, int partCount, Distributions distributions, TextPool textPool)
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
        return new CustomerGeneratorIterator(
                distributions,
                textPool,
                calculateStartIndex(SCALE_BASE, scaleFactor, part, partCount),
                calculateRowCount(SCALE_BASE, scaleFactor, part, partCount));
    }

    private static class CustomerGeneratorIterator
            extends AbstractIterator<List<Object>>
    {
        private final RandomAlphaNumeric addressRandom = new RandomAlphaNumeric(881155353, ADDRESS_AVERAGE_LENGTH);
        private final RandomBoundedInt nationKeyRandom;
        private final RandomPhoneNumber phoneRandom = new RandomPhoneNumber(1521138112);
        private final RandomBoundedInt accountBalanceRandom = new RandomBoundedInt(298370230, ACCOUNT_BALANCE_MIN, ACCOUNT_BALANCE_MAX);
        private final RandomString marketSegmentRandom;
        private final RandomText commentRandom;

        private final long startIndex;
        private final long rowCount;

        private long index;

        private CustomerGeneratorIterator(Distributions distributions, TextPool textPool, long startIndex, long rowCount)
        {
            this.startIndex = startIndex;
            this.rowCount = rowCount;

            nationKeyRandom = new RandomBoundedInt(1489529863, 0, distributions.getNations().size() - 1);
            marketSegmentRandom = new RandomString(1140279430, distributions.getMarketSegments());
            commentRandom = new RandomText(1335826707, textPool, COMMENT_AVERAGE_LENGTH);

            addressRandom.advanceRows(startIndex);
            nationKeyRandom.advanceRows(startIndex);
            phoneRandom.advanceRows(startIndex);
            accountBalanceRandom.advanceRows(startIndex);
            marketSegmentRandom.advanceRows(startIndex);
            commentRandom.advanceRows(startIndex);
        }

        @Override
        protected List<Object> computeNext()
        {
            if (index >= rowCount) {
                return endOfData();
            }

            List<Object> customer = makeCustomer(startIndex + index + 1);

            addressRandom.rowFinished();
            nationKeyRandom.rowFinished();
            phoneRandom.rowFinished();
            accountBalanceRandom.rowFinished();
            marketSegmentRandom.rowFinished();
            commentRandom.rowFinished();

            index++;

            return customer;
        }

        private List<Object> makeCustomer(long customerKey)
        {
            long nationKey = nationKeyRandom.nextValue();

            List<Object> customer = new ArrayList<>();

            customer.add(customerKey);
            customer.add(String.format(ENGLISH, "Customer#%09d", customerKey));
            customer.add(addressRandom.nextValue());
            customer.add(nationKey);
            customer.add(phoneRandom.nextValue(nationKey));
            customer.add((double)accountBalanceRandom.nextValue() / 100.);
            customer.add(marketSegmentRandom.nextValue());
            customer.add(commentRandom.nextValue());

            return customer;
        }
    }
}
