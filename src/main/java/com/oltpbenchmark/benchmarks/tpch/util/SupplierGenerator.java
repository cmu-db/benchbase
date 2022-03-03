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
import com.oltpbenchmark.util.RowRandomInt;

import static com.oltpbenchmark.benchmarks.tpch.util.GenerateUtils.calculateRowCount;
import static com.oltpbenchmark.benchmarks.tpch.util.GenerateUtils.calculateStartIndex;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SupplierGenerator
        implements Iterable<List<Object>> {
    public static final int SCALE_BASE = 10_000;

    private static final int ACCOUNT_BALANCE_MIN = -99999;
    private static final int ACCOUNT_BALANCE_MAX = 999999;
    private static final int ADDRESS_AVERAGE_LENGTH = 25;
    private static final int COMMENT_AVERAGE_LENGTH = 63;

    public static final String BBB_BASE_TEXT = "Customer ";
    public static final String BBB_COMPLAINT_TEXT = "Complaints";
    public static final String BBB_RECOMMEND_TEXT = "Recommends";
    public static final int BBB_COMMENT_LENGTH = BBB_BASE_TEXT.length() + BBB_COMPLAINT_TEXT.length();
    public static final int BBB_COMMENTS_PER_SCALE_BASE = 10;
    public static final int BBB_COMPLAINT_PERCENT = 50;

    private final double scaleFactor;
    private final int part;
    private final int partCount;

    private final Distributions distributions;
    private final TextPool textPool;

    public SupplierGenerator(double scaleFactor, int part, int partCount) {
        this(scaleFactor, part, partCount, Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool());
    }

    public SupplierGenerator(double scaleFactor, int part, int partCount, Distributions distributions,
            TextPool textPool) {
        this.scaleFactor = scaleFactor;
        this.part = part;
        this.partCount = partCount;

        this.distributions = requireNonNull(distributions, "distributions is null");
        this.textPool = requireNonNull(textPool, "textPool is null");
    }

    @Override
    public Iterator<List<Object>> iterator() {
        return new SupplierGeneratorIterator(
                distributions,
                textPool,
                calculateStartIndex(SCALE_BASE, scaleFactor, part, partCount),
                calculateRowCount(SCALE_BASE, scaleFactor, part, partCount));
    }

    private static class SupplierGeneratorIterator
            implements Iterator<List<Object>> {
        private final TPCHRandomAlphaNumeric addressRandom = new TPCHRandomAlphaNumeric(706178559L,
                ADDRESS_AVERAGE_LENGTH);
        private final RowRandomBoundedInt nationKeyRandom;
        private final TPCHRandomPhoneNumber phoneRandom = new TPCHRandomPhoneNumber(884434366L);
        private final RowRandomBoundedInt accountBalanceRandom = new RowRandomBoundedInt(962338209L,
                ACCOUNT_BALANCE_MIN, ACCOUNT_BALANCE_MAX);
        private final TPCHRandomText commentRandom;
        private final RowRandomBoundedInt bbbCommentRandom = new RowRandomBoundedInt(202794285L, 1, SCALE_BASE);
        private final RowRandomInt bbbJunkRandom = new RowRandomInt(263032577L, 1);
        private final RowRandomInt bbbOffsetRandom = new RowRandomInt(715851524L, 1);
        private final RowRandomBoundedInt bbbTypeRandom = new RowRandomBoundedInt(753643799L, 0, 100);

        private final long startIndex;
        private final long rowCount;

        private long index;

        private SupplierGeneratorIterator(Distributions distributions, TextPool textPool, long startIndex,
                long rowCount) {
            this.startIndex = startIndex;
            this.rowCount = rowCount;

            nationKeyRandom = new RowRandomBoundedInt(110356601L, 0, distributions.getNations().size() - 1);
            commentRandom = new TPCHRandomText(1341315363L, textPool, COMMENT_AVERAGE_LENGTH);

            addressRandom.advanceRows(startIndex);
            nationKeyRandom.advanceRows(startIndex);
            phoneRandom.advanceRows(startIndex);
            accountBalanceRandom.advanceRows(startIndex);
            commentRandom.advanceRows(startIndex);
            bbbCommentRandom.advanceRows(startIndex);
            bbbJunkRandom.advanceRows(startIndex);
            bbbOffsetRandom.advanceRows(startIndex);
            bbbTypeRandom.advanceRows(startIndex);
        }

        @Override
        public boolean hasNext() {
            return index < rowCount;
        }

        @Override
        public List<Object> next() {
            List<Object> supplier = makeSupplier(startIndex + index + 1);

            addressRandom.rowFinished();
            nationKeyRandom.rowFinished();
            phoneRandom.rowFinished();
            accountBalanceRandom.rowFinished();
            commentRandom.rowFinished();
            bbbCommentRandom.rowFinished();
            bbbJunkRandom.rowFinished();
            bbbOffsetRandom.rowFinished();
            bbbTypeRandom.rowFinished();

            index++;

            return supplier;
        }

        private List<Object> makeSupplier(long supplierKey) {
            String comment = commentRandom.nextValue();

            // Add supplier complaints or commendation to the comment
            int bbbCommentRandomValue = bbbCommentRandom.nextValue();
            if (bbbCommentRandomValue <= BBB_COMMENTS_PER_SCALE_BASE) {
                StringBuilder buffer = new StringBuilder(comment);

                // select random place for BBB comment
                int noise = bbbJunkRandom.nextInt(0, (comment.length() - BBB_COMMENT_LENGTH));
                int offset = bbbOffsetRandom.nextInt(0, (comment.length() - (BBB_COMMENT_LENGTH + noise)));

                // select complaint or recommendation
                String type;
                if (bbbTypeRandom.nextValue() < BBB_COMPLAINT_PERCENT) {
                    type = BBB_COMPLAINT_TEXT;
                } else {
                    type = BBB_RECOMMEND_TEXT;
                }

                // write base text (e.g., "Customer ")
                buffer.replace(offset, offset + BBB_BASE_TEXT.length(), BBB_BASE_TEXT);

                // write complaint or commendation text (e.g., "Complaints" or "Recommends")
                buffer.replace(
                        BBB_BASE_TEXT.length() + offset + noise,
                        BBB_BASE_TEXT.length() + offset + noise + type.length(),
                        type);

                comment = buffer.toString();
            }

            long nationKey = nationKeyRandom.nextValue();

            List<Object> supplier = new ArrayList<Object>();
            supplier.add(supplierKey);
            supplier.add(String.format(ENGLISH, "Supplier#%09d", supplierKey));
            supplier.add(addressRandom.nextValue());
            supplier.add(nationKey);
            supplier.add(phoneRandom.nextValue(nationKey));
            supplier.add((double) accountBalanceRandom.nextValue() / 100.);
            supplier.add(comment);

            return supplier;
        }
    }
}
