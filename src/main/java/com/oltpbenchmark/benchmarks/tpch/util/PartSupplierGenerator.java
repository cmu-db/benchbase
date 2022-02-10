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
import static java.util.Objects.requireNonNull;

public class PartSupplierGenerator
        implements Iterable<List<Object>> {
    private static final int SUPPLIERS_PER_PART = 4;

    private static final int AVAILABLE_QUANTITY_MIN = 1;
    private static final int AVAILABLE_QUANTITY_MAX = 9999;

    private static final int SUPPLY_COST_MIN = 100;
    private static final int SUPPLY_COST_MAX = 100000;

    private static final int COMMENT_AVERAGE_LENGTH = 124;

    private final double scaleFactor;
    private final int part;
    private final int partCount;

    private final TextPool textPool;

    public PartSupplierGenerator(double scaleFactor, int part, int partCount) {
        this(scaleFactor, part, partCount, TextPool.getDefaultTestPool());
    }

    public PartSupplierGenerator(double scaleFactor, int part, int partCount, TextPool textPool) {
        this.scaleFactor = scaleFactor;
        this.part = part;
        this.partCount = partCount;

        this.textPool = requireNonNull(textPool, "textPool is null");
    }

    @Override
    public Iterator<List<Object>> iterator() {
        return new PartSupplierGeneratorIterator(
                textPool,
                scaleFactor,
                calculateStartIndex(PartGenerator.SCALE_BASE, scaleFactor, part, partCount),
                calculateRowCount(PartGenerator.SCALE_BASE, scaleFactor, part, partCount));
    }

    private static class PartSupplierGeneratorIterator
            implements Iterator<List<Object>> {
        private final double scaleFactor;
        private final long startIndex;
        private final long rowCount;

        private final RowRandomBoundedInt availableQuantityRandom;
        private final RowRandomBoundedInt supplyCostRandom;
        private final TPCHRandomText commentRandom;

        private long index;
        private int partSupplierNumber;

        private PartSupplierGeneratorIterator(TextPool textPool, double scaleFactor, long startIndex, long rowCount) {
            this.scaleFactor = scaleFactor;
            this.startIndex = startIndex;
            this.rowCount = rowCount;

            availableQuantityRandom = new RowRandomBoundedInt(1671059989L, AVAILABLE_QUANTITY_MIN,
                    AVAILABLE_QUANTITY_MAX, SUPPLIERS_PER_PART);
            supplyCostRandom = new RowRandomBoundedInt(1051288424L, SUPPLY_COST_MIN, SUPPLY_COST_MAX,
                    SUPPLIERS_PER_PART);
            commentRandom = new TPCHRandomText(1961692154L, textPool, COMMENT_AVERAGE_LENGTH, SUPPLIERS_PER_PART);

            availableQuantityRandom.advanceRows(startIndex);
            supplyCostRandom.advanceRows(startIndex);
            commentRandom.advanceRows(startIndex);
        }

        @Override
        public boolean hasNext() {
            return index < rowCount;
        }

        @Override
        public List<Object> next() {
            List<Object> partSupplier = makePartSupplier(startIndex + index + 1);
            partSupplierNumber++;

            // advance next row only when all lines for the order have been produced
            if (partSupplierNumber >= SUPPLIERS_PER_PART) {
                availableQuantityRandom.rowFinished();
                supplyCostRandom.rowFinished();
                commentRandom.rowFinished();

                index++;
                partSupplierNumber = 0;
            }

            return partSupplier;
        }

        private List<Object> makePartSupplier(long partKey) {
            List<Object> partSupplier = new ArrayList<>();
            partSupplier.add(partKey);
            partSupplier.add(selectPartSupplier(partKey, partSupplierNumber, scaleFactor));
            partSupplier.add((long) availableQuantityRandom.nextValue());
            partSupplier.add((double) supplyCostRandom.nextValue() / 100.);
            partSupplier.add(commentRandom.nextValue());
            return partSupplier;
        }
    }

    static long selectPartSupplier(long partKey, long supplierNumber, double scaleFactor) {
        long supplierCount = (long) (SupplierGenerator.SCALE_BASE * scaleFactor);
        return ((partKey + (supplierNumber * ((supplierCount / SUPPLIERS_PER_PART) + ((partKey - 1) / supplierCount))))
                % supplierCount) + 1;
    }
}
