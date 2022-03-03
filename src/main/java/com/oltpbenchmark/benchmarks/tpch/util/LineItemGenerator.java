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
import com.oltpbenchmark.util.RowRandomBoundedLong;

import static com.oltpbenchmark.benchmarks.tpch.util.GenerateUtils.calculateRowCount;
import static com.oltpbenchmark.benchmarks.tpch.util.GenerateUtils.calculateStartIndex;
import static com.oltpbenchmark.benchmarks.tpch.util.GenerateUtils.toEpochDate;
import static com.oltpbenchmark.benchmarks.tpch.util.OrderGenerator.LINE_COUNT_MAX;
import static com.oltpbenchmark.benchmarks.tpch.util.OrderGenerator.createLineCountRandom;
import static com.oltpbenchmark.benchmarks.tpch.util.OrderGenerator.createOrderDateRandom;
import static com.oltpbenchmark.benchmarks.tpch.util.OrderGenerator.makeOrderKey;
import static com.oltpbenchmark.benchmarks.tpch.util.PartSupplierGenerator.selectPartSupplier;
import static java.util.Objects.requireNonNull;

public class LineItemGenerator
        implements Iterable<List<Object>> {
    private static final int QUANTITY_MIN = 1;
    private static final int QUANTITY_MAX = 50;
    private static final int TAX_MIN = 0;
    private static final int TAX_MAX = 8;
    private static final int DISCOUNT_MIN = 0;
    private static final int DISCOUNT_MAX = 10;
    private static final int PART_KEY_MIN = 1;

    private static final int SHIP_DATE_MIN = 1;
    private static final int SHIP_DATE_MAX = 121;
    private static final int COMMIT_DATE_MIN = 30;
    private static final int COMMIT_DATE_MAX = 90;
    private static final int RECEIPT_DATE_MIN = 1;
    private static final int RECEIPT_DATE_MAX = 30;

    static final int ITEM_SHIP_DAYS = SHIP_DATE_MAX + RECEIPT_DATE_MAX;

    private static final int COMMENT_AVERAGE_LENGTH = 27;

    private final double scaleFactor;
    private final int part;
    private final int partCount;

    private final Distributions distributions;
    private final TextPool textPool;

    public LineItemGenerator(double scaleFactor, int part, int partCount) {
        this(scaleFactor, part, partCount, Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool());
    }

    public LineItemGenerator(double scaleFactor, int part, int partCount, Distributions distributions,
            TextPool textPool) {
        this.scaleFactor = scaleFactor;
        this.part = part;
        this.partCount = partCount;

        this.distributions = requireNonNull(distributions, "distributions is null");
        this.textPool = requireNonNull(textPool, "textPool is null");
    }

    @Override
    public Iterator<List<Object>> iterator() {
        return new LineItemGeneratorIterator(
                distributions,
                textPool,
                scaleFactor,
                calculateStartIndex(OrderGenerator.SCALE_BASE, scaleFactor, part, partCount),
                calculateRowCount(OrderGenerator.SCALE_BASE, scaleFactor, part, partCount));
    }

    private static class LineItemGeneratorIterator
            implements Iterator<List<Object>> {
        private final RowRandomBoundedInt orderDateRandom = createOrderDateRandom();
        private final RowRandomBoundedInt lineCountRandom = createLineCountRandom();

        private final RowRandomBoundedInt quantityRandom = createQuantityRandom();
        private final RowRandomBoundedInt discountRandom = createDiscountRandom();
        private final RowRandomBoundedInt taxRandom = createTaxRandom();

        private final RowRandomBoundedLong linePartKeyRandom;

        private final RowRandomBoundedInt supplierNumberRandom = new RowRandomBoundedInt(2095021727L, 0, 3,
                LINE_COUNT_MAX);

        private final RowRandomBoundedInt shipDateRandom = createShipDateRandom();
        private final RowRandomBoundedInt commitDateRandom = new RowRandomBoundedInt(904914315L, COMMIT_DATE_MIN,
                COMMIT_DATE_MAX, LINE_COUNT_MAX);
        private final RowRandomBoundedInt receiptDateRandom = new RowRandomBoundedInt(373135028L, RECEIPT_DATE_MIN,
                RECEIPT_DATE_MAX, LINE_COUNT_MAX);

        private final TPCHRandomString returnedFlagRandom;
        private final TPCHRandomString shipInstructionsRandom;
        private final TPCHRandomString shipModeRandom;

        private final TPCHRandomText commentRandom;

        private final double scaleFactor;
        private final long startIndex;

        private final long rowCount;

        private long index;
        private int orderDate;
        private int lineCount;
        private int lineNumber;

        private LineItemGeneratorIterator(Distributions distributions, TextPool textPool, double scaleFactor,
                long startIndex, long rowCount) {
            this.scaleFactor = scaleFactor;
            this.startIndex = startIndex;
            this.rowCount = rowCount;

            returnedFlagRandom = new TPCHRandomString(717419739L, distributions.getReturnFlags(), LINE_COUNT_MAX);
            shipInstructionsRandom = new TPCHRandomString(1371272478L, distributions.getShipInstructions(),
                    LINE_COUNT_MAX);
            shipModeRandom = new TPCHRandomString(675466456L, distributions.getShipModes(), LINE_COUNT_MAX);
            commentRandom = new TPCHRandomText(1095462486L, textPool, COMMENT_AVERAGE_LENGTH, LINE_COUNT_MAX);

            linePartKeyRandom = createPartKeyRandom(scaleFactor);

            orderDateRandom.advanceRows(startIndex);
            lineCountRandom.advanceRows(startIndex);

            quantityRandom.advanceRows(startIndex);
            discountRandom.advanceRows(startIndex);
            taxRandom.advanceRows(startIndex);

            linePartKeyRandom.advanceRows(startIndex);

            supplierNumberRandom.advanceRows(startIndex);

            shipDateRandom.advanceRows(startIndex);
            commitDateRandom.advanceRows(startIndex);
            receiptDateRandom.advanceRows(startIndex);

            returnedFlagRandom.advanceRows(startIndex);
            shipInstructionsRandom.advanceRows(startIndex);
            shipModeRandom.advanceRows(startIndex);

            commentRandom.advanceRows(startIndex);

            // generate information for initial order
            orderDate = orderDateRandom.nextValue();
            lineCount = lineCountRandom.nextValue() - 1;
        }

        @Override
        public boolean hasNext() {
            return index < rowCount;
        }

        @Override
        public List<Object> next() {
            List<Object> lineitem = makeLineitem(startIndex + index + 1);
            lineNumber++;

            // advance next row only when all lines for the order have been produced
            if (lineNumber > lineCount) {
                orderDateRandom.rowFinished();

                lineCountRandom.rowFinished();
                quantityRandom.rowFinished();
                discountRandom.rowFinished();
                taxRandom.rowFinished();

                linePartKeyRandom.rowFinished();

                supplierNumberRandom.rowFinished();

                shipDateRandom.rowFinished();
                commitDateRandom.rowFinished();
                receiptDateRandom.rowFinished();

                returnedFlagRandom.rowFinished();
                shipInstructionsRandom.rowFinished();
                shipModeRandom.rowFinished();

                commentRandom.rowFinished();

                index++;

                // generate information for next order
                lineCount = lineCountRandom.nextValue() - 1;
                orderDate = orderDateRandom.nextValue();
                lineNumber = 0;
            }

            return lineitem;
        }

        private List<Object> makeLineitem(long orderIndex) {
            long orderKey = makeOrderKey(orderIndex);

            int quantity = quantityRandom.nextValue();
            int discount = discountRandom.nextValue();
            int tax = taxRandom.nextValue();

            long partKey = linePartKeyRandom.nextValue();

            int supplierNumber = supplierNumberRandom.nextValue();
            long supplierKey = selectPartSupplier(partKey, supplierNumber, scaleFactor);

            long partPrice = PartGenerator.calculatePartPrice(partKey);
            long extendedPrice = partPrice * quantity;

            int shipDate = shipDateRandom.nextValue();
            shipDate += orderDate;
            int commitDate = commitDateRandom.nextValue();
            commitDate += orderDate;
            int receiptDate = receiptDateRandom.nextValue();
            receiptDate += shipDate;

            String returnedFlag;
            if (GenerateUtils.isInPast(receiptDate)) {
                returnedFlag = returnedFlagRandom.nextValue();
            } else {
                returnedFlag = "N";
            }

            String status;
            if (GenerateUtils.isInPast(shipDate)) {
                status = "F";
            } else {
                status = "O";
            }

            String shipInstructions = shipInstructionsRandom.nextValue();
            String shipMode = shipModeRandom.nextValue();
            String comment = commentRandom.nextValue();

            List<Object> lineItem = new ArrayList<>();
            lineItem.add(orderKey);
            lineItem.add(partKey);
            lineItem.add(supplierKey);
            lineItem.add((long) (lineNumber + 1));
            lineItem.add((double) quantity);
            lineItem.add((double) extendedPrice / 100.);
            lineItem.add((double) discount / 100.);
            lineItem.add((double) tax / 100.);
            lineItem.add(returnedFlag);
            lineItem.add(status);
            lineItem.add(toEpochDate(shipDate));
            lineItem.add(toEpochDate(commitDate));
            lineItem.add(toEpochDate(receiptDate));
            lineItem.add(shipInstructions);
            lineItem.add(shipMode);
            lineItem.add(comment);

            return lineItem;
        }
    }

    static RowRandomBoundedInt createQuantityRandom() {
        return new RowRandomBoundedInt(209208115L, QUANTITY_MIN, QUANTITY_MAX, LINE_COUNT_MAX);
    }

    static RowRandomBoundedInt createDiscountRandom() {
        return new RowRandomBoundedInt(554590007L, DISCOUNT_MIN, DISCOUNT_MAX, LINE_COUNT_MAX);
    }

    static RowRandomBoundedInt createTaxRandom() {
        return new RowRandomBoundedInt(721958466L, TAX_MIN, TAX_MAX, LINE_COUNT_MAX);
    }

    static RowRandomBoundedLong createPartKeyRandom(double scaleFactor) {
        return new RowRandomBoundedLong(1808217256L, scaleFactor >= 30000, PART_KEY_MIN,
                (long) (PartGenerator.SCALE_BASE * scaleFactor), LINE_COUNT_MAX);
    }

    static RowRandomBoundedInt createShipDateRandom() {
        return new RowRandomBoundedInt(1769349045L, SHIP_DATE_MIN, SHIP_DATE_MAX, LINE_COUNT_MAX);
    }
}
