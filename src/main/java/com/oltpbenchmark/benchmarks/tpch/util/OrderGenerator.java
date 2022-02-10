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

import static com.oltpbenchmark.benchmarks.tpch.util.GenerateUtils.MIN_GENERATE_DATE;
import static com.oltpbenchmark.benchmarks.tpch.util.GenerateUtils.TOTAL_DATE_RANGE;
import static com.oltpbenchmark.benchmarks.tpch.util.GenerateUtils.calculateRowCount;
import static com.oltpbenchmark.benchmarks.tpch.util.GenerateUtils.calculateStartIndex;
import static com.oltpbenchmark.benchmarks.tpch.util.GenerateUtils.toEpochDate;
import static com.oltpbenchmark.benchmarks.tpch.util.LineItemGenerator.ITEM_SHIP_DAYS;
import static com.oltpbenchmark.benchmarks.tpch.util.LineItemGenerator.createDiscountRandom;
import static com.oltpbenchmark.benchmarks.tpch.util.LineItemGenerator.createPartKeyRandom;
import static com.oltpbenchmark.benchmarks.tpch.util.LineItemGenerator.createQuantityRandom;
import static com.oltpbenchmark.benchmarks.tpch.util.LineItemGenerator.createShipDateRandom;
import static com.oltpbenchmark.benchmarks.tpch.util.LineItemGenerator.createTaxRandom;
import static com.oltpbenchmark.benchmarks.tpch.util.PartGenerator.calculatePartPrice;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class OrderGenerator
        implements Iterable<List<Object>> {
    public static final int SCALE_BASE = 1_500_000;

    // portion with have no orders
    public static final int CUSTOMER_MORTALITY = 3;

    private static final int ORDER_DATE_MIN = MIN_GENERATE_DATE;
    private static final int ORDER_DATE_MAX = ORDER_DATE_MIN + (TOTAL_DATE_RANGE - ITEM_SHIP_DAYS - 1);
    private static final int CLERK_SCALE_BASE = 1000;

    private static final int LINE_COUNT_MIN = 1;
    static final int LINE_COUNT_MAX = 7;

    private static final int COMMENT_AVERAGE_LENGTH = 49;

    private static final int ORDER_KEY_SPARSE_BITS = 2;
    private static final int ORDER_KEY_SPARSE_KEEP = 3;

    private final double scaleFactor;
    private final int part;
    private final int partCount;

    private final Distributions distributions;
    private final TextPool textPool;

    public OrderGenerator(double scaleFactor, int part, int partCount) {
        this(scaleFactor, part, partCount, Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool());
    }

    public OrderGenerator(double scaleFactor, int part, int partCount, Distributions distributions, TextPool textPool) {
        this.scaleFactor = scaleFactor;
        this.part = part;
        this.partCount = partCount;

        this.distributions = requireNonNull(distributions, "distributions is null");
        this.textPool = requireNonNull(textPool, "textPool is null");
    }

    @Override
    public Iterator<List<Object>> iterator() {
        return new OrderGeneratorIterator(
                distributions,
                textPool,
                scaleFactor,
                calculateStartIndex(SCALE_BASE, scaleFactor, part, partCount),
                calculateRowCount(SCALE_BASE, scaleFactor, part, partCount));
    }

    private static class OrderGeneratorIterator
            implements Iterator<List<Object>> {
        private final RowRandomBoundedInt orderDateRandom = createOrderDateRandom();
        private final RowRandomBoundedInt lineCountRandom = createLineCountRandom();
        private final RowRandomBoundedLong customerKeyRandom;
        private final TPCHRandomString orderPriorityRandom;
        private final RowRandomBoundedInt clerkRandom;
        private final TPCHRandomText commentRandom;

        private final RowRandomBoundedInt lineQuantityRandom = createQuantityRandom();
        private final RowRandomBoundedInt lineDiscountRandom = createDiscountRandom();
        private final RowRandomBoundedInt lineTaxRandom = createTaxRandom();
        private final RowRandomBoundedLong linePartKeyRandom;
        private final RowRandomBoundedInt lineShipDateRandom = createShipDateRandom();

        private final long startIndex;
        private final long rowCount;

        private final long maxCustomerKey;

        private long index;

        private OrderGeneratorIterator(Distributions distributions, TextPool textPool, double scaleFactor,
                long startIndex, long rowCount) {
            this.startIndex = startIndex;
            this.rowCount = rowCount;

            clerkRandom = new RowRandomBoundedInt(1171034773L, 1,
                    Math.max((int) (scaleFactor * CLERK_SCALE_BASE), CLERK_SCALE_BASE));

            maxCustomerKey = (long) (CustomerGenerator.SCALE_BASE * scaleFactor);
            customerKeyRandom = new RowRandomBoundedLong(851767375L, scaleFactor >= 30000, 1, maxCustomerKey);

            orderPriorityRandom = new TPCHRandomString(591449447L, distributions.getOrderPriorities());
            commentRandom = new TPCHRandomText(276090261L, textPool, COMMENT_AVERAGE_LENGTH);

            linePartKeyRandom = createPartKeyRandom(scaleFactor);

            orderDateRandom.advanceRows(startIndex);
            lineCountRandom.advanceRows(startIndex);
            customerKeyRandom.advanceRows(startIndex);
            orderPriorityRandom.advanceRows(startIndex);
            clerkRandom.advanceRows(startIndex);
            commentRandom.advanceRows(startIndex);

            lineQuantityRandom.advanceRows(startIndex);
            lineDiscountRandom.advanceRows(startIndex);
            lineShipDateRandom.advanceRows(startIndex);
            lineTaxRandom.advanceRows(startIndex);
            linePartKeyRandom.advanceRows(startIndex);
        }

        @Override
        public boolean hasNext() {
            return index < rowCount;
        }

        @Override
        public List<Object> next() {
            List<Object> order = makeOrder(startIndex + index + 1);

            orderDateRandom.rowFinished();
            lineCountRandom.rowFinished();
            customerKeyRandom.rowFinished();
            orderPriorityRandom.rowFinished();
            clerkRandom.rowFinished();
            commentRandom.rowFinished();

            lineQuantityRandom.rowFinished();
            lineDiscountRandom.rowFinished();
            lineShipDateRandom.rowFinished();
            lineTaxRandom.rowFinished();
            linePartKeyRandom.rowFinished();

            index++;

            return order;
        }

        private List<Object> makeOrder(long index) {
            long orderKey = makeOrderKey(index);

            int orderDate = orderDateRandom.nextValue();

            // generate customer key, taking into account customer mortality rate
            long customerKey = customerKeyRandom.nextValue();
            int delta = 1;
            while (customerKey % CUSTOMER_MORTALITY == 0) {
                customerKey += delta;
                customerKey = Math.min(customerKey, maxCustomerKey);
                delta *= -1;
            }

            long totalPrice = 0;
            int shippedCount = 0;

            int lineCount = lineCountRandom.nextValue();
            for (long lineNumber = 0; lineNumber < lineCount; lineNumber++) {
                int quantity = lineQuantityRandom.nextValue();
                int discount = lineDiscountRandom.nextValue();
                int tax = lineTaxRandom.nextValue();

                long partKey = linePartKeyRandom.nextValue();

                long partPrice = calculatePartPrice(partKey);
                long extendedPrice = partPrice * quantity;
                long discountedPrice = extendedPrice * (100 - discount);
                totalPrice += ((discountedPrice / 100) * (100 + tax)) / 100;

                int shipDate = lineShipDateRandom.nextValue();
                shipDate += orderDate;
                if (GenerateUtils.isInPast(shipDate)) {
                    shippedCount++;
                }
            }

            char orderStatus;
            if (shippedCount == lineCount) {
                orderStatus = 'F';
            } else if (shippedCount > 0) {
                orderStatus = 'P';
            } else {
                orderStatus = 'O';
            }

            List<Object> order = new ArrayList<>();
            order.add(orderKey);
            order.add(customerKey);
            order.add(Character.valueOf(orderStatus).toString());
            order.add((double) totalPrice / 100.);
            order.add(toEpochDate(orderDate));
            order.add(orderPriorityRandom.nextValue());
            order.add(String.format(ENGLISH, "Clerk#%09d", clerkRandom.nextValue()));
            order.add(0L);
            order.add(commentRandom.nextValue());

            return order;
        }
    }

    static RowRandomBoundedInt createLineCountRandom() {
        return new RowRandomBoundedInt(1434868289L, LINE_COUNT_MIN, LINE_COUNT_MAX);
    }

    static RowRandomBoundedInt createOrderDateRandom() {
        return new RowRandomBoundedInt(1066728069L, ORDER_DATE_MIN, ORDER_DATE_MAX);
    }

    static long makeOrderKey(long orderIndex) {
        long lowBits = orderIndex & ((1 << ORDER_KEY_SPARSE_KEEP) - 1);

        long ok = orderIndex;
        ok >>= ORDER_KEY_SPARSE_KEEP;
        ok <<= ORDER_KEY_SPARSE_BITS;
        ok <<= ORDER_KEY_SPARSE_KEEP;
        ok += lowBits;

        return ok;
    }
}
