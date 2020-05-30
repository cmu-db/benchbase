/*
 * Copyright 2020 by OLTPBenchmark Project
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
 *
 */


package com.oltpbenchmark.benchmarks.twitter.util;

import com.oltpbenchmark.api.TransactionGenerator;
import com.oltpbenchmark.distributions.CounterGenerator;

import java.util.List;

public class TraceTransactionGenerator implements TransactionGenerator<TwitterOperation> {
    private static CounterGenerator nextInTrace;
    private final List<TwitterOperation> transactions;

    /**
     * @param transactions a list of transactions shared between threads.
     */
    public TraceTransactionGenerator(List<TwitterOperation> transactions) {
        this.transactions = transactions;
        nextInTrace = new CounterGenerator(transactions.size());
    }

    @Override
    public TwitterOperation nextTransaction() {
        try {
            return transactions.get(nextInTrace.nextInt());
        } catch (IndexOutOfBoundsException id) {
            nextInTrace.reset();
            return transactions.get(nextInTrace.nextInt());
        }
    }
}
