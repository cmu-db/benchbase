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
package com.oltpbenchmark.benchmarks.templated.util;

import com.oltpbenchmark.api.TransactionGenerator;
import com.oltpbenchmark.distributions.CyclicCounterGenerator;
import java.util.Collections;
import java.util.List;

public class TraceTransactionGenerator implements TransactionGenerator<GenericQueryOperation> {

  private final List<GenericQueryOperation> transactions;
  private final CyclicCounterGenerator nextInTrace;

  /**
   * @param transactions a list of transactions shared between threads.
   */
  public TraceTransactionGenerator(List<GenericQueryOperation> transactions) {
    this.transactions = Collections.unmodifiableList(transactions);
    this.nextInTrace = new CyclicCounterGenerator(transactions.size());
  }

  @Override
  public GenericQueryOperation nextTransaction() {
    return transactions.get(nextInTrace.nextInt());
  }

  public boolean isEmpty() {
    return transactions.size() == 0;
  }
}
