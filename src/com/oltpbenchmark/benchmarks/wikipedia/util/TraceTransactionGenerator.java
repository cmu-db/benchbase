/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.benchmarks.wikipedia.util;

import java.util.List;
import java.util.Random;

import com.oltpbenchmark.api.TransactionGenerator;

public class TraceTransactionGenerator implements TransactionGenerator<WikipediaOperation> {
	private final Random rng = new Random();
	private final List<WikipediaOperation> transactions;

	/**
	 * @param transactions
	 *            a list of transactions shared between threads.
	 */
	public TraceTransactionGenerator(List<WikipediaOperation> transactions) {
		this.transactions = transactions;
	}

	@Override
	public WikipediaOperation nextTransaction() {
		int transactionIndex = rng.nextInt(transactions.size());
		return transactions.get(transactionIndex);
	}
}
