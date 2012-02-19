/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
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
