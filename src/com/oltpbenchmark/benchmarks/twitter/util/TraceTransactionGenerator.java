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
package com.oltpbenchmark.benchmarks.twitter.util;

import java.util.List;
import java.util.Random;

import com.oltpbenchmark.api.TransactionGenerator;
import com.oltpbenchmark.distributions.CounterGenerator;

public class TraceTransactionGenerator implements TransactionGenerator<TwitterOperation> {
    private static CounterGenerator nextInTrace;
	private final List<TwitterOperation> transactions;

	/**
	 * @param transactions
	 *            a list of transactions shared between threads.
	 */
	public TraceTransactionGenerator(List<TwitterOperation> transactions) {
		this.transactions = transactions;
		nextInTrace= new CounterGenerator(transactions.size());
	}

	@Override
	public TwitterOperation nextTransaction() {
	    try{
		return transactions.get(nextInTrace.nextInt());
	    }
	    catch(IndexOutOfBoundsException id)
	    {
	        nextInTrace.reset();
	        return transactions.get(nextInTrace.nextInt());
	    }
	}
}
