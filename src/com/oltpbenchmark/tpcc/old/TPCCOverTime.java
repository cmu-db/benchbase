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
package com.oltpbenchmark.tpcc.old;

import java.io.IOException;
import java.util.ArrayList;

import com.oltpbenchmark.QueueLimitException;
import com.oltpbenchmark.ThreadBench;
import com.oltpbenchmark.Worker;
import com.oltpbenchmark.tpcc.TPCCRateLimited;


public class TPCCOverTime {

	public static void main(String[] args) throws QueueLimitException,
			IOException {
		if (args.length < 5) {
			System.err
					.println("Usage: TPCCOverTime <measurement second> <new_order(%)> <payment(%)> <order_status(%)> <delivery(%)> [rate limit] [warmup time in sec]");
			System.exit(1);
		}

		final int measurementSeconds = Integer.parseInt(args[0]);
		final int new_order = Integer.parseInt(args[1]);
		final int payment = Integer.parseInt(args[2]);
		final int order_status = Integer.parseInt(args[3]);
		final int delivery = Integer.parseInt(args[4]);
		if (new_order < 0 || payment < 0 || order_status < 0 || delivery < 0
				|| new_order > 100 || payment > 100 || order_status > 100
				|| delivery > 100
				|| new_order + payment + order_status + delivery > 100) {
			System.err
					.println("Error: the workload parameters must be between 0 and 100, inclusive and their sum  should also be no larger than 100.");
			System.exit(1);
		}
		com.oltpbenchmark.tpcc.jTPCCConfig.defaultPaymentWeight = Integer
				.toString(payment);
		com.oltpbenchmark.tpcc.jTPCCConfig.defaultOrderStatusWeight = Integer
				.toString(order_status);
		com.oltpbenchmark.tpcc.jTPCCConfig.defaultDeliveryWeight = Integer
				.toString(delivery);
		com.oltpbenchmark.tpcc.jTPCCConfig.defaultStockLevelWeight = Integer
				.toString(100 - new_order - payment - order_status - delivery);

		int WARMUP_SECONDS = 1200; // this is the default... it might change
									// below
		int rateLimit = 2000;

		if (args.length >= 6)
			rateLimit = Integer.parseInt(args[5]);

		if (args.length >= 7)
			WARMUP_SECONDS = Integer.parseInt(args[6]);

		ArrayList<Worker> workers = new TPCCRateLimited().makeWorkers(true,null);

		ThreadBench.Results r = ThreadBench.runRateLimitedBenchmark(workers,
				WARMUP_SECONDS, measurementSeconds, rateLimit);

		System.out.println("Rate limited reqs/s: " + r);

		r.writeAllCSVAbsoluteTiming(System.out);
	}
}
