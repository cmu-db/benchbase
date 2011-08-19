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
package com.oltpbenchmark.wikipedia;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.oltpbenchmark.QueueLimitException;
import com.oltpbenchmark.ThreadBench;


public class WikipediaBenchmark {

	public static void main(String[] arguments) throws QueueLimitException,
			SQLException, IOException {
		String driver = arguments[0];
		String database = arguments[1];
		String username = arguments[2];
		String password = arguments[3];
		String tracefile = arguments[4];
		Random rand = new Random();
		final String BASE_IP = "10.1."; // fake ip address for each worker

		int WARMUP_SEC = Integer.parseInt(arguments[6]);
		int MEASURE_SEC = Integer.parseInt(arguments[7]);
		int RATE = 0;
		if (arguments.length == 9)
			RATE = Integer.parseInt(arguments[8]); // if <=0 means infinite

		try {
			Class.forName(driver);
		} catch (Exception ex) {
			System.err.println("Unable to load the database driver!");
			System.exit(-1);
		}

		TransactionSelector transSel = new TransactionSelector(tracefile);
		List<Transaction> trace = Collections.unmodifiableList(transSel
				.readAll());
		transSel.close();

		int threads = Integer.parseInt(arguments[5]);
		ArrayList<WikipediaWorker> workers = new ArrayList<WikipediaWorker>();
		for (int i = 0; i < threads; ++i) {
			Connection conn = DriverManager.getConnection(database, username,
					password);
			conn.setAutoCommit(false);
			TransactionGenerator generator = new TraceTransactionGenerator(
					trace);
			workers.add(new WikipediaWorker(conn, generator, BASE_IP
					+ (i % 256) + "." + rand.nextInt(256)));
		}
		ThreadBench.Results r = null;
		if (RATE <= 0) {
			r = ThreadBench.runBenchmark(workers, WARMUP_SEC, MEASURE_SEC);
			System.out.println("Unlimited rate with " + threads + " threads: "
					+ r);
			int maxRate = (int) (r.getRequestsPerSecond() + 0.5);
			System.out.println("MaxRate:" + maxRate);

		} else {
			r = ThreadBench.runRateLimitedBenchmark(workers, WARMUP_SEC,
					MEASURE_SEC, RATE);
			System.out.println("Limiting rate at: " + RATE + " with " + threads
					+ " threads:" + r);
			int maxRate = (int) (r.getRequestsPerSecond() + 0.5);
			System.out.println("Rate:" + maxRate);
		}

		// RATE LIMITED EXAMPLE
		// for (int rate = 5; rate < maxRate; rate += 5) {
		// r = ThreadBench.runRateLimitedBenchmark(workers, WARMUP_SEC,
		// MEASURE_SEC, rate);
		// System.out.println("rate: " + rate + " " + r);
		// }
	}
}
