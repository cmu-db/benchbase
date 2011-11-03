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
package com.oltpbenchmark;

import java.util.ArrayList;
import java.util.Random;

import com.oltpbenchmark.Phase;
import com.oltpbenchmark.tpcc.jTPCCConfig;
import com.oltpbenchmark.TransactionType;


public class ThreadBenchExample {
	private static final class RandomSleepWorker extends Worker {
		private final Random rng = new Random();
		private final int maxMs;

		public RandomSleepWorker(int maxMs) {
			this.maxMs = maxMs;
		}

		@Override
		protected TransactionType doWork(boolean measure, Phase phase) {
			int ms = rng.nextInt(maxMs);
			try {
				Thread.sleep(ms);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			return new TransactionType("INVALID",0); // I don't know the type here!
		}
	}

	static private final int WARMUP_S = 1;
	static private final int MEASURE_S = 4;

	public static void main(String[] arguments) throws QueueLimitException {
		if (arguments.length != 1) {
			System.err.println("ThreadBenchExample (num threads)");
			System.exit(1);
		}

		int threads = Integer.parseInt(arguments[0]);
		ArrayList<RandomSleepWorker> workers = new ArrayList<RandomSleepWorker>();
		for (int i = 0; i < threads; ++i) {
			workers.add(new RandomSleepWorker(100));
		}

		ThreadBench.Results r = ThreadBench.runBenchmark(workers, WARMUP_S,
				MEASURE_S);
		System.out.println("Unlimited with " + threads + " threads: " + r);
		int maxRate = (int) (r.getRequestsPerSecond() + 0.5);

		for (int rate = 5; rate < maxRate; rate += 5) {
			r = ThreadBench.runRateLimitedBenchmark(workers, WARMUP_S,
					MEASURE_S, rate);
			System.out.println("rate: " + rate + " " + r);
		}

		int rate = (maxRate / 5) * 5;
		System.out.println("\nCSV output rate = " + rate + "\n");
		r = ThreadBench.runRateLimitedBenchmark(workers, WARMUP_S, 120, rate);
		r.writeCSV(30, System.out);
	}
}
