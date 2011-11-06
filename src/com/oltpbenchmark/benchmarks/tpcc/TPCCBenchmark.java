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
package com.oltpbenchmark.benchmarks.tpcc;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.oltpbenchmark.QueueLimitException;
import com.oltpbenchmark.ThreadBench;
import com.oltpbenchmark.Phase;
import com.oltpbenchmark.Worker;
import com.oltpbenchmark.TransactionType;


public class TPCCBenchmark {

	private static final class TPCCWorker extends Worker {
		private final jTPCCTerminal terminal;

		public TPCCWorker(jTPCCTerminal terminal) {
			super(terminal.getConnection(), null); // XXX
			this.terminal = terminal;
		}

		@Override
		protected TransactionType doWork(boolean measure,
				Phase phase) {
			TransactionType type = terminal
					.chooseTransaction(phase);
			terminal.executeTransaction(type.getId());
			return type;
		}
	}

	public static ArrayList<TPCCWorker> makeTPCCWorkers() throws IOException {
		// HACK: Turn off terminal messages
		jTPCCHeadless.SILENT = true;
		jTPCCConfig.TERMINAL_MESSAGES = false;

		jTPCCHeadless head = new jTPCCHeadless();
		head.createTerminals();

		ArrayList<TPCCWorker> workers = new ArrayList<TPCCWorker>();
		List<jTPCCTerminal> terminals = head.getTerminals();
		for (jTPCCTerminal terminal : terminals) {
			workers.add(new TPCCWorker(terminal));
		}
		return workers;
	}

	static final int WARMUP_SECONDS = 30;
	static final int MEASURE_SECONDS = 30;

	public static void main(String[] args) throws IOException, SQLException,
			QueueLimitException {

		Properties ini = new Properties();
		ini.load(new FileInputStream(System.getProperty("prop")));

		int intermediateWarmupTime = Integer.parseInt(ini
				.getProperty("intermediateWarmupTimeInSec"));
		int measuringTime = Integer.parseInt(ini.getProperty("measuringTime"));
		int rateLimit = Integer.parseInt(ini.getProperty("rateLimit"));

		ArrayList<TPCCWorker> workers = makeTPCCWorkers();

		ThreadBench.Results r = ThreadBench.runRateLimitedBenchmark(workers,
				intermediateWarmupTime, measuringTime, rateLimit);
		System.out.println("Rate limited " + rateLimit + " reqs/s: " + r);

	}
}
