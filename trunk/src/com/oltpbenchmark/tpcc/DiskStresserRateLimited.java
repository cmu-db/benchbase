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
package com.oltpbenchmark.tpcc;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.oltpbenchmark.QueueLimitException;
import com.oltpbenchmark.ThreadBench;
import com.oltpbenchmark.WorkLoadConfiguration.Phase;
import com.oltpbenchmark.tpcc.jTPCCConfig.TransactionType;


public class DiskStresserRateLimited {

	private static final class DiskStresserWorker extends ThreadBench.Worker {
		private final DiskStresser terminal;
		private final int type;

		public DiskStresserWorker(DiskStresser terminal, int type) {
			this.terminal = terminal;
			this.type = type;
		}

		@Override
		protected TransactionType doWork(boolean measure, Phase phase) {

			try {
				terminal.executeTransaction(type);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return jTPCCConfig.TransactionType.values()[type];
		}
	}

	public static ArrayList<DiskStresserWorker> makeDiskStresserWorkers(
			int size, int numTerminals, int type, BufferedWriter out3)
			throws IOException, SQLException {
		// HACK: Turn off terminal messages
		jTPCCHeadless.SILENT = true;
		jTPCCConfig.TERMINAL_MESSAGES = false;

		jTPCCHeadless head = new jTPCCHeadless();
		head.createTerminals();

		ArrayList<DiskStresserWorker> workers = new ArrayList<DiskStresserWorker>();
		List<DiskStresser> terminals = DiskStresser.getTerminals(size,
				numTerminals, out3);

		for (DiskStresser terminal : terminals) {
			workers.add(new DiskStresserWorker(terminal, type));
		}
		return workers;
	}

	static final int WARMUP_SECONDS = 30;
	static final int MEASURE_SECONDS = 30;

	public static void main(String[] args) throws IOException, SQLException {

		Properties ini = new Properties();
		ini.load(new FileInputStream(System.getProperty("prop")));

		int initialWarmupTime = Integer.parseInt(ini
				.getProperty("initialWarmupTimeInSec"));
		int intermediateWarmupTime = Integer.parseInt(ini
				.getProperty("intermediateWarmupTimeInSec"));
		int measuringTime = Integer.parseInt(ini.getProperty("measuringTime"));
		// int coolDownBeforeThrottledMeasures =
		// Integer.parseInt(ini.getProperty("coolDownBeforeThrottledMeasures"));
		int scaleDownPace = Integer.parseInt(ini.getProperty("scaleDownPace"));
		int maxSpeed = Integer.parseInt(ini.getProperty("maxSpeed"));

		String fileName = ini.getProperty("logfile");

		FileWriter fstream = new FileWriter(fileName + "_details.csv", true);
		BufferedWriter out = new BufferedWriter(fstream);

		FileWriter fstream2 = new FileWriter(fileName + "_aggregated.csv", true);
		BufferedWriter out2 = new BufferedWriter(fstream2);

		FileWriter fstream3 = new FileWriter(fileName + "_latency.csv", true);
		BufferedWriter out3 = new BufferedWriter(fstream2);

		MeasureTargetSystem m = new MeasureTargetSystem(out, out2,
				new StatisticsCollector(ini), intermediateWarmupTime,
				measuringTime);
		Thread t = new Thread(m);
		t.start();

		for (int j = 1; j < 10; j++) {

			// args[0] number of terminals args[1] type of terminal (control
			// type of queries in diskStresser.
			int temp = 0;
			if (args.length == 2)
				temp = Integer.parseInt(args[1]);

			ArrayList<DiskStresserWorker> workers = makeDiskStresserWorkers(j,
					Integer.parseInt(args[0]), temp, out3);

			// Run the unlimited test
			m.setSpeed(-1);
			ThreadBench.Results r = ThreadBench.runBenchmark(workers,
					initialWarmupTime, measuringTime);
			System.out.println("Unlimited: " + r);

			for (int i = scaleDownPace; i < maxSpeed; i += scaleDownPace) {
				// Run a rate-limited test
				m.setSpeed(i);
				try {
					r = ThreadBench.runRateLimitedBenchmark(workers,
							intermediateWarmupTime, measuringTime, i);
					System.out.println("Rate limited " + i + " reqs/s: " + r);
				} catch (QueueLimitException e) {
					System.out
							.println("Can't keep up at "
									+ i
									+ "reqs/s... terminating this run and moving on to next workload size");
					break;
				}
			}

			for (DiskStresserWorker te : workers)
				te.terminal.close();

		}

	}

}
