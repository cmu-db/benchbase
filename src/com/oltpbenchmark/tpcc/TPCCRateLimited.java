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

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.IBenchmarkModule;
import com.oltpbenchmark.QueueLimitException;
import com.oltpbenchmark.ThreadBench;
import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.WorkLoadConfiguration.Phase;
import com.oltpbenchmark.Worker;
import com.oltpbenchmark.tpcc.jTPCCConfig.TransactionType;


public class TPCCRateLimited implements IBenchmarkModule {

	public static final class TPCCWorker extends Worker {
		private final jTPCCTerminal terminal;

		public TPCCWorker(jTPCCTerminal terminal) {
			this.terminal = terminal;
		}

		@Override
		protected TransactionType doWork(boolean measure, Phase phase) {
			jTPCCConfig.TransactionType type = terminal
					.chooseTransaction(phase);
			terminal.executeTransaction(type.ordinal());
			return type;
		}
	}

	/**
	 * @param Bool
	 */
	public ArrayList<Worker> makeWorkers(boolean verbose,WorkLoadConfiguration wrkld) throws IOException {
		// HACK: Turn off terminal messages
		jTPCCHeadless.SILENT = !verbose;
		jTPCCConfig.TERMINAL_MESSAGES = false;

		jTPCCHeadless head = new jTPCCHeadless();

		ArrayList<Worker> workers = new ArrayList<Worker>();
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

		/*
		 * Properties ini = new Properties(); ini.load(new
		 * FileInputStream(System.getProperty("prop")));
		 * 
		 * int initialWarmupTime =
		 * Integer.parseInt(ini.getProperty("initialWarmupTimeInSec")); int
		 * intermediateWarmupTime =
		 * Integer.parseInt(ini.getProperty("intermediateWarmupTimeInSec")); int
		 * measuringTime = Integer.parseInt(ini.getProperty("measuringTime"));
		 * //int coolDownBeforeThrottledMeasures =
		 * Integer.parseInt(ini.getProperty("coolDownBeforeThrottledMeasures"));
		 * int scaleDownPace =
		 * Integer.parseInt(ini.getProperty("scaleDownPace")); int maxSpeed =
		 * Integer.parseInt(ini.getProperty("maxSpeed"));
		 * 
		 * String fileName = ini.getProperty("logfile");
		 * 
		 * FileWriter fstream = new FileWriter(fileName + "_details.csv",true);
		 * BufferedWriter out = new BufferedWriter(fstream);
		 * 
		 * FileWriter fstream2 = new FileWriter(fileName +
		 * "_aggregated.csv",true); BufferedWriter out2 = new
		 * BufferedWriter(fstream2);
		 */

		ArrayList<Worker> workers = new TPCCRateLimited().makeWorkers(false,null);

		/*
		 * MeasureTargetSystem m = new MeasureTargetSystem(out,out2,new
		 * StatisticsCollector(ini),intermediateWarmupTime,measuringTime);
		 * Thread t = new Thread(m); t.start();
		 * 
		 * // Run the unlimited test m.setSpeed(-1); ThreadBench.Results r =
		 * ThreadBench.runBenchmark(workers, initialWarmupTime, measuringTime);
		 * System.out.println("Unlimited: " + r);
		 * 
		 * 
		 * for(int i = scaleDownPace; i<maxSpeed; i+=scaleDownPace){ // Run a
		 * rate-limited test m.setSpeed(i); r =
		 * ThreadBench.runRateLimitedBenchmark(workers, intermediateWarmupTime,
		 * measuringTime, i); System.out.println("Rate limited "+ i +" reqs/s: "
		 * + r); }
		 */
		ThreadBench.Results r = ThreadBench.runRateLimitedBenchmark(workers,
				10, 30 * 60, 1300);
		System.out.println("Rate limited reqs/s: " + r);
		r.writeCSV(30, System.out);
	}

}
