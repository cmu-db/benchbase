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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

import com.oltpbenchmark.ThreadBench;
import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.Phase;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.util.QueueLimitException;


public class TPCCBenchmark extends BenchmarkModule {

	public TPCCBenchmark(WorkLoadConfiguration wrkld) {
		super("tpcc", wrkld);
	}

	@Override
	protected Map<TransactionType, Procedure> getProcedures(
			Collection<TransactionType> txns) {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
	 * @param Bool
	 */
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
		// HACK: Turn off terminal messages
		jTPCCHeadless.SILENT = !verbose;
		jTPCCConfig.TERMINAL_MESSAGES = false;
		jTPCCHeadless head = new jTPCCHeadless();
		List<TPCCWorker> terminals = head.getTerminals();
		ArrayList<Worker> workers = new ArrayList<Worker>();
		workers.addAll(terminals);
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

		List<Worker> workers = new TPCCBenchmark(null).makeWorkers(false);

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
	
	@Override
	protected void loadDatabaseImpl(Connection conn) throws SQLException {
		// TODO TPCCLoader loader = new TPCCLoader();
		throw new NotImplementedException();
	}

}
