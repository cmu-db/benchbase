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
package com.oltpbenchmark.benchmarks.tpcc.old;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import com.oltpbenchmark.ThreadBench;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark;
import com.oltpbenchmark.util.QueueLimitException;


public class TPCCRateLimitedFromFile {

	public static void main(String[] args) throws QueueLimitException,
			IOException {
		if (args.length < 1) {
			System.err
					.println("Usage: TPCCRateLimitedFromFile <config file> <load profile file>");
			System.exit(1);
		}

		// Load configuration parameters from property file
		Properties ini = new Properties();
		ini.load(new FileInputStream(args[0])); // useless for now... leaving it
												// there for all other
												// parameters..

		File profileFile = new File(args[1]);

		List<Worker> workers = new TPCCBenchmark(null).makeWorkers(true);

		ThreadBench.Results r = ThreadBench.runRateLimitedBenchmark(workers,
				profileFile);

		System.out.println("Rate limited reqs/s: " + r);

		r.writeAllCSVAbsoluteTiming(System.out);
	}
}
