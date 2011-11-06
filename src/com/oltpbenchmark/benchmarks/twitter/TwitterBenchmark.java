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
package com.oltpbenchmark.benchmarks.twitter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.oltpbenchmark.BenchmarkModule;
import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.Worker;
import com.oltpbenchmark.benchmarks.TransactionGenerator;

public class TwitterBenchmark extends BenchmarkModule {

	public TwitterBenchmark(WorkLoadConfiguration workConf) {
		super(workConf);
	}

	@Override
	public List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
		TransactionSelector transSel = new TransactionSelector(workConf
				.getTracefile(), workConf.getTracefile2(), workConf
				.getTransTypes());
		List<TwitterOperation> trace = Collections.unmodifiableList(transSel.readAll());
		transSel.close();
		ArrayList<Worker> workers = new ArrayList<Worker>();
		try {
			for (int i = 0; i < this.workConf.getTerminals(); ++i) {
				Connection conn = this.getConnection();
				conn.setAutoCommit(false);
				TransactionGenerator<TwitterOperation> generator = new TraceTransactionGenerator(
						trace);
				workers.add(new TwitterWorker(conn, generator, this.workConf));
			} // FOR
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return workers;
	}
}
