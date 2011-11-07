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
package com.oltpbenchmark.benchmarks.wikipedia;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.NotImplementedException;

import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionGenerator;
import com.oltpbenchmark.api.Worker;

public class WikipediaBenchmark extends BenchmarkModule {

	public WikipediaBenchmark(WorkLoadConfiguration workConf) {
		super(workConf);
	}

	@Override
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
		// System.out.println("Using trace:" +workConf.getTracefile());

		TransactionSelector transSel = new TransactionSelector(workConf
				.getTracefile(), workConf.getTransTypes());
		List<WikipediaOperation> trace = Collections.unmodifiableList(transSel
				.readAll());
		transSel.close();
		Random rand = new Random();
		ArrayList<Worker> workers = new ArrayList<Worker>();

		try {
			for (int i = 0; i < workConf.getTerminals(); ++i) {
				Connection conn = this.getConnection();
				conn.setAutoCommit(false);
				TransactionGenerator<WikipediaOperation> generator = new TraceTransactionGenerator(
						trace);
				workers.add(new WikipediaWorker(conn, workConf, generator, workConf
						.getBaseIP()
						+ (i % 256) + "." + rand.nextInt(256), workConf
						.getTransTypes()));
			} // FOR
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return workers;
	}
	
	@Override
	protected void createDatabaseImpl(Connection conn) throws SQLException {
		throw new NotImplementedException();
	}
	
	@Override
	protected void loadDatabaseImpl(Connection conn) throws SQLException {
		throw new NotImplementedException();
	}
}
