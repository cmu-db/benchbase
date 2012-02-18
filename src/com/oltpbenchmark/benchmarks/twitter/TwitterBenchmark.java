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

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.TransactionGenerator;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.twitter.procedures.GetFollowers;
import com.oltpbenchmark.benchmarks.twitter.util.TraceTransactionGenerator;
import com.oltpbenchmark.benchmarks.twitter.util.TransactionSelector;
import com.oltpbenchmark.benchmarks.twitter.util.TwitterOperation;

public class TwitterBenchmark extends BenchmarkModule {
	
	private TwitterConfiguration twitterConf;

	public TwitterBenchmark(WorkloadConfiguration workConf) {
		super("twitter", workConf, true);
		this.twitterConf = new TwitterConfiguration(workConf);
	}
	
	@Override
	protected Package getProcedurePackageImpl() {
	    return GetFollowers.class.getPackage();
	}

	@Override
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
		TransactionSelector transSel = new TransactionSelector(
		twitterConf.getTracefile(), 
		twitterConf.getTracefile2(), 
		workConf.getTransTypes());
		List<TwitterOperation> trace = Collections.unmodifiableList(transSel.readAll());
		transSel.close();
		ArrayList<Worker> workers = new ArrayList<Worker>();
		for (int i = 0; i < workConf.getTerminals(); ++i) {
			TransactionGenerator<TwitterOperation> generator = 
			    new TraceTransactionGenerator(trace);
			workers.add(new TwitterWorker(i, this, generator));
		} // FOR
		return workers;
	}
	
	@Override
	protected Loader makeLoaderImpl(Connection conn) throws SQLException {
		return new TwitterLoader(this, conn);
	}
}
