/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
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
	protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException {
		TransactionSelector transSel = new TransactionSelector(
		twitterConf.getTracefile(), 
		twitterConf.getTracefile2(), 
		workConf.getTransTypes());
		List<TwitterOperation> trace = Collections.unmodifiableList(transSel.readAll());
		transSel.close();
		List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();
		for (int i = 0; i < workConf.getTerminals(); ++i) {
			TransactionGenerator<TwitterOperation> generator = 
			    new TraceTransactionGenerator(trace);
			workers.add(new TwitterWorker(this, i, generator));
		} // FOR
		return workers;
	}
	
	@Override
	protected Loader<TwitterBenchmark> makeLoaderImpl() throws SQLException {
		return new TwitterLoader(this);
	}
}
