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

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.TransactionGenerator;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.wikipedia.procedures.AddWatchList;
import com.oltpbenchmark.benchmarks.wikipedia.util.TraceTransactionGenerator;
import com.oltpbenchmark.benchmarks.wikipedia.util.TransactionSelector;
import com.oltpbenchmark.benchmarks.wikipedia.util.WikipediaOperation;

public class WikipediaBenchmark extends BenchmarkModule {
    private static final Logger LOG = Logger.getLogger(WikipediaBenchmark.class);

	private final WikipediaConfiguration wikiConf;
	
	public WikipediaBenchmark(WorkloadConfiguration workConf) {		
		super("wikipedia", workConf, true);
		this.wikiConf = new WikipediaConfiguration(workConf);
	}

	@Override
	protected Package getProcedurePackageImpl() {
		return (AddWatchList.class.getPackage());
	}
	
	@Override
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
	    if (LOG.isDebugEnabled())
	        LOG.debug("Using trace:" + wikiConf.getTracefile());

		TransactionSelector transSel = new TransactionSelector(wikiConf.getTracefile(), 
				                                               workConf.getTransTypes());
		List<WikipediaOperation> trace = Collections.unmodifiableList(transSel.readAll());
		transSel.close();
		
		Random rand = new Random();
		ArrayList<Worker> workers = new ArrayList<Worker>();
		for (int i = 0; i < workConf.getTerminals(); ++i) {
			TransactionGenerator<WikipediaOperation> generator = new TraceTransactionGenerator(trace);
			String ipAddress = String.format("%s.%d.%d", wikiConf.getBaseIP(), (i % 256), rand.nextInt(256));
			WikipediaWorker worker = new WikipediaWorker(i, this, generator, ipAddress);
			workers.add(worker);
		} // FOR
		return workers;
	}
	
	@Override
	protected Loader makeLoaderImpl(Connection conn) throws SQLException {
		return new WikipediaLoader(this, conn);
	}
}
