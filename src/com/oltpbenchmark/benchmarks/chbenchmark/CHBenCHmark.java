package com.oltpbenchmark.benchmarks.chbenchmark;


import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.Q1;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;

public class CHBenCHmark extends BenchmarkModule {
	private static final Logger LOG = Logger.getLogger(CHBenCHmark.class);
	
	public CHBenCHmark(WorkloadConfiguration workConf) {
		super("chbenchmark", workConf, true);
	}
	
	protected Package getProcedurePackageImpl() {
		return (Q1.class.getPackage());
	}
	
	/**
	 * @param Bool
	 */
	@Override
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
		// HACK: Turn off terminal messages
		jTPCCConfig.TERMINAL_MESSAGES = false;
		ArrayList<Worker> workers = new ArrayList<Worker>();

		try {
			List<CHBenCHmarkWorker> terminals = createCHTerminals();
			workers.addAll(terminals);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return workers;
	}
	
	protected ArrayList<CHBenCHmarkWorker> createCHTerminals() throws SQLException {

		int numTerminals = workConf.getTerminals();
		
		

		ArrayList<CHBenCHmarkWorker> ret = new ArrayList<CHBenCHmarkWorker>();
		LOG.info(String.format("Creating %d workers for CHBenCHMark", numTerminals));
		for (int i = 0; i < numTerminals; i++)
			
			ret.add(new CHBenCHmarkWorker(this));
		return ret;
	}

	protected Loader makeLoaderImpl(Connection conn) throws SQLException {
		return new CHBenCHmarkLoader(this, conn);
	}
	
}
