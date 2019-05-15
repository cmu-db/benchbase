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



/***
 *   TPC-H implementation
 *
 *   Ben Reilly (bd.reilly@gmail.com)
 *   Ippokratis Pandis (ipandis@us.ibm.com)
 *
 ***/
 
package com.oltpbenchmark.benchmarks.tpch;

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

import com.oltpbenchmark.benchmarks.tpch.procedures.Q1;

public class TPCHBenchmark extends BenchmarkModule {
    private static final Logger LOG = Logger.getLogger(TPCHBenchmark.class);

	public TPCHBenchmark(WorkloadConfiguration workConf) {
		super("tpch", workConf, true);
	}

	@Override
	protected Package getProcedurePackageImpl() {
		return (Q1.class.getPackage());
	}

	
	/**
	 * @param Bool
	 */
	@Override
	protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException {
		List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();

		int numTerminals = workConf.getTerminals();
        LOG.info(String.format("Creating %d workers for TPC-H", numTerminals));
        for (int i = 0; i < numTerminals; i++) {
            workers.add(new TPCHWorker(this, i));
		}
		return workers;
	}

	@Override
	protected Loader<TPCHBenchmark> makeLoaderImpl() throws SQLException {
		return new TPCHLoader(this);
	}

} 
