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
import com.oltpbenchmark.util.SimpleSystemPrinter;

import com.oltpbenchmark.benchmarks.tpch.queries.Q1;


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
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
		ArrayList<Worker> workers = new ArrayList<Worker>();

		try {
			List<TPCHWorker> terminals = createTerminals();
			workers.addAll(terminals);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return workers;
	}

	@Override
	protected Loader makeLoaderImpl(Connection conn) throws SQLException {
		return new TPCHLoader(this, conn);
	}

	protected ArrayList<TPCHWorker> createTerminals() throws SQLException {
        int numTerminals = workConf.getTerminals();

        ArrayList<TPCHWorker> ret = new ArrayList<TPCHWorker>();
        LOG.info(String.format("Creating %d workers for TPC-H", numTerminals));
        for (int i = 0; i < numTerminals; i++)
            ret.add(new TPCHWorker(this));

        return ret;
    }


} 
