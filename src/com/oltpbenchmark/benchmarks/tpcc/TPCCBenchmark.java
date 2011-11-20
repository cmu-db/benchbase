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

import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.terminalPrefix;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.epinions.procedures.GetAverageRatingByTrustedUser;
import com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder;
import com.oltpbenchmark.util.SimpleSystemPrinter;


public class TPCCBenchmark extends BenchmarkModule {

	boolean verbose;
	public TPCCBenchmark(WorkLoadConfiguration wrkld) {
		super("tpcc", wrkld);
	}

	@Override
	protected Package getProcedurePackageImpl() {
	    return NewOrder.class.getPackage();
	}
	
	/**
	 * @param Bool
	 */
	@Override
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
		// HACK: Turn off terminal messages
		this.verbose = !verbose;
		jTPCCConfig.TERMINAL_MESSAGES = false;
		ArrayList<Worker> workers = new ArrayList<Worker>();
		
		try {
			List<TPCCWorker> terminals = createTerminals();
			workers.addAll(terminals);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return workers;
	}
	
	@Override
	protected void loadDatabaseImpl(Connection conn) throws SQLException {
		// TODO TPCCLoader loader = new TPCCLoader();
		throw new NotImplementedException();
	}
	
	
	protected ArrayList<TPCCWorker> createTerminals() throws SQLException{
		
		TPCCWorker[] terminals = new TPCCWorker[workConf.getTerminals()];

		int numWarehouses = workConf.getNumWarehouses();
		int numTerminals = workConf.getTerminals();
		assert (numTerminals <= 0 || numTerminals > 10 * numWarehouses);
		 
		String[] terminalNames = new String[numTerminals];
		// TODO: This is currenly broken: fix it!
		int warehouseOffset = Integer.getInteger("warehouseOffset", 1);
		assert warehouseOffset == 1;

		// We distribute terminals evenly across the warehouses
		// Eg. if there are 10 terminals across 7 warehouses, they
		// are distributed as
		// 1, 1, 2, 1, 2, 1, 2
		final double terminalsPerWarehouse = (double) numTerminals
				/ numWarehouses;
		assert terminalsPerWarehouse >= 1;
		for (int w = 0; w < numWarehouses; w++) {
			// Compute the number of terminals in *this* warehouse
			int lowerTerminalId = (int) (w * terminalsPerWarehouse);
			int upperTerminalId = (int) ((w + 1) * terminalsPerWarehouse);
			// protect against double rounding errors
			int w_id = w + 1;
			if (w_id == numWarehouses)
				upperTerminalId = numTerminals;
			int numWarehouseTerminals = upperTerminalId - lowerTerminalId;

			// System.out.println("w_id " + w_id + " = " +
			// numWarehouseTerminals +" terminals");

			final double districtsPerTerminal = jTPCCConfig.configDistPerWhse
					/ (double) numWarehouseTerminals;
			assert districtsPerTerminal >= 1;
			for (int terminalId = 0; terminalId < numWarehouseTerminals; terminalId++) {
				int lowerDistrictId = (int) (terminalId * districtsPerTerminal);
				int upperDistrictId = (int) ((terminalId + 1) * districtsPerTerminal);
				if (terminalId + 1 == numWarehouseTerminals) {
					upperDistrictId = jTPCCConfig.configDistPerWhse;
				}
				lowerDistrictId += 1;

				String terminalName = terminalPrefix + "w" + w_id + "d" + lowerDistrictId + "-" + upperDistrictId;

				TPCCWorker terminal = new TPCCWorker(terminalName, w_id,
						lowerDistrictId, upperDistrictId, this,
						new SimpleSystemPrinter(null), new SimpleSystemPrinter(
								System.err), numWarehouses);
				terminals[lowerTerminalId + terminalId] = terminal;
				terminalNames[lowerTerminalId + terminalId] = terminalName;
			}

		}
		assert terminals[terminals.length - 1] != null;
		
		ArrayList<TPCCWorker> ret = new ArrayList<TPCCWorker>();
		for(TPCCWorker w:terminals)
			ret.add(w);
		return ret;
	}

}
