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


package com.oltpbenchmark.benchmarks.tpcc;

/*
 * jTPCCTerminal - Terminal emulator code for jTPCC (transactions)
 *
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */

import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcc.procedures.Delivery;
import com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder;
import com.oltpbenchmark.benchmarks.tpcc.procedures.OrderStatus;
import com.oltpbenchmark.benchmarks.tpcc.procedures.Payment;
import com.oltpbenchmark.benchmarks.tpcc.procedures.StockLevel;
import com.oltpbenchmark.benchmarks.tpcc.procedures.TPCCProcedure;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.SimplePrinter;

public class TPCCWorker extends Worker {

	// private TransactionTypes transactionTypes;

	private String terminalName;

	private final int terminalWarehouseID;
	/** Forms a range [lower, upper] (inclusive). */
	private final int terminalDistrictLowerID;
	private final int terminalDistrictUpperID;
	private SimplePrinter terminalOutputArea, errorOutputArea;
	// private boolean debugMessages;
	private final Random gen = new Random();

	private int transactionCount = 1, numWarehouses;

	private static final AtomicInteger terminalId = new AtomicInteger(0);

	public TPCCWorker(String terminalName, int terminalWarehouseID,
			int terminalDistrictLowerID, int terminalDistrictUpperID,
			TPCCBenchmark benchmarkModule, SimplePrinter terminalOutputArea,
			SimplePrinter errorOutputArea, int numWarehouses)
			throws SQLException {
		super(benchmarkModule, terminalId.getAndIncrement());
		
		this.terminalName = terminalName;

		this.terminalWarehouseID = terminalWarehouseID;
		this.terminalDistrictLowerID = terminalDistrictLowerID;
		this.terminalDistrictUpperID = terminalDistrictUpperID;
		assert this.terminalDistrictLowerID >= 1;
		assert this.terminalDistrictUpperID <= jTPCCConfig.configDistPerWhse;
		assert this.terminalDistrictLowerID <= this.terminalDistrictUpperID;
		this.terminalOutputArea = terminalOutputArea;
		this.errorOutputArea = errorOutputArea;
		this.numWarehouses = numWarehouses;
	}

	/**
	 * Executes a single TPCC transaction of type transactionType.
	 */
	@Override
    protected TransactionStatus executeWork(TransactionType nextTransaction) throws UserAbortException, SQLException {
        try {
            TPCCProcedure proc = (TPCCProcedure) this.getProcedure(nextTransaction.getProcedureClass());
            proc.run(conn, gen, terminalWarehouseID, numWarehouses,
                    terminalDistrictLowerID, terminalDistrictUpperID, this);
        } catch (ClassCastException ex){
            //fail gracefully
        	System.err.println("We have been invoked with an INVALID transactionType?!");
        	throw new RuntimeException("Bad transaction type = "+ nextTransaction);
	    } catch (RuntimeException ex) {
	        conn.rollback();
	        return (TransactionStatus.RETRY_DIFFERENT);
	    }
		transactionCount++;
        conn.commit();
        return (TransactionStatus.SUCCESS);
	}

//	/**
//	 * Rolls back the current transaction, then rethrows e if it is not a
//	 * serialization error. Serialization errors are exceptions caused by
//	 * deadlock detection, lock wait timeout, or similar.
//	 * 
//	 * @param e
//	 *            Exception to check if it is a serialization error.
//	 * @throws SQLException
//	 */
//	// Lame deadlock profiling: set this to new HashMap<Integer, Integer>() to
//	// enable.
//	private final HashMap<Integer, Integer> deadlockLocations = null;
//
//	public void rollbackAndHandleError(SQLException e, Connection conn)
//			throws SQLException {
//		conn.rollback();
//
//		// Unfortunately, JDBC provides no standardized way to do this, so we
//		// resort to this ugly hack.
//		boolean isSerialization = false;
//		if (e.getErrorCode() == 1213 && e.getSQLState().equals("40001")) {
//			// MySQL serialization
//			isSerialization = true;
//			assert e.getMessage()
//					.equals("Deadlock found when trying to get lock; try restarting transaction");
//		} else if (e.getErrorCode() == 1205 && e.getSQLState().equals("40001")) {
//			// SQL Server serialization
//			isSerialization = true;
//			assert e.getMessage().equals("Rerun the transaction.");
//		} else if (e.getErrorCode() == 8177 && e.getSQLState().equals("72000")) {
//			// Oracle serialization
//			isSerialization = true;
//			assert e.getMessage().equals("Rerun the transaction.");
//		} else if (e.getErrorCode() == 0 && e.getSQLState().equals("40001")) {
//			// Postgres serialization
//			isSerialization = true;
//			assert e.getMessage().equals(
//					"could not serialize access due to concurrent update");
//		} else if (e.getErrorCode() == 1205 && e.getSQLState().equals("41000")) {
//			// TODO: This probably shouldn't really happen?
//			// FIXME: What is this?
//			isSerialization = true;
//			assert e.getMessage().equals(
//					"Lock wait timeout exceeded; try restarting transaction");
//		}
//
//		// Djellel
//		// This is to prevent other errors to kill the thread.
//		// Errors may include -- duplicate key
//		if (!isSerialization) {
//			error("Oops SQLException code " + e.getErrorCode() + " state "
//					+ e.getSQLState() + " message: " + e.getMessage());
//			// throw e; //Otherwise the benchmark will keep going
//		}
//
//		if (deadlockLocations != null) {
//			String className = this.getClass().getCanonicalName();
//			for (StackTraceElement trace : e.getStackTrace()) {
//				if (trace.getClassName().equals(className)) {
//					int line = trace.getLineNumber();
//					Integer count = deadlockLocations.get(line);
//					if (count == null)
//						count = 0;
//
//					count += 1;
//					deadlockLocations.put(line, count);
//					return;
//				}
//			}
//			assert false;
//		}
//	}
//
//	PreparedStatement customerByName;
//	boolean isCustomerByName = false;
//
//	private void error(String type) {
//		errorOutputArea.println("[ERROR] TERMINAL=" + terminalName + "  TYPE="
//				+ type + "  COUNT=" + transactionCount);
//	}
//
//
//	public Connection getConnection() {
//		return conn;
//	}
}
