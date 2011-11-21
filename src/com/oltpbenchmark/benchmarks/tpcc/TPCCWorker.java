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

/*
 * jTPCCTerminal - Terminal emulator code for jTPCC (transactions)
 *
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */

import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.DELIVERY;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.NEW_ORDER;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.ORDER_STATUS;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.PAYMENT;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.STOCK_LEVEL;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.TERMINAL_MESSAGES;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.oltpbenchmark.Phase;
import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.epinions.procedures.GetReviewItemById;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.oltpbenchmark.benchmarks.tpcc.procedures.Delivery;
import com.oltpbenchmark.benchmarks.tpcc.procedures.NewOrder;
import com.oltpbenchmark.benchmarks.tpcc.procedures.OrderStatus;
import com.oltpbenchmark.benchmarks.tpcc.procedures.Payment;
import com.oltpbenchmark.benchmarks.tpcc.procedures.StockLevel;
import com.oltpbenchmark.util.SimplePrinter;

public class TPCCWorker extends Worker {

	@Override
	protected TransactionType doWork(boolean measure, Phase phase) {
		TransactionType nextTrans = transactionTypes.getType(phase
				.chooseTransaction());
		return executeTransaction(nextTrans);
	}

	// private TransactionTypes transactionTypes;

	private String terminalName;
	private ResultSet rs = null;
	private final int terminalWarehouseID;
	/** Forms a range [lower, upper] (inclusive). */
	private final int terminalDistrictLowerID;
	private final int terminalDistrictUpperID;
	private double paymentWeight, orderStatusWeight, deliveryWeight,
			stockLevelWeight, newOrderWeight;
	private SimplePrinter terminalOutputArea, errorOutputArea;
	// private boolean debugMessages;
	private final Random gen = new Random();

	private int transactionCount = 1, numWarehouses;
	private int result = 0;

	private static final AtomicInteger terminalId = new AtomicInteger(0);

	public TPCCWorker(String terminalName, int terminalWarehouseID,
			int terminalDistrictLowerID, int terminalDistrictUpperID,
			TPCCBenchmark benchmarkModule, SimplePrinter terminalOutputArea,
			SimplePrinter errorOutputArea, int numWarehouses)
			throws SQLException {
		super(terminalId.getAndIncrement(), benchmarkModule);
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
		terminalMessage("Terminal \'" + terminalName + "\' has WarehouseID="
				+ terminalWarehouseID + " and DistrictID=["
				+ terminalDistrictLowerID + ", " + terminalDistrictUpperID
				+ "].");
	}

	/**
	 * Executes a single TPCC transaction of type transactionType.
	 */
	public TransactionType executeTransaction(TransactionType nextTransaction) {

		int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,
				terminalDistrictUpperID, gen);
		int customerID = TPCCUtil.getCustomerID(gen);

		try {
			switch (nextTransaction.getId()) { // eventually remove this switch
												// and use types
			case 0: // NEW_ORDER
				NewOrder proc = (NewOrder) this.getProcedure(NewOrder.class);
				proc.run(conn, gen, terminalWarehouseID, numWarehouses,
						terminalDistrictLowerID, terminalDistrictUpperID, this);
				break;

			case 1: // PAYMENT
				Payment proc2 = (Payment) this.getProcedure(Payment.class);
				proc2.run(conn, gen, terminalWarehouseID, numWarehouses,
						terminalDistrictLowerID, terminalDistrictUpperID, this);
				break;

			case 2: // STOCK_LEVEL
				StockLevel proc3 = (StockLevel) this
						.getProcedure(StockLevel.class);
				proc3.run(conn, gen, terminalWarehouseID, numWarehouses,
						terminalDistrictLowerID, terminalDistrictUpperID, this);
				break;

			case 3: // ORDER_STATUS
				OrderStatus proc4 = (OrderStatus) this
						.getProcedure(OrderStatus.class);
				proc4.run(conn, gen, terminalWarehouseID, numWarehouses,
						terminalDistrictLowerID, terminalDistrictUpperID, this);
				break;

			case 4:// DELIVERY
				Delivery proc5 = (Delivery) this.getProcedure(Delivery.class);
				proc5.run(conn, gen, terminalWarehouseID, numWarehouses,
						terminalDistrictLowerID, terminalDistrictUpperID, this);
				break;

			default:
				throw new RuntimeException("Bad transaction type = "
						+ nextTransaction);
			}
			transactionCount++;

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		return nextTransaction;
	}

	@Override
	protected void executeWork(TransactionType txnType) {
		// TODO Auto-generated method stub
	}

	/**
	 * Rolls back the current transaction, then rethrows e if it is not a
	 * serialization error. Serialization errors are exceptions caused by
	 * deadlock detection, lock wait timeout, or similar.
	 * 
	 * @param e
	 *            Exception to check if it is a serialization error.
	 * @throws SQLException
	 */
	// Lame deadlock profiling: set this to new HashMap<Integer, Integer>() to
	// enable.
	private final HashMap<Integer, Integer> deadlockLocations = null;

	public void rollbackAndHandleError(SQLException e, Connection conn)
			throws SQLException {
		conn.rollback();

		// Unfortunately, JDBC provides no standardized way to do this, so we
		// resort to this ugly hack.
		boolean isSerialization = false;
		if (e.getErrorCode() == 1213 && e.getSQLState().equals("40001")) {
			// MySQL serialization
			isSerialization = true;
			assert e.getMessage()
					.equals("Deadlock found when trying to get lock; try restarting transaction");
		} else if (e.getErrorCode() == 1205 && e.getSQLState().equals("40001")) {
			// SQL Server serialization
			isSerialization = true;
			assert e.getMessage().equals("Rerun the transaction.");
		} else if (e.getErrorCode() == 8177 && e.getSQLState().equals("72000")) {
			// Oracle serialization
			isSerialization = true;
			assert e.getMessage().equals("Rerun the transaction.");
		} else if (e.getErrorCode() == 0 && e.getSQLState().equals("40001")) {
			// Postgres serialization
			isSerialization = true;
			assert e.getMessage().equals(
					"could not serialize access due to concurrent update");
		} else if (e.getErrorCode() == 1205 && e.getSQLState().equals("41000")) {
			// TODO: This probably shouldn't really happen?
			// FIXME: What is this?
			isSerialization = true;
			assert e.getMessage().equals(
					"Lock wait timeout exceeded; try restarting transaction");
		}

		// Djellel
		// This is to prevent other errors to kill the thread.
		// Errors may include -- duplicate key
		if (!isSerialization) {
			error("Oops SQLException code " + e.getErrorCode() + " state "
					+ e.getSQLState() + " message: " + e.getMessage());
			// throw e; //Otherwise the benchmark will keep going
		}

		if (deadlockLocations != null) {
			String className = this.getClass().getCanonicalName();
			for (StackTraceElement trace : e.getStackTrace()) {
				if (trace.getClassName().equals(className)) {
					int line = trace.getLineNumber();
					Integer count = deadlockLocations.get(line);
					if (count == null)
						count = 0;

					count += 1;
					deadlockLocations.put(line, count);
					return;
				}
			}
			assert false;
		}
	}

	PreparedStatement customerByName;
	boolean isCustomerByName = false;

	private void error(String type) {
		errorOutputArea.println("[ERROR] TERMINAL=" + terminalName + "  TYPE="
				+ type + "  COUNT=" + transactionCount);
	}

	public void terminalMessage(String message) {
		if (TERMINAL_MESSAGES)
			terminalOutputArea.println(message);
	}

	public Connection getConnection() {
		return conn;
	}
}
