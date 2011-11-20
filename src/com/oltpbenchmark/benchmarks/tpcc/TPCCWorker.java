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

		int nextTrans = phase.chooseTransaction();
		executeTransaction(nextTrans);
		return transactionTypes.getType(nextTrans);
	}
	
	//private TransactionTypes transactionTypes;
	
	private String terminalName;
	private ResultSet rs = null;
	private final int terminalWarehouseID;
	/** Forms a range [lower, upper] (inclusive). */
	private final int terminalDistrictLowerID;
	private final int terminalDistrictUpperID;
	private double paymentWeight, orderStatusWeight, deliveryWeight,
			stockLevelWeight, newOrderWeight;
	private SimplePrinter terminalOutputArea, errorOutputArea;
	//private boolean debugMessages;
	private final Random gen = new Random();

	private int transactionCount = 1, numWarehouses;
	private int result = 0;



	// Payment Txn
	private PreparedStatement payUpdateWhse = null;
	private PreparedStatement payGetWhse = null;
	private PreparedStatement payUpdateDist = null;
	private PreparedStatement payGetDist = null;
	private PreparedStatement payGetCust = null;
	private PreparedStatement payGetCustCdata = null;
	private PreparedStatement payUpdateCustBalCdata = null;
	private PreparedStatement payUpdateCustBal = null;
	private PreparedStatement payInsertHist = null;

	// Order Status Txn
	private PreparedStatement ordStatGetNewestOrd = null;
	private PreparedStatement ordStatGetOrderLines = null;

	// Delivery Txn
	private PreparedStatement delivGetOrderId = null;
	private PreparedStatement delivDeleteNewOrder = null;
	private PreparedStatement delivGetCustId = null;
	private PreparedStatement delivUpdateCarrierId = null;
	private PreparedStatement delivUpdateDeliveryDate = null;
	private PreparedStatement delivSumOrderAmount = null;
	private PreparedStatement delivUpdateCustBalDelivCnt = null;

	// Stock Level Txn
	private PreparedStatement stockGetDistOrderId = null;
	private PreparedStatement stockGetCountStock = null;

	private static final AtomicInteger terminalId = new AtomicInteger(0);
	
	public TPCCWorker(String terminalName, int terminalWarehouseID,
			int terminalDistrictLowerID, int terminalDistrictUpperID,
			TPCCBenchmark benchmarkModule,
			SimplePrinter terminalOutputArea, SimplePrinter errorOutputArea,
			 int numWarehouses) throws SQLException {
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


	public TransactionType chooseTransaction(Phase phase) {
		int nextTrans = phase.chooseTransaction();
		
		if (phase != null) {

			double count = 0;
			
			for(Double d:phase.weights)
				count+=d;
					
			if (Math.abs(count - 100) > 0.000001)
				throw new RuntimeException(
						"Wrong transaction percentages in Configuration file.. they don't add up to 100%");

			//weights of 0 is for the "INVALID" transaction
			newOrderWeight = phase.weights.get(transactionTypes.getType(NewOrder.class).getId());
			paymentWeight = phase.weights.get(transactionTypes.getType(Payment.class).getId());
			orderStatusWeight = phase.weights.get(transactionTypes.getType(OrderStatus.class).getId());
			deliveryWeight = phase.weights.get(transactionTypes.getType(Delivery.class).getId());
			stockLevelWeight = phase.weights.get(transactionTypes.getType(StockLevel.class).getId());
		}

		// Generate an integer in the range [1, 100] (that means inclusive!)
		int randomPercentage = gen.nextInt(100) + 1;

		jTPCCConfig.TransactionType type;
		if (randomPercentage <= paymentWeight) {
			type = jTPCCConfig.TransactionType.PAYMENT;
		} else if (randomPercentage <= paymentWeight + stockLevelWeight) {
			type = jTPCCConfig.TransactionType.STOCK_LEVEL;
		} else if (randomPercentage <= paymentWeight + stockLevelWeight
				+ orderStatusWeight) {
			type = jTPCCConfig.TransactionType.ORDER_STATUS;
		} else if (randomPercentage <= paymentWeight + stockLevelWeight
				+ orderStatusWeight + deliveryWeight) {
			type = jTPCCConfig.TransactionType.DELIVERY;
		} else {
			assert paymentWeight + stockLevelWeight + orderStatusWeight
					+ deliveryWeight < randomPercentage
					&& randomPercentage <= 100;
			type = jTPCCConfig.TransactionType.NEW_ORDER;
		}

		return transactionTypes.getType(type.ordinal());
	}

	/** 
	 * Executes a single TPCC transaction of type transactionType. 
	 */
	public int executeTransaction(int transaction) {

		int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
		int customerID = TPCCUtil.getCustomerID(gen);

		
		int result = 0;

		try {
			switch (transaction) {
			case 0: //INVALID
				System.err.println("We have been invoked with an INVALID transactionType?!");
				break;
				
			case 1: //NEW_ORDER
				NewOrder proc = (NewOrder) this.getProcedure(NewOrder.class);
					NewOrder nt = new NewOrder(conn,gen,terminalWarehouseID,numWarehouses,terminalDistrictLowerID,terminalDistrictUpperID,this);
					nt.run();
				break;

			case 2: //PAYMENT
				districtID = chooseRandomDistrict();

				int x = TPCCUtil.randomNumber(1, 100, gen);
				int customerDistrictID;
				int customerWarehouseID;
				if (x <= 85) {
					customerDistrictID = districtID;
					customerWarehouseID = terminalWarehouseID;
				} else {
					customerDistrictID = TPCCUtil.randomNumber(1,
							jTPCCConfig.configDistPerWhse, gen);
					do {
						customerWarehouseID = TPCCUtil.randomNumber(1,
								numWarehouses, gen);
					} while (customerWarehouseID == terminalWarehouseID
							&& numWarehouses > 1);
				}

				long y = TPCCUtil.randomNumber(1, 100, gen);
				boolean customerByName;
				String customerLastName = null;
				customerID = -1;
				if (y <= 60) {
					// 60% lookups by last name
					customerByName = true;
					customerLastName = TPCCUtil
							.getNonUniformRandomLastNameForRun(gen);
				} else {
					// 40% lookups by customer ID
					customerByName = false;
					customerID = TPCCUtil.getCustomerID(gen);
				}

				float paymentAmount = (float) (TPCCUtil.randomNumber(100,
						500000, gen) / 100.0);

				terminalMessage("\nStarting transaction #" + transactionCount
						+ " (Payment)...");
				while (true) {
					try {
						paymentTransaction(terminalWarehouseID,
								customerWarehouseID, paymentAmount, districtID,
								customerDistrictID, customerID,
								customerLastName, customerByName);
						break;
					} catch (SQLException e) {
						rollbackAndHandleError(e,conn);
					}
				}
				break;

			case 3: //STOCK_LEVEL
				int threshold = TPCCUtil.randomNumber(10, 20, gen);

				terminalMessage("\nStarting transaction #" + transactionCount
						+ " (Stock-Level)...");
				districtID = chooseRandomDistrict();

				while (true) {
					try {
						stockLevelTransaction(terminalWarehouseID, districtID,
								threshold);
						break;
					} catch (SQLException e) {
						rollbackAndHandleError(e,conn);
					}
				}
				break;

			case 4: //ORDER_STATUS
				districtID = chooseRandomDistrict();

				y = TPCCUtil.randomNumber(1, 100, gen);
				customerLastName = null;
				customerID = -1;
				if (y <= 60) {
					customerByName = true;
					customerLastName = TPCCUtil
							.getNonUniformRandomLastNameForRun(gen);
				} else {
					customerByName = false;
					customerID = TPCCUtil.getCustomerID(gen);
				}

				terminalMessage("\nStarting transaction #" + transactionCount
						+ " (Order-Status)...");
				while (true) {
					try {
						orderStatusTransaction(terminalWarehouseID, districtID,
								customerID, customerLastName, customerByName);
						break;
					} catch (SQLException e) {
						rollbackAndHandleError(e,conn);
					}
				}
				break;

			case 5://DELIVERY
				int orderCarrierID = TPCCUtil.randomNumber(1, 10, gen);

				terminalMessage("\nStarting transaction #" + transactionCount
						+ " (Delivery)...");
				while (true) {
					try {
						result = deliveryTransaction(terminalWarehouseID,
								orderCarrierID);
						break;
					} catch (SQLException e) {
						rollbackAndHandleError(e,conn);
					}
				}
				break;

			default:
				throw new RuntimeException("Bad transaction type = "
						+ transaction);
			}
			transactionCount++;

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		return result;
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

	public void rollbackAndHandleError(SQLException e, Connection conn) throws SQLException {
		conn.rollback();

		// Unfortunately, JDBC provides no standardized way to do this, so we
		// resort to this ugly hack.
		boolean isSerialization = false;
		if (e.getErrorCode() == 1213 && e.getSQLState().equals("40001")) {
			// MySQL serialization
			isSerialization = true;
			assert e.getMessage().equals("Deadlock found when trying to get lock; try restarting transaction");
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
			assert e.getMessage().equals("could not serialize access due to concurrent update");
		}  else if (e.getErrorCode() == 1205 && e.getSQLState().equals("41000")) {
			// TODO: This probably shouldn't really happen?
			// FIXME: What is this?
			isSerialization = true;
			assert e.getMessage().equals("Lock wait timeout exceeded; try restarting transaction");	
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

	private int chooseRandomDistrict() {
		return TPCCUtil.randomNumber(terminalDistrictLowerID,
				terminalDistrictUpperID, gen);
	}

	private int deliveryTransaction(int w_id, int o_carrier_id)
			throws SQLException {
		int d_id, c_id;
		float ol_total;
		int[] orderIDs;

		orderIDs = new int[10];
		for (d_id = 1; d_id <= 10; d_id++) {

			if (delivGetOrderId == null) {
				delivGetOrderId = conn
						.prepareStatement("SELECT no_o_id FROM new_order WHERE no_d_id = ?"
								+ " AND no_w_id = ? AND ROWNUM = 1"
								+ " ORDER BY no_o_id ASC");
			}
			delivGetOrderId.setInt(1, d_id);
			delivGetOrderId.setInt(2, w_id);
			rs = delivGetOrderId.executeQuery();
			if (!rs.next()) {
				// This district has no new orders; this can happen but should
				// be rare
				continue;
			}

			int no_o_id = rs.getInt("no_o_id");
			orderIDs[d_id - 1] = no_o_id;
			rs.close();
			rs = null;

			if (delivDeleteNewOrder == null) {
				delivDeleteNewOrder = conn
						.prepareStatement("DELETE FROM new_order"
								+ " WHERE no_o_id = ? AND no_d_id = ?"
								+ " AND no_w_id = ?");
			}
			delivDeleteNewOrder.setInt(1, no_o_id);
			delivDeleteNewOrder.setInt(2, d_id);
			delivDeleteNewOrder.setInt(3, w_id);
			result = delivDeleteNewOrder.executeUpdate();
			if (result != 1) {
				// This code used to run in a loop in an attempt to make this
				// work
				// with MySQL's default weird consistency level. We just always
				// run
				// this as SERIALIZABLE instead. I don't *think* that fixing
				// this one
				// error makes this work with MySQL's default consistency.
				// Careful
				// auditing would be required.
				throw new RuntimeException(
						"new order w_id="
								+ w_id
								+ " d_id="
								+ d_id
								+ " no_o_id="
								+ no_o_id
								+ " delete failed (not running with SERIALIZABLE isolation?)");
			}

			if (delivGetCustId == null) {
				delivGetCustId = conn.prepareStatement("SELECT o_c_id"
						+ " FROM oorder" + " WHERE o_id = ?"
						+ " AND o_d_id = ?" + " AND o_w_id = ?");
			}
			delivGetCustId.setInt(1, no_o_id);
			delivGetCustId.setInt(2, d_id);
			delivGetCustId.setInt(3, w_id);
			rs = delivGetCustId.executeQuery();

			if (!rs.next())
				throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID="
						+ d_id + " O_W_ID=" + w_id + " not found!");
			c_id = rs.getInt("o_c_id");
			rs.close();
			rs = null;

			if (delivUpdateCarrierId == null) {
				delivUpdateCarrierId = conn
						.prepareStatement("UPDATE oorder SET o_carrier_id = ?"
								+ " WHERE o_id = ?" + " AND o_d_id = ?"
								+ " AND o_w_id = ?");
			}
			delivUpdateCarrierId.setInt(1, o_carrier_id);
			delivUpdateCarrierId.setInt(2, no_o_id);
			delivUpdateCarrierId.setInt(3, d_id);
			delivUpdateCarrierId.setInt(4, w_id);
			result = delivUpdateCarrierId.executeUpdate();

			if (result != 1)
				throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID="
						+ d_id + " O_W_ID=" + w_id + " not found!");

			if (delivUpdateDeliveryDate == null) {
				delivUpdateDeliveryDate = conn
						.prepareStatement("UPDATE order_line SET ol_delivery_d = ?"
								+ " WHERE ol_o_id = ?"
								+ " AND ol_d_id = ?"
								+ " AND ol_w_id = ?");
			}
			delivUpdateDeliveryDate.setTimestamp(1,
					new Timestamp(System.currentTimeMillis()));
			delivUpdateDeliveryDate.setInt(2, no_o_id);
			delivUpdateDeliveryDate.setInt(3, d_id);
			delivUpdateDeliveryDate.setInt(4, w_id);
			result = delivUpdateDeliveryDate.executeUpdate();

			if (result == 0)
				throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID="
						+ d_id + " OL_W_ID=" + w_id + " not found!");

			if (delivSumOrderAmount == null) {
				delivSumOrderAmount = conn
						.prepareStatement("SELECT SUM(ol_amount) AS ol_total"
								+ " FROM order_line" + " WHERE ol_o_id = ?"
								+ " AND ol_d_id = ?" + " AND ol_w_id = ?");
			}
			delivSumOrderAmount.setInt(1, no_o_id);
			delivSumOrderAmount.setInt(2, d_id);
			delivSumOrderAmount.setInt(3, w_id);
			rs = delivSumOrderAmount.executeQuery();

			if (!rs.next())
				throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID="
						+ d_id + " OL_W_ID=" + w_id + " not found!");
			ol_total = rs.getFloat("ol_total");
			rs.close();
			rs = null;

			if (delivUpdateCustBalDelivCnt == null) {
				delivUpdateCustBalDelivCnt = conn
						.prepareStatement("UPDATE customer SET c_balance = c_balance + ?"
								+ ", c_delivery_cnt = c_delivery_cnt + 1"
								+ " WHERE c_w_id = ?"
								+ " AND c_d_id = ?"
								+ " AND c_id = ?");
			}
			delivUpdateCustBalDelivCnt.setFloat(1, ol_total);
			delivUpdateCustBalDelivCnt.setInt(2, w_id);
			delivUpdateCustBalDelivCnt.setInt(3, d_id);
			delivUpdateCustBalDelivCnt.setInt(4, c_id);
			result = delivUpdateCustBalDelivCnt.executeUpdate();

			if (result == 0)
				throw new RuntimeException("C_ID=" + c_id + " C_W_ID=" + w_id
						+ " C_D_ID=" + d_id + " not found!");
		}

		conn.commit();

		StringBuilder terminalMessage = new StringBuilder();
		terminalMessage
				.append("\n+---------------------------- DELIVERY ---------------------------+\n");
		terminalMessage.append(" Date: ");
		terminalMessage.append(TPCCUtil.getCurrentTime());
		terminalMessage.append("\n\n Warehouse: ");
		terminalMessage.append(w_id);
		terminalMessage.append("\n Carrier:   ");
		terminalMessage.append(o_carrier_id);
		terminalMessage.append("\n\n Delivered Orders\n");
		int skippedDeliveries = 0;
		for (int i = 1; i <= 10; i++) {
			if (orderIDs[i - 1] >= 0) {
				terminalMessage.append("  District ");
				terminalMessage.append(i < 10 ? " " : "");
				terminalMessage.append(i);
				terminalMessage.append(": Order number ");
				terminalMessage.append(orderIDs[i - 1]);
				terminalMessage.append(" was delivered.\n");
			} else {
				terminalMessage.append("  District ");
				terminalMessage.append(i < 10 ? " " : "");
				terminalMessage.append(i);
				terminalMessage.append(": No orders to be delivered.\n");
				skippedDeliveries++;
			}
		}
		terminalMessage
				.append("+-----------------------------------------------------------------+\n\n");
		terminalMessage(terminalMessage.toString());

		return skippedDeliveries;
	}

	private void orderStatusTransaction(int w_id, int d_id, int c_id,
			String c_last, boolean c_by_name) throws SQLException {
		int o_id = -1, o_carrier_id = -1;
		Timestamp entdate;
		ArrayList<String> orderLines = new ArrayList<String>();

		Customer c;
		if (c_by_name) {
			assert c_id <= 0;
			// TODO: This only needs c_balance, c_first, c_middle, c_id
			// only fetch those columns?
			c = getCustomerByName(w_id, d_id, c_last);
		} else {
			assert c_last == null;
			c = getCustomerById(w_id, d_id, c_id);
		}

		// find the newest order for the customer
		// retrieve the carrier & order date for the most recent order.

		if (ordStatGetNewestOrd == null) {
			ordStatGetNewestOrd = conn
					.prepareStatement("SELECT o_id, o_carrier_id, o_entry_d FROM oorder"
							+ " WHERE o_w_id = ?"
							+ " AND o_d_id = ? AND ROWNUM = 1"
							+ " AND o_c_id = ? ORDER BY o_id DESC");
		}
		ordStatGetNewestOrd.setInt(1, w_id);
		ordStatGetNewestOrd.setInt(2, d_id);
		ordStatGetNewestOrd.setInt(3, c.c_id);
		rs = ordStatGetNewestOrd.executeQuery();

		if (!rs.next()) {
			throw new RuntimeException("No orders for o_w_id=" + w_id
					+ " o_d_id=" + d_id + " o_c_id=" + c.c_id);
		}

		o_id = rs.getInt("o_id");
		o_carrier_id = rs.getInt("o_carrier_id");
		entdate = rs.getTimestamp("o_entry_d");
		rs.close();
		rs = null;

		// retrieve the order lines for the most recent order

		if (ordStatGetOrderLines == null) {
			ordStatGetOrderLines = conn
					.prepareStatement("SELECT ol_i_id, ol_supply_w_id, ol_quantity,"
							+ " ol_amount, ol_delivery_d"
							+ " FROM order_line"
							+ " WHERE ol_o_id = ?"
							+ " AND ol_d_id =?"
							+ " AND ol_w_id = ?");
		}
		ordStatGetOrderLines.setInt(1, o_id);
		ordStatGetOrderLines.setInt(2, d_id);
		ordStatGetOrderLines.setInt(3, w_id);
		rs = ordStatGetOrderLines.executeQuery();

		while (rs.next()) {
			StringBuilder orderLine = new StringBuilder();
			orderLine.append("[");
			orderLine.append(rs.getLong("ol_supply_w_id"));
			orderLine.append(" - ");
			orderLine.append(rs.getLong("ol_i_id"));
			orderLine.append(" - ");
			orderLine.append(rs.getLong("ol_quantity"));
			orderLine.append(" - ");
			orderLine.append(TPCCUtil.formattedDouble(rs
					.getDouble("ol_amount")));
			orderLine.append(" - ");
			if (rs.getTimestamp("ol_delivery_d") != null)
				orderLine.append(rs.getTimestamp("ol_delivery_d"));
			else
				orderLine.append("99-99-9999");
			orderLine.append("]");
			orderLines.add(orderLine.toString());
		}
		rs.close();
		rs = null;

		// commit the transaction
		conn.commit();

		StringBuilder terminalMessage = new StringBuilder();
		terminalMessage.append("\n");
		terminalMessage
				.append("+-------------------------- ORDER-STATUS -------------------------+\n");
		terminalMessage.append(" Date: ");
		terminalMessage.append(TPCCUtil.getCurrentTime());
		terminalMessage.append("\n\n Warehouse: ");
		terminalMessage.append(w_id);
		terminalMessage.append("\n District:  ");
		terminalMessage.append(d_id);
		terminalMessage.append("\n\n Customer:  ");
		terminalMessage.append(c.c_id);
		terminalMessage.append("\n   Name:    ");
		terminalMessage.append(c.c_first);
		terminalMessage.append(" ");
		terminalMessage.append(c.c_middle);
		terminalMessage.append(" ");
		terminalMessage.append(c.c_last);
		terminalMessage.append("\n   Balance: ");
		terminalMessage.append(c.c_balance);
		terminalMessage.append("\n\n");
		if (o_id == -1) {
			terminalMessage.append(" Customer has no orders placed.\n");
		} else {
			terminalMessage.append(" Order-Number: ");
			terminalMessage.append(o_id);
			terminalMessage.append("\n    Entry-Date: ");
			terminalMessage.append(entdate);
			terminalMessage.append("\n    Carrier-Number: ");
			terminalMessage.append(o_carrier_id);
			terminalMessage.append("\n\n");
			if (orderLines.size() != 0) {
				terminalMessage
						.append(" [Supply_W - Item_ID - Qty - Amount - Delivery-Date]\n");
				for (String orderLine : orderLines) {
					terminalMessage.append(" ");
					terminalMessage.append(orderLine);
					terminalMessage.append("\n");
				}
			} else {
				terminalMessage(" This Order has no Order-Lines.\n");
			}
		}
		terminalMessage
				.append("+-----------------------------------------------------------------+\n\n");
		terminalMessage(terminalMessage.toString());
	}

	private void stockLevelTransaction(int w_id, int d_id, int threshold)
			throws SQLException {
		int o_id = 0;
		// XXX int i_id = 0;
		int stock_count = 0;

		// XXX District dist = new District();
		// XXX OrderLine orln = new OrderLine();
		// XXX Stock stck = new Stock();



		if (stockGetDistOrderId == null) {
			stockGetDistOrderId = conn.prepareStatement("SELECT d_next_o_id"
					+ " FROM district" + " WHERE d_w_id = ?" + " AND d_id = ?");
		}
		stockGetDistOrderId.setInt(1, w_id);
		stockGetDistOrderId.setInt(2, d_id);
		rs = stockGetDistOrderId.executeQuery();

		if (!rs.next())
			throw new RuntimeException("D_W_ID=" + w_id + " D_ID=" + d_id
					+ " not found!");
		o_id = rs.getInt("d_next_o_id");
		rs.close();
		rs = null;

		if (stockGetCountStock == null) {
			stockGetCountStock = conn
					.prepareStatement("SELECT COUNT(DISTINCT (s_i_id)) AS stock_count"
							+ " FROM order_line, stock"
							+ " WHERE ol_w_id = ?"
							+ " AND ol_d_id = ?"
							+ " AND ol_o_id < ?"
							+ " AND ol_o_id >= ? - 20"
							+ " AND s_w_id = ?"
							+ " AND s_i_id = ol_i_id" + " AND s_quantity < ?");
		}
		stockGetCountStock.setInt(1, w_id);
		stockGetCountStock.setInt(2, d_id);
		stockGetCountStock.setInt(3, o_id);
		stockGetCountStock.setInt(4, o_id);
		stockGetCountStock.setInt(5, w_id);
		stockGetCountStock.setInt(6, threshold);
		rs = stockGetCountStock.executeQuery();

		if (!rs.next())
			throw new RuntimeException("OL_W_ID=" + w_id + " OL_D_ID=" + d_id
					+ " OL_O_ID=" + o_id + " (...) not found!");
		stock_count = rs.getInt("stock_count");

		conn.commit();

		rs.close();
		rs = null;

		StringBuilder terminalMessage = new StringBuilder();
		terminalMessage
				.append("\n+-------------------------- STOCK-LEVEL --------------------------+");
		terminalMessage.append("\n Warehouse: ");
		terminalMessage.append(w_id);
		terminalMessage.append("\n District:  ");
		terminalMessage.append(d_id);
		terminalMessage.append("\n\n Stock Level Threshold: ");
		terminalMessage.append(threshold);
		terminalMessage.append("\n Low Stock Count:       ");
		terminalMessage.append(stock_count);
		terminalMessage
				.append("\n+-----------------------------------------------------------------+\n\n");
		terminalMessage(terminalMessage.toString());
	}

	private Customer newCustomerFromResults(ResultSet rs) throws SQLException {
		Customer c = new Customer();
		// TODO: Use column indices: probably faster?
		c.c_first = rs.getString("c_first");
		c.c_middle = rs.getString("c_middle");
		c.c_street_1 = rs.getString("c_street_1");
		c.c_street_2 = rs.getString("c_street_2");
		c.c_city = rs.getString("c_city");
		c.c_state = rs.getString("c_state");
		c.c_zip = rs.getString("c_zip");
		c.c_phone = rs.getString("c_phone");
		c.c_credit = rs.getString("c_credit");
		c.c_credit_lim = rs.getFloat("c_credit_lim");
		c.c_discount = rs.getFloat("c_discount");
		c.c_balance = rs.getFloat("c_balance");
		c.c_ytd_payment = rs.getFloat("c_ytd_payment");
		c.c_payment_cnt = rs.getInt("c_payment_cnt");
		c.c_since = rs.getTimestamp("c_since");
		return c;
	}

	PreparedStatement customerByName;


	private Customer getCustomerByName(int c_w_id, int c_d_id, String c_last)
			throws SQLException {
		ArrayList<Customer> customers = new ArrayList<Customer>();
		if (customerByName == null) {
			customerByName = conn
					.prepareStatement("SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, "
							+ "c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, "
							+ "c_balance, c_ytd_payment, c_payment_cnt, c_since FROM customer "
							+ "WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first");
		}
		customerByName.setInt(1, c_w_id);
		customerByName.setInt(2, c_d_id);
		customerByName.setString(3, c_last);
		ResultSet rs = customerByName.executeQuery();

		while (rs.next()) {
			Customer c = newCustomerFromResults(rs);
			c.c_id = rs.getInt("c_id");
			c.c_last = c_last;
			customers.add(c);
		}
		rs.close();

		if (customers.size() == 0) {
			throw new RuntimeException("C_LAST=" + c_last + " C_D_ID=" + c_d_id
					+ " C_W_ID=" + c_w_id + " not found!");
		}

		// TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
		// that
		// counts starting from 1.
		int index = customers.size() / 2;
		if (customers.size() % 2 == 0) {
			index -= 1;
		}
		return customers.get(index);
	}

	private Customer getCustomerById(int c_w_id, int c_d_id, int c_id)
			throws SQLException {
		if (payGetCust == null) {
			payGetCust = conn
					.prepareStatement("SELECT c_first, c_middle, c_last, c_street_1, c_street_2, "
							+ "c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, "
							+ "c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_since FROM customer WHERE "
							+ "c_w_id = ? AND c_d_id = ? AND c_id = ?");
		}
		payGetCust.setInt(1, c_w_id);
		payGetCust.setInt(2, c_d_id);
		payGetCust.setInt(3, c_id);
		ResultSet rs = payGetCust.executeQuery();
		if (!rs.next()) {
			throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id
					+ " C_W_ID=" + c_w_id + " not found!");
		}

		Customer c = newCustomerFromResults(rs);
		c.c_id = c_id;
		c.c_last = rs.getString("c_last");
		rs.close();
		return c;
	}

	private void paymentTransaction(int w_id, int c_w_id, float h_amount,
			int d_id, int c_d_id, int c_id, String c_last, boolean c_by_name)
			throws SQLException {
		String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
		String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;

		if (payUpdateWhse == null) {
			payUpdateWhse = conn
					.prepareStatement("UPDATE warehouse SET w_ytd = w_ytd + ?  WHERE w_id = ? ");
		}
		payUpdateWhse.setFloat(1, h_amount);
		payUpdateWhse.setInt(2, w_id);
		// MySQL reports deadlocks due to lock upgrades:
		// t1: read w_id = x; t2: update w_id = x; t1 update w_id = x
		result = payUpdateWhse.executeUpdate();
		if (result == 0)
			throw new RuntimeException("W_ID=" + w_id + " not found!");

		if (payGetWhse == null) {
			payGetWhse = conn
					.prepareStatement("SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name"
							+ " FROM warehouse WHERE w_id = ?");
		}
		payGetWhse.setInt(1, w_id);
		rs = payGetWhse.executeQuery();
		if (!rs.next())
			throw new RuntimeException("W_ID=" + w_id + " not found!");
		w_street_1 = rs.getString("w_street_1");
		w_street_2 = rs.getString("w_street_2");
		w_city = rs.getString("w_city");
		w_state = rs.getString("w_state");
		w_zip = rs.getString("w_zip");
		w_name = rs.getString("w_name");
		rs.close();
		rs = null;

		if (payUpdateDist == null) {
			payUpdateDist = conn
					.prepareStatement("UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?");
		}
		payUpdateDist.setFloat(1, h_amount);
		payUpdateDist.setInt(2, w_id);
		payUpdateDist.setInt(3, d_id);
		result = payUpdateDist.executeUpdate();
		if (result == 0)
			throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id
					+ " not found!");

		if (payGetDist == null) {
			payGetDist = conn
					.prepareStatement("SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name"
							+ " FROM district WHERE d_w_id = ? AND d_id = ?");
		}
		payGetDist.setInt(1, w_id);
		payGetDist.setInt(2, d_id);
		rs = payGetDist.executeQuery();
		if (!rs.next())
			throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id
					+ " not found!");
		d_street_1 = rs.getString("d_street_1");
		d_street_2 = rs.getString("d_street_2");
		d_city = rs.getString("d_city");
		d_state = rs.getString("d_state");
		d_zip = rs.getString("d_zip");
		d_name = rs.getString("d_name");
		rs.close();
		rs = null;

		Customer c;
		if (c_by_name) {
			assert c_id <= 0;
			c = getCustomerByName(c_w_id, c_d_id, c_last);
		} else {
			assert c_last == null;
			c = getCustomerById(c_w_id, c_d_id, c_id);
		}

		c.c_balance -= h_amount;
		c.c_ytd_payment += h_amount;
		c.c_payment_cnt += 1;
		String c_data = null;
		if (c.c_credit.equals("BC")) { // bad credit

			if (payGetCustCdata == null) {
				payGetCustCdata = conn
						.prepareStatement("SELECT c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
			}
			payGetCustCdata.setInt(1, c_w_id);
			payGetCustCdata.setInt(2, c_d_id);
			payGetCustCdata.setInt(3, c.c_id);
			rs = payGetCustCdata.executeQuery();
			if (!rs.next())
				throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID="
						+ c_w_id + " C_D_ID=" + c_d_id + " not found!");
			c_data = rs.getString("c_data");
			rs.close();
			rs = null;

			c_data = c.c_id + " " + c_d_id + " " + c_w_id + " " + d_id + " "
					+ w_id + " " + h_amount + " | " + c_data;
			if (c_data.length() > 500)
				c_data = c_data.substring(0, 500);

			if (payUpdateCustBalCdata == null) {
				payUpdateCustBalCdata = conn
						.prepareStatement("UPDATE customer SET c_balance = ?, c_ytd_payment = ?, "
								+ "c_payment_cnt = ?, c_data = ? "
								+ "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
			}
			payUpdateCustBalCdata.setFloat(1, c.c_balance);
			payUpdateCustBalCdata.setFloat(2, c.c_ytd_payment);
			payUpdateCustBalCdata.setInt(3, c.c_payment_cnt);
			payUpdateCustBalCdata.setString(4, c_data);
			payUpdateCustBalCdata.setInt(5, c_w_id);
			payUpdateCustBalCdata.setInt(6, c_d_id);
			payUpdateCustBalCdata.setInt(7, c.c_id);
			result = payUpdateCustBalCdata.executeUpdate();

			if (result == 0)
				throw new RuntimeException(
						"Error in PYMNT Txn updating Customer C_ID=" + c.c_id
								+ " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id);

		} else { // GoodCredit

			if (payUpdateCustBal == null) {
				payUpdateCustBal = conn
						.prepareStatement("UPDATE customer SET c_balance = ?, c_ytd_payment = ?, "
								+ "c_payment_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
			}
			payUpdateCustBal.setFloat(1, c.c_balance);
			payUpdateCustBal.setFloat(2, c.c_ytd_payment);
			payUpdateCustBal.setFloat(3, c.c_payment_cnt);
			payUpdateCustBal.setInt(4, c_w_id);
			payUpdateCustBal.setInt(5, c_d_id);
			payUpdateCustBal.setInt(6, c.c_id);
			result = payUpdateCustBal.executeUpdate();

			if (result == 0)
				throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID="
						+ c_w_id + " C_D_ID=" + c_d_id + " not found!");

		}

		if (w_name.length() > 10)
			w_name = w_name.substring(0, 10);
		if (d_name.length() > 10)
			d_name = d_name.substring(0, 10);
		String h_data = w_name + "    " + d_name;

		if (payInsertHist == null) {
			payInsertHist = conn
					.prepareStatement("INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) "
							+ " VALUES (?,?,?,?,?,?,?,?)");
		}
		payInsertHist.setInt(1, c_d_id);
		payInsertHist.setInt(2, c_w_id);
		payInsertHist.setInt(3, c.c_id);
		payInsertHist.setInt(4, d_id);
		payInsertHist.setInt(5, w_id);
		payInsertHist
				.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
		payInsertHist.setFloat(7, h_amount);
		payInsertHist.setString(8, h_data);
		payInsertHist.executeUpdate();

		conn.commit();

		StringBuilder terminalMessage = new StringBuilder();
		terminalMessage
				.append("\n+---------------------------- PAYMENT ----------------------------+");
		terminalMessage.append("\n Date: " + TPCCUtil.getCurrentTime());
		terminalMessage.append("\n\n Warehouse: ");
		terminalMessage.append(w_id);
		terminalMessage.append("\n   Street:  ");
		terminalMessage.append(w_street_1);
		terminalMessage.append("\n   Street:  ");
		terminalMessage.append(w_street_2);
		terminalMessage.append("\n   City:    ");
		terminalMessage.append(w_city);
		terminalMessage.append("   State: ");
		terminalMessage.append(w_state);
		terminalMessage.append("  Zip: ");
		terminalMessage.append(w_zip);
		terminalMessage.append("\n\n District:  ");
		terminalMessage.append(d_id);
		terminalMessage.append("\n   Street:  ");
		terminalMessage.append(d_street_1);
		terminalMessage.append("\n   Street:  ");
		terminalMessage.append(d_street_2);
		terminalMessage.append("\n   City:    ");
		terminalMessage.append(d_city);
		terminalMessage.append("   State: ");
		terminalMessage.append(d_state);
		terminalMessage.append("  Zip: ");
		terminalMessage.append(d_zip);
		terminalMessage.append("\n\n Customer:  ");
		terminalMessage.append(c.c_id);
		terminalMessage.append("\n   Name:    ");
		terminalMessage.append(c.c_first);
		terminalMessage.append(" ");
		terminalMessage.append(c.c_middle);
		terminalMessage.append(" ");
		terminalMessage.append(c.c_last);
		terminalMessage.append("\n   Street:  ");
		terminalMessage.append(c.c_street_1);
		terminalMessage.append("\n   Street:  ");
		terminalMessage.append(c.c_street_2);
		terminalMessage.append("\n   City:    ");
		terminalMessage.append(c.c_city);
		terminalMessage.append("   State: ");
		terminalMessage.append(c.c_state);
		terminalMessage.append("  Zip: ");
		terminalMessage.append(c.c_zip);
		terminalMessage.append("\n   Since:   ");
		if (c.c_since != null) {
			terminalMessage.append(c.c_since.toString());
		} else {
			terminalMessage.append("");
		}
		terminalMessage.append("\n   Credit:  ");
		terminalMessage.append(c.c_credit);
		terminalMessage.append("\n   %Disc:   ");
		terminalMessage.append(c.c_discount);
		terminalMessage.append("\n   Phone:   ");
		terminalMessage.append(c.c_phone);
		terminalMessage.append("\n\n Amount Paid:      ");
		terminalMessage.append(h_amount);
		terminalMessage.append("\n Credit Limit:     ");
		terminalMessage.append(c.c_credit_lim);
		terminalMessage.append("\n New Cust-Balance: ");
		terminalMessage.append(c.c_balance);
		if (c.c_credit.equals("BC")) {
			if (c_data.length() > 50) {
				terminalMessage.append("\n\n Cust-Data: "
						+ c_data.substring(0, 50));
				int data_chunks = c_data.length() > 200 ? 4
						: c_data.length() / 50;
				for (int n = 1; n < data_chunks; n++)
					terminalMessage.append("\n            "
							+ c_data.substring(n * 50, (n + 1) * 50));
			} else {
				terminalMessage.append("\n\n Cust-Data: " + c_data);
			}
		}
		terminalMessage
				.append("\n+-----------------------------------------------------------------+\n\n");
		terminalMessage(terminalMessage.toString());

	}

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
