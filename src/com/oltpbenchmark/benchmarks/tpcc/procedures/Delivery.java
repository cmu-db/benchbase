package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Random;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;

public class Delivery extends TPCCProcedure {

	
	public SQLStmt delivGetOrderIdSQL = new SQLStmt("SELECT no_o_id FROM " + TPCCConstants.TABLENAME_NEWORDER + " WHERE no_d_id = ?"
			+ " AND no_w_id = ? ORDER BY no_o_id ASC LIMIT 1");
	public SQLStmt delivDeleteNewOrderSQL = new SQLStmt("DELETE FROM " + TPCCConstants.TABLENAME_NEWORDER + ""
			+ " WHERE no_o_id = ? AND no_d_id = ?"
			+ " AND no_w_id = ?");
	public SQLStmt delivGetCustIdSQL = new SQLStmt("SELECT o_c_id"
			+ " FROM " + TPCCConstants.TABLENAME_OPENORDER + " WHERE o_id = ?"
			+ " AND o_d_id = ?" + " AND o_w_id = ?");
	public SQLStmt delivUpdateCarrierIdSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_OPENORDER + " SET o_carrier_id = ?"
			+ " WHERE o_id = ?" + " AND o_d_id = ?"
			+ " AND o_w_id = ?");
	public SQLStmt delivUpdateDeliveryDateSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_ORDERLINE + " SET ol_delivery_d = ?"
			+ " WHERE ol_o_id = ?"
			+ " AND ol_d_id = ?"
			+ " AND ol_w_id = ?");
	public SQLStmt delivSumOrderAmountSQL = new SQLStmt("SELECT SUM(ol_amount) AS ol_total"
			+ " FROM " + TPCCConstants.TABLENAME_ORDERLINE + "" + " WHERE ol_o_id = ?"
			+ " AND ol_d_id = ?" + " AND ol_w_id = ?");
	public SQLStmt delivUpdateCustBalDelivCntSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET c_balance = c_balance + ?"
			+ ", c_delivery_cnt = c_delivery_cnt + 1"
			+ " WHERE c_w_id = ?"
			+ " AND c_d_id = ?"
			+ " AND c_id = ?");

	
	// Delivery Txn
	private PreparedStatement delivGetOrderId = null;
	private PreparedStatement delivDeleteNewOrder = null;
	private PreparedStatement delivGetCustId = null;
	private PreparedStatement delivUpdateCarrierId = null;
	private PreparedStatement delivUpdateDeliveryDate = null;
	private PreparedStatement delivSumOrderAmount = null;
	private PreparedStatement delivUpdateCustBalDelivCnt = null;

	
    public ResultSet run(Connection conn, Random gen,
			int terminalWarehouseID, int numWarehouses,
			int terminalDistrictLowerID, int terminalDistrictUpperID,
			TPCCWorker w) throws SQLException {
		int orderCarrierID = TPCCUtil.randomNumber(1, 10, gen);
	
		
		delivGetOrderId = this.getPreparedStatement(conn, delivGetOrderIdSQL);
		delivDeleteNewOrder =  this.getPreparedStatement(conn, delivDeleteNewOrderSQL);
		delivGetCustId = this.getPreparedStatement(conn, delivGetCustIdSQL);
		delivUpdateCarrierId = this.getPreparedStatement(conn, delivUpdateCarrierIdSQL);
		delivUpdateDeliveryDate = this.getPreparedStatement(conn, delivUpdateDeliveryDateSQL);
		delivSumOrderAmount = this.getPreparedStatement(conn, delivSumOrderAmountSQL);
		delivUpdateCustBalDelivCnt = this.getPreparedStatement(conn, delivUpdateCustBalDelivCntSQL);

		deliveryTransaction(terminalWarehouseID,orderCarrierID, conn, w);
		return null;
    }
    

	private int deliveryTransaction(int w_id, int o_carrier_id, Connection conn, TPCCWorker w) throws SQLException {
	    
	    int d_id, c_id;
		float ol_total;
		int[] orderIDs;

		orderIDs = new int[10];
		for (d_id = 1; d_id <= 10; d_id++) {


			delivGetOrderId.setInt(1, d_id);
			delivGetOrderId.setInt(2, w_id);
			ResultSet rs = delivGetOrderId.executeQuery();
			if (!rs.next()) {
				// This district has no new orders; this can happen but should
				// be rare
				continue;
			}

			int no_o_id = rs.getInt("no_o_id");
			orderIDs[d_id - 1] = no_o_id;
			rs.close();
			rs = null;

			delivDeleteNewOrder.setInt(1, no_o_id);
			delivDeleteNewOrder.setInt(2, d_id);
			delivDeleteNewOrder.setInt(3, w_id);
			int result = delivDeleteNewOrder.executeUpdate();
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
				throw new UserAbortException(
						"New order w_id="
								+ w_id
								+ " d_id="
								+ d_id
								+ " no_o_id="
								+ no_o_id
								+ " delete failed (not running with SERIALIZABLE isolation?)");
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


			delivUpdateCarrierId.setInt(1, o_carrier_id);
			delivUpdateCarrierId.setInt(2, no_o_id);
			delivUpdateCarrierId.setInt(3, d_id);
			delivUpdateCarrierId.setInt(4, w_id);
			result = delivUpdateCarrierId.executeUpdate();

			if (result != 1)
				throw new RuntimeException("O_ID=" + no_o_id + " O_D_ID="
						+ d_id + " O_W_ID=" + w_id + " not found!");


			delivUpdateDeliveryDate.setTimestamp(1,
					new Timestamp(System.currentTimeMillis()));
			delivUpdateDeliveryDate.setInt(2, no_o_id);
			delivUpdateDeliveryDate.setInt(3, d_id);
			delivUpdateDeliveryDate.setInt(4, w_id);
			result = delivUpdateDeliveryDate.executeUpdate();

			if (result == 0)
				throw new RuntimeException("OL_O_ID=" + no_o_id + " OL_D_ID="
						+ d_id + " OL_W_ID=" + w_id + " not found!");

			

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

		//TODO: This part is not used
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
		terminalMessage.append("+-----------------------------------------------------------------+\n\n");

		return skippedDeliveries;
	}


    
}
