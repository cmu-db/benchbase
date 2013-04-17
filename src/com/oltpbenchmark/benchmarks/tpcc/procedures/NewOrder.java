package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;

public class NewOrder extends TPCCProcedure {
    
    private static final Logger LOG = Logger.getLogger(NewOrder.class);

    public final SQLStmt stmtGetCustWhseSQL = new SQLStmt(
    		"SELECT c_discount, c_last, c_credit, w_tax"
			+ "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + ", " + TPCCConstants.TABLENAME_WAREHOUSE
			+ " WHERE w_id = ? AND c_w_id = ?"
			+ " AND c_d_id = ? AND c_id = ?");
    
    public final SQLStmt stmtGetDistSQL = new SQLStmt(
    		"SELECT d_next_o_id, d_tax FROM " + TPCCConstants.TABLENAME_DISTRICT
					+ " WHERE d_w_id = ? AND d_id = ? FOR UPDATE"
    				);
    
	public final SQLStmt  stmtInsertNewOrderSQL = new SQLStmt("INSERT INTO "+ TPCCConstants.TABLENAME_NEWORDER + " (no_o_id, no_d_id, no_w_id) VALUES ( ?, ?, ?)");
	
	public final SQLStmt  stmtUpdateDistSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = ? AND d_id = ?");
	
	public final SQLStmt  stmtInsertOOrderSQL = new SQLStmt("INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER
			+ " (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)"
			+ " VALUES (?, ?, ?, ?, ?, ?, ?)");
	
	public final SQLStmt  stmtGetItemSQL = new SQLStmt("SELECT i_price, i_name , i_data FROM " + TPCCConstants.TABLENAME_ITEM + " WHERE i_id = ?");

	public final SQLStmt  stmtGetStockSQL = new SQLStmt("SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, "
			+ "       s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10"
			+ " FROM " + TPCCConstants.TABLENAME_STOCK + " WHERE s_i_id = ? AND s_w_id = ? FOR UPDATE");
	
	public final SQLStmt  stmtUpdateStockSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_STOCK + " SET s_quantity = ? , s_ytd = s_ytd + ?, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + ? "
			+ " WHERE s_i_id = ? AND s_w_id = ?");
	
	public final SQLStmt  stmtInsertOrderLineSQL = new SQLStmt("INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE + " (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id,"
			+ "  ol_quantity, ol_amount, ol_dist_info) VALUES (?,?,?,?,?,?,?,?,?)");
    

	// NewOrder Txn
	private PreparedStatement stmtGetCustWhse = null; 
	private PreparedStatement stmtGetDist = null;
	private PreparedStatement stmtInsertNewOrder = null;
	private PreparedStatement stmtUpdateDist = null;
	private PreparedStatement stmtInsertOOrder = null;
	private PreparedStatement stmtGetItem = null;
	private PreparedStatement stmtGetStock = null;
	private PreparedStatement stmtUpdateStock = null;
	private PreparedStatement stmtInsertOrderLine = null;
	
    
    public ResultSet run(Connection conn, Random gen,
			int terminalWarehouseID, int numWarehouses,
			int terminalDistrictLowerID, int terminalDistrictUpperID,
			TPCCWorker w) throws SQLException {
    
    	
		
		//initializing all prepared statements
		stmtGetCustWhse=this.getPreparedStatement(conn, stmtGetCustWhseSQL);
		stmtGetDist=this.getPreparedStatement(conn, stmtGetDistSQL);
		stmtInsertNewOrder=this.getPreparedStatement(conn, stmtInsertNewOrderSQL);
		stmtUpdateDist =this.getPreparedStatement(conn, stmtUpdateDistSQL);
		stmtInsertOOrder =this.getPreparedStatement(conn, stmtInsertOOrderSQL);
		stmtGetItem =this.getPreparedStatement(conn, stmtGetItemSQL);
		stmtGetStock =this.getPreparedStatement(conn, stmtGetStockSQL);
		stmtUpdateStock =this.getPreparedStatement(conn, stmtUpdateStockSQL);
		stmtInsertOrderLine =this.getPreparedStatement(conn, stmtInsertOrderLineSQL);
    	
    	
		int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
		int customerID = TPCCUtil.getCustomerID(gen);

		int numItems = (int) TPCCUtil.randomNumber(5, 15, gen);
		int[] itemIDs = new int[numItems];
		int[] supplierWarehouseIDs = new int[numItems];
		int[] orderQuantities = new int[numItems];
		int allLocal = 1;
		for (int i = 0; i < numItems; i++) {
			itemIDs[i] = TPCCUtil.getItemID(gen);
			if (TPCCUtil.randomNumber(1, 100, gen) > 1) {
				supplierWarehouseIDs[i] = terminalWarehouseID;
			} else {
				do {
					supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1,
							numWarehouses, gen);
				} while (supplierWarehouseIDs[i] == terminalWarehouseID
						&& numWarehouses > 1);
				allLocal = 0;
			}
			orderQuantities[i] = TPCCUtil.randomNumber(1, 10, gen);
		}

		// we need to cause 1% of the new orders to be rolled back.
		if (TPCCUtil.randomNumber(1, 100, gen) == 1)
			itemIDs[numItems - 1] = jTPCCConfig.INVALID_ITEM_ID;

		
		newOrderTransaction(terminalWarehouseID, districtID,
						customerID, numItems, allLocal, itemIDs,
						supplierWarehouseIDs, orderQuantities, conn, w);
		return null;
    
    }
    
	


	private void newOrderTransaction(int w_id, int d_id, int c_id,
			int o_ol_cnt, int o_all_local, int[] itemIDs,
			int[] supplierWarehouseIDs, int[] orderQuantities, Connection conn, TPCCWorker w)
			throws SQLException {
		float c_discount, w_tax, d_tax = 0, i_price;
		int d_next_o_id, o_id = -1, s_quantity;
		String c_last = null, c_credit = null, i_name, i_data, s_data;
		String s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05;
		String s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, ol_dist_info = null;
		float[] itemPrices = new float[o_ol_cnt];
		float[] orderLineAmounts = new float[o_ol_cnt];
		String[] itemNames = new String[o_ol_cnt];
		int[] stockQuantities = new int[o_ol_cnt];
		char[] brandGeneric = new char[o_ol_cnt];
		int ol_supply_w_id, ol_i_id, ol_quantity;
		int s_remote_cnt_increment;
		float ol_amount, total_amount = 0;
		try
		{
			stmtGetCustWhse.setInt(1, w_id);
			stmtGetCustWhse.setInt(2, w_id);
			stmtGetCustWhse.setInt(3, d_id);
			stmtGetCustWhse.setInt(4, c_id);
			ResultSet rs = stmtGetCustWhse.executeQuery();
			if (!rs.next())
				throw new RuntimeException("W_ID=" + w_id + " C_D_ID=" + d_id
						+ " C_ID=" + c_id + " not found!");
			c_discount = rs.getFloat("c_discount");
			c_last = rs.getString("c_last");
			c_credit = rs.getString("c_credit");
			w_tax = rs.getFloat("w_tax");
			rs.close();
			rs = null;


			stmtGetDist.setInt(1, w_id);
			stmtGetDist.setInt(2, d_id);
			rs = stmtGetDist.executeQuery();
			if (!rs.next()) {
				throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id
						+ " not found!");
			}
			d_next_o_id = rs.getInt("d_next_o_id");
			d_tax = rs.getFloat("d_tax");
			rs.close();
			rs = null;
			o_id = d_next_o_id;


			stmtInsertNewOrder.setInt(1, o_id);
			stmtInsertNewOrder.setInt(2, d_id);
			stmtInsertNewOrder.setInt(3, w_id);
			stmtInsertNewOrder.executeUpdate();

			stmtUpdateDist.setInt(1, w_id);
			stmtUpdateDist.setInt(2, d_id);
			int result = stmtUpdateDist.executeUpdate();
			if (result == 0)
				throw new RuntimeException(
						"Error!! Cannot update next_order_id on district for D_ID="
								+ d_id + " D_W_ID=" + w_id);

			stmtInsertOOrder.setInt(1, o_id);
			stmtInsertOOrder.setInt(2, d_id);
			stmtInsertOOrder.setInt(3, w_id);
			stmtInsertOOrder.setInt(4, c_id);
			stmtInsertOOrder.setTimestamp(5,
					new Timestamp(System.currentTimeMillis()));
			stmtInsertOOrder.setInt(6, o_ol_cnt);
			stmtInsertOOrder.setInt(7, o_all_local);
			stmtInsertOOrder.executeUpdate();

			for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
				ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
				ol_i_id = itemIDs[ol_number - 1];
				ol_quantity = orderQuantities[ol_number - 1];
				stmtGetItem.setInt(1, ol_i_id);
				rs = stmtGetItem.executeQuery();
				if (!rs.next()) {
					// This is (hopefully) an expected error: this is an
					// expected new order rollback
					assert ol_number == o_ol_cnt;
					assert ol_i_id == jTPCCConfig.INVALID_ITEM_ID;
					rs.close();
					throw new UserAbortException(
							"EXPECTED new order rollback: I_ID=" + ol_i_id
									+ " not found!");
				}

				i_price = rs.getFloat("i_price");
				i_name = rs.getString("i_name");
				i_data = rs.getString("i_data");
				rs.close();
				rs = null;

				itemPrices[ol_number - 1] = i_price;
				itemNames[ol_number - 1] = i_name;

	
				stmtGetStock.setInt(1, ol_i_id);
				stmtGetStock.setInt(2, ol_supply_w_id);
				rs = stmtGetStock.executeQuery();
				if (!rs.next())
					throw new RuntimeException("I_ID=" + ol_i_id
							+ " not found!");
				s_quantity = rs.getInt("s_quantity");
				s_data = rs.getString("s_data");
				s_dist_01 = rs.getString("s_dist_01");
				s_dist_02 = rs.getString("s_dist_02");
				s_dist_03 = rs.getString("s_dist_03");
				s_dist_04 = rs.getString("s_dist_04");
				s_dist_05 = rs.getString("s_dist_05");
				s_dist_06 = rs.getString("s_dist_06");
				s_dist_07 = rs.getString("s_dist_07");
				s_dist_08 = rs.getString("s_dist_08");
				s_dist_09 = rs.getString("s_dist_09");
				s_dist_10 = rs.getString("s_dist_10");
				rs.close();
				rs = null;

				stockQuantities[ol_number - 1] = s_quantity;

				if (s_quantity - ol_quantity >= 10) {
					s_quantity -= ol_quantity;
				} else {
					s_quantity += -ol_quantity + 91;
				}

				if (ol_supply_w_id == w_id) {
					s_remote_cnt_increment = 0;
				} else {
					s_remote_cnt_increment = 1;
				}

		
				stmtUpdateStock.setInt(1, s_quantity);
				stmtUpdateStock.setInt(2, ol_quantity);
				stmtUpdateStock.setInt(3, s_remote_cnt_increment);
				stmtUpdateStock.setInt(4, ol_i_id);
				stmtUpdateStock.setInt(5, ol_supply_w_id);
				stmtUpdateStock.addBatch();

				ol_amount = ol_quantity * i_price;
				orderLineAmounts[ol_number - 1] = ol_amount;
				total_amount += ol_amount;

				if (i_data.indexOf("GENERIC") != -1
						&& s_data.indexOf("GENERIC") != -1) {
					brandGeneric[ol_number - 1] = 'B';
				} else {
					brandGeneric[ol_number - 1] = 'G';
				}

				switch ((int) d_id) {
				case 1:
					ol_dist_info = s_dist_01;
					break;
				case 2:
					ol_dist_info = s_dist_02;
					break;
				case 3:
					ol_dist_info = s_dist_03;
					break;
				case 4:
					ol_dist_info = s_dist_04;
					break;
				case 5:
					ol_dist_info = s_dist_05;
					break;
				case 6:
					ol_dist_info = s_dist_06;
					break;
				case 7:
					ol_dist_info = s_dist_07;
					break;
				case 8:
					ol_dist_info = s_dist_08;
					break;
				case 9:
					ol_dist_info = s_dist_09;
					break;
				case 10:
					ol_dist_info = s_dist_10;
					break;
				}

				stmtInsertOrderLine.setInt(1, o_id);
				stmtInsertOrderLine.setInt(2, d_id);
				stmtInsertOrderLine.setInt(3, w_id);
				stmtInsertOrderLine.setInt(4, ol_number);
				stmtInsertOrderLine.setInt(5, ol_i_id);
				stmtInsertOrderLine.setInt(6, ol_supply_w_id);
				stmtInsertOrderLine.setInt(7, ol_quantity);
				stmtInsertOrderLine.setFloat(8, ol_amount);
				stmtInsertOrderLine.setString(9, ol_dist_info);
				stmtInsertOrderLine.addBatch();

			} // end-for

			stmtInsertOrderLine.executeBatch();
			stmtUpdateStock.executeBatch();

			total_amount *= (1 + w_tax + d_tax) * (1 - c_discount);
		} catch(UserAbortException userEx)
		{
		    LOG.debug("Caught an expected error in New Order");
		    throw userEx;
		}
	    finally {
            if (stmtInsertOrderLine != null)
                stmtInsertOrderLine.clearBatch();
              if (stmtUpdateStock != null)
                stmtUpdateStock.clearBatch();
        }

	}
    
}
