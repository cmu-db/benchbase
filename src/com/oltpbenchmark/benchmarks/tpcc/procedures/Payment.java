package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;

public class Payment extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(Payment.class);
	
	public SQLStmt payUpdateWhseSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE + " SET w_ytd = w_ytd + ?  WHERE w_id = ? ");
	public SQLStmt payGetWhseSQL = new SQLStmt("SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name"
			+ " FROM " + TPCCConstants.TABLENAME_WAREHOUSE + " WHERE w_id = ?");
	public SQLStmt payUpdateDistSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?");
	public SQLStmt payGetDistSQL = new SQLStmt("SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name"
			+ " FROM " + TPCCConstants.TABLENAME_DISTRICT + " WHERE d_w_id = ? AND d_id = ?");
	public SQLStmt payGetCustSQL = new SQLStmt("SELECT c_first, c_middle, c_last, c_street_1, c_street_2, "
			+ "c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, "
			+ "c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_since FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE "
			+ "c_w_id = ? AND c_d_id = ? AND c_id = ?");
	public SQLStmt payGetCustCdataSQL = new SQLStmt("SELECT c_data FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
	public SQLStmt payUpdateCustBalCdataSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET c_balance = ?, c_ytd_payment = ?, "
			+ "c_payment_cnt = ?, c_data = ? "
			+ "WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
	public SQLStmt payUpdateCustBalSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + " SET c_balance = ?, c_ytd_payment = ?, "
			+ "c_payment_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
	public SQLStmt payInsertHistSQL = new SQLStmt("INSERT INTO " + TPCCConstants.TABLENAME_HISTORY + " (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) "
			+ " VALUES (?,?,?,?,?,?,?,?)");
	public SQLStmt customerByNameSQL = new SQLStmt("SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, "
			+ "c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, "
			+ "c_balance, c_ytd_payment, c_payment_cnt, c_since FROM " + TPCCConstants.TABLENAME_CUSTOMER + " "
			+ "WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first");
	
	
	
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
	private PreparedStatement customerByName = null;
	
	
	
	 public ResultSet run(Connection conn, Random gen,
				int terminalWarehouseID, int numWarehouses,
				int terminalDistrictLowerID, int terminalDistrictUpperID,
				TPCCWorker w) throws SQLException{
	
		 
			//initializing all prepared statements
			payUpdateWhse=this.getPreparedStatement(conn, payUpdateWhseSQL);
			payGetWhse=this.getPreparedStatement(conn, payGetWhseSQL);
			payUpdateDist=this.getPreparedStatement(conn, payUpdateDistSQL);
			payGetDist =this.getPreparedStatement(conn, payGetDistSQL);
			payGetCust =this.getPreparedStatement(conn, payGetCustSQL);
			payGetCustCdata =this.getPreparedStatement(conn, payGetCustCdataSQL);
			payUpdateCustBalCdata =this.getPreparedStatement(conn, payUpdateCustBalCdataSQL);
			payUpdateCustBal =this.getPreparedStatement(conn, payUpdateCustBalSQL);
			payInsertHist =this.getPreparedStatement(conn, payInsertHistSQL);
			customerByName=this.getPreparedStatement(conn, customerByNameSQL);
		 
		 
		    // payUpdateWhse =this.getPreparedStatement(conn, payUpdateWhseSQL);
		 
		 
            int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
        	int customerID = TPCCUtil.getCustomerID(gen);
        
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
        
        	float paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, gen) / 100.0);

			paymentTransaction(terminalWarehouseID,
					customerWarehouseID, paymentAmount, districtID,
					customerDistrictID, customerID,
					customerLastName, customerByName, conn, w);

			return null;
	}
	 
    private void paymentTransaction(int w_id, int c_w_id, float h_amount,
				int d_id, int c_d_id, int c_id, String c_last, boolean c_by_name, Connection conn, TPCCWorker w)
				throws SQLException {
			String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
			String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;

		
			payUpdateWhse.setFloat(1, h_amount);
			payUpdateWhse.setInt(2, w_id);
			// MySQL reports deadlocks due to lock upgrades:
			// t1: read w_id = x; t2: update w_id = x; t1 update w_id = x
			int result = payUpdateWhse.executeUpdate();
			if (result == 0)
				throw new RuntimeException("W_ID=" + w_id + " not found!");

	
			payGetWhse.setInt(1, w_id);
			ResultSet rs = payGetWhse.executeQuery();
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


			payUpdateDist.setFloat(1, h_amount);
			payUpdateDist.setInt(2, w_id);
			payUpdateDist.setInt(3, d_id);
			result = payUpdateDist.executeUpdate();
			if (result == 0)
				throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id
						+ " not found!");


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
				c = getCustomerById(c_w_id, c_d_id, c_id, conn);
			}

			c.c_balance -= h_amount;
			c.c_ytd_payment += h_amount;
			c.c_payment_cnt += 1;
			String c_data = null;
			if (c.c_credit.equals("BC")) { // bad credit

	
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
			terminalMessage.append("\n+-----------------------------------------------------------------+\n\n");
			
			if(LOG.isTraceEnabled())LOG.trace(terminalMessage.toString());
			
		} 
	 
	 	// attention duplicated code across trans... ok for now to maintain separate prepared statements
		public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, Connection conn)
				throws SQLException {
	
			payGetCust.setInt(1, c_w_id);
			payGetCust.setInt(2, c_d_id);
			payGetCust.setInt(3, c_id);
			ResultSet rs = payGetCust.executeQuery();
			if (!rs.next()) {
				throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id
						+ " C_W_ID=" + c_w_id + " not found!");
			}

			Customer c = TPCCUtil.newCustomerFromResults(rs);
			c.c_id = c_id;
			c.c_last = rs.getString("c_last");
			rs.close();
			return c;
		}
		
		//attention this code is repeated in other transacitons... ok for now to allow for separate statements.
		public Customer getCustomerByName(int c_w_id, int c_d_id, String c_last)
				throws SQLException {
			ArrayList<Customer> customers = new ArrayList<Customer>();

			customerByName.setInt(1, c_w_id);
			customerByName.setInt(2, c_d_id);
			customerByName.setString(3, c_last);
			ResultSet rs = customerByName.executeQuery();

			while (rs.next()) {
				Customer c = TPCCUtil.newCustomerFromResults(rs);
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

		
}
