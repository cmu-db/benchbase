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
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;

public class OrderStatus extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(OrderStatus.class);
	
	public SQLStmt ordStatGetNewestOrdSQL = new SQLStmt("SELECT o_id, o_carrier_id, o_entry_d FROM " + TPCCConstants.TABLENAME_OPENORDER
			+ " WHERE o_w_id = ?"
			+ " AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1");
	
	public SQLStmt ordStatGetOrderLinesSQL = new SQLStmt("SELECT ol_i_id, ol_supply_w_id, ol_quantity,"
			+ " ol_amount, ol_delivery_d"
			+ " FROM " + TPCCConstants.TABLENAME_ORDERLINE
			+ " WHERE ol_o_id = ?"
			+ " AND ol_d_id =?"
			+ " AND ol_w_id = ?");
	
	public SQLStmt payGetCustSQL = new SQLStmt("SELECT c_first, c_middle, c_last, c_street_1, c_street_2, "
			+ "c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, "
			+ "c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_since FROM " + TPCCConstants.TABLENAME_CUSTOMER + " WHERE "
			+ "c_w_id = ? AND c_d_id = ? AND c_id = ?");
	
	public SQLStmt customerByNameSQL = new SQLStmt("SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, "
			+ "c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, "
			+ "c_balance, c_ytd_payment, c_payment_cnt, c_since FROM " + TPCCConstants.TABLENAME_CUSTOMER
			+ " WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first");

	private PreparedStatement ordStatGetNewestOrd = null;
	private PreparedStatement ordStatGetOrderLines = null;
	private PreparedStatement payGetCust = null;
	private PreparedStatement customerByName = null;


	 public ResultSet run(Connection conn, Random gen,
				int terminalWarehouseID, int numWarehouses,
				int terminalDistrictLowerID, int terminalDistrictUpperID,
				TPCCWorker w) throws SQLException{
	
		 
			//initializing all prepared statements
			payGetCust =this.getPreparedStatement(conn, payGetCustSQL);
			customerByName=this.getPreparedStatement(conn, customerByNameSQL);
			ordStatGetNewestOrd =this.getPreparedStatement(conn, ordStatGetNewestOrdSQL);
			ordStatGetOrderLines=this.getPreparedStatement(conn, ordStatGetOrderLinesSQL);
				
			int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
			boolean isCustomerByName=false;
			int y = TPCCUtil.randomNumber(1, 100, gen);
			String customerLastName = null;
			int customerID = -1;
			if (y <= 60) {
				isCustomerByName = true;
				customerLastName = TPCCUtil
						.getNonUniformRandomLastNameForRun(gen);
			} else {
				isCustomerByName = false;
				customerID = TPCCUtil.getCustomerID(gen);
			}

			orderStatusTransaction(terminalWarehouseID, districtID,
							customerID, customerLastName, isCustomerByName, conn, w);
			return null;
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
	
			private void orderStatusTransaction(int w_id, int d_id, int c_id,
					String c_last, boolean c_by_name, Connection conn, TPCCWorker w) throws SQLException {
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
					c = getCustomerById(w_id, d_id, c_id,conn);
				}

				// find the newest order for the customer
				// retrieve the carrier & order date for the most recent order.

	
				ordStatGetNewestOrd.setInt(1, w_id);
				ordStatGetNewestOrd.setInt(2, d_id);
				ordStatGetNewestOrd.setInt(3, c.c_id);
				ResultSet rs = ordStatGetNewestOrd.executeQuery();

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
					    if(LOG.isTraceEnabled()) LOG.trace(" This Order has no Order-Lines.\n");
					}
				}
				terminalMessage.append("+-----------------------------------------------------------------+\n\n");
				if(LOG.isTraceEnabled()) LOG.trace(terminalMessage.toString());
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



