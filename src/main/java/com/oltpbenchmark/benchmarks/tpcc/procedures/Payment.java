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

package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;

public class Payment extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(Payment.class);

    public SQLStmt payUpdateWhseSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE + 
            "   SET W_YTD = W_YTD + ? " +
            " WHERE W_ID = ? ");
    
    public SQLStmt payGetWhseSQL = new SQLStmt(
            "SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME" + 
            "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE + 
            " WHERE W_ID = ?");
    
    public SQLStmt payUpdateDistSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_DISTRICT + 
            "   SET D_YTD = D_YTD + ? " +
            " WHERE D_W_ID = ? " +
            "   AND D_ID = ?");
    
    public SQLStmt payGetDistSQL = new SQLStmt(
            "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME" + 
            "  FROM " + TPCCConstants.TABLENAME_DISTRICT + 
            " WHERE D_W_ID = ? " +
            "   AND D_ID = ?");
    
    public SQLStmt payGetCustSQL = new SQLStmt(
            "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " + 
            "       C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, " + 
            "       C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + 
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?");
    
    public SQLStmt payGetCustCdataSQL = new SQLStmt(
            "SELECT C_DATA " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + 
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?");
    
    public SQLStmt payUpdateCustBalCdataSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + 
            "   SET C_BALANCE = ?, " +
            "       C_YTD_PAYMENT = ?, " + 
            "       C_PAYMENT_CNT = ?, " +
            "       C_DATA = ? " +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " + 
            "   AND C_ID = ?");
    
    public SQLStmt payUpdateCustBalSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + 
            "   SET C_BALANCE = ?, " +
            "       C_YTD_PAYMENT = ?, " +
            "       C_PAYMENT_CNT = ? " +
            " WHERE C_W_ID = ? " + 
            "   AND C_D_ID = ? " + 
            "   AND C_ID = ?");
    
    public SQLStmt payInsertHistSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_HISTORY + 
            " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) " +
            " VALUES (?,?,?,?,?,?,?,?)");
    
    public SQLStmt customerByNameSQL = new SQLStmt(
            "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " + 
            "       C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, " +
            "       C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + 
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_LAST = ? " +
            " ORDER BY C_FIRST");

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
                         int w_id, int numWarehouses,
                         int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) throws SQLException {

        // initializing all prepared statements
        payUpdateWhse = this.getPreparedStatement(conn, payUpdateWhseSQL);
        payGetWhse = this.getPreparedStatement(conn, payGetWhseSQL);
        payUpdateDist = this.getPreparedStatement(conn, payUpdateDistSQL);
        payGetDist = this.getPreparedStatement(conn, payGetDistSQL);
        payGetCust = this.getPreparedStatement(conn, payGetCustSQL);
        payGetCustCdata = this.getPreparedStatement(conn, payGetCustCdataSQL);
        payUpdateCustBalCdata = this.getPreparedStatement(conn, payUpdateCustBalCdataSQL);
        payUpdateCustBal = this.getPreparedStatement(conn, payUpdateCustBalSQL);
        payInsertHist = this.getPreparedStatement(conn, payInsertHistSQL);
        customerByName = this.getPreparedStatement(conn, customerByNameSQL);

        // payUpdateWhse =this.getPreparedStatement(conn, payUpdateWhseSQL);

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int customerID = TPCCUtil.getCustomerID(gen);

        int x = TPCCUtil.randomNumber(1, 100, gen);
        int customerDistrictID;
        int customerWarehouseID;
        if (x <= 85) {
            customerDistrictID = districtID;
            customerWarehouseID = w_id;
        } else {
            customerDistrictID = TPCCUtil.randomNumber(1, TPCCConfig.configDistPerWhse, gen);
            do {
                customerWarehouseID = TPCCUtil.randomNumber(1, numWarehouses, gen);
            } while (customerWarehouseID == w_id && numWarehouses > 1);
        }

        long y = TPCCUtil.randomNumber(1, 100, gen);
        boolean customerByName;
        String customerLastName = null;
        customerID = -1;
        if (y <= 60) {
            // 60% lookups by last name
            customerByName = true;
            customerLastName = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
        } else {
            // 40% lookups by customer ID
            customerByName = false;
            customerID = TPCCUtil.getCustomerID(gen);
        }

        float paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, gen) / 100.0);

        String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
        String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;

        payUpdateWhse.setDouble(1, paymentAmount);
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
        w_street_1 = rs.getString("W_STREET_1");
        w_street_2 = rs.getString("W_STREET_2");
        w_city = rs.getString("W_CITY");
        w_state = rs.getString("W_STATE");
        w_zip = rs.getString("W_ZIP");
        w_name = rs.getString("W_NAME");
        rs.close();
        rs = null;

        payUpdateDist.setDouble(1, paymentAmount);
        payUpdateDist.setInt(2, w_id);
        payUpdateDist.setInt(3, districtID);
        result = payUpdateDist.executeUpdate();
        if (result == 0)
            throw new RuntimeException("D_ID=" + districtID + " D_W_ID=" + w_id + " not found!");

        payGetDist.setInt(1, w_id);
        payGetDist.setInt(2, districtID);
        rs = payGetDist.executeQuery();
        if (!rs.next())
            throw new RuntimeException("D_ID=" + districtID + " D_W_ID=" + w_id + " not found!");
        d_street_1 = rs.getString("D_STREET_1");
        d_street_2 = rs.getString("D_STREET_2");
        d_city = rs.getString("D_CITY");
        d_state = rs.getString("D_STATE");
        d_zip = rs.getString("D_ZIP");
        d_name = rs.getString("D_NAME");
        rs.close();
        rs = null;

        Customer c;
        if (customerByName) {
            assert customerID <= 0;
            c = getCustomerByName(customerWarehouseID, customerDistrictID, customerLastName);
        } else {
            assert customerLastName == null;
            c = getCustomerById(customerWarehouseID, customerDistrictID, customerID, conn);
        }

        c.c_balance -= paymentAmount;
        c.c_ytd_payment += paymentAmount;
        c.c_payment_cnt += 1;
        String c_data = null;
        if (c.c_credit.equals("BC")) { // bad credit
            payGetCustCdata.setInt(1, customerWarehouseID);
            payGetCustCdata.setInt(2, customerDistrictID);
            payGetCustCdata.setInt(3, c.c_id);
            rs = payGetCustCdata.executeQuery();
            if (!rs.next())
                throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID + " not found!");
            c_data = rs.getString("C_DATA");
            rs.close();
            rs = null;

            c_data = c.c_id + " " + customerDistrictID + " " + customerWarehouseID + " " + districtID + " " + w_id + " " + paymentAmount + " | " + c_data;
            if (c_data.length() > 500)
                c_data = c_data.substring(0, 500);

            payUpdateCustBalCdata.setDouble(1, c.c_balance);
            payUpdateCustBalCdata.setDouble(2, c.c_ytd_payment);
            payUpdateCustBalCdata.setInt(3, c.c_payment_cnt);
            payUpdateCustBalCdata.setString(4, c_data);
            payUpdateCustBalCdata.setInt(5, customerWarehouseID);
            payUpdateCustBalCdata.setInt(6, customerDistrictID);
            payUpdateCustBalCdata.setInt(7, c.c_id);
            result = payUpdateCustBalCdata.executeUpdate();

            if (result == 0)
                throw new RuntimeException("Error in PYMNT Txn updating Customer C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID);

        } else { // GoodCredit

            payUpdateCustBal.setDouble(1, c.c_balance);
            payUpdateCustBal.setDouble(2, c.c_ytd_payment);
            payUpdateCustBal.setInt(3, c.c_payment_cnt);
            payUpdateCustBal.setInt(4, customerWarehouseID);
            payUpdateCustBal.setInt(5, customerDistrictID);
            payUpdateCustBal.setInt(6, c.c_id);
            result = payUpdateCustBal.executeUpdate();

            if (result == 0)
                throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID + " not found!");

        }

        if (w_name.length() > 10)
            w_name = w_name.substring(0, 10);
        if (d_name.length() > 10)
            d_name = d_name.substring(0, 10);
        String h_data = w_name + "    " + d_name;

        payInsertHist.setInt(1, customerDistrictID);
        payInsertHist.setInt(2, customerWarehouseID);
        payInsertHist.setInt(3, c.c_id);
        payInsertHist.setInt(4, districtID);
        payInsertHist.setInt(5, w_id);
        payInsertHist.setTimestamp(6, w.getBenchmarkModule().getTimestamp(System.currentTimeMillis()));
        payInsertHist.setDouble(7, paymentAmount);
        payInsertHist.setString(8, h_data);
        payInsertHist.executeUpdate();

        conn.commit();

        if (LOG.isTraceEnabled()) {
            StringBuilder terminalMessage = new StringBuilder();
            terminalMessage.append("\n+---------------------------- PAYMENT ----------------------------+");
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
            terminalMessage.append(districtID);
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
            terminalMessage.append(paymentAmount);
            terminalMessage.append("\n Credit Limit:     ");
            terminalMessage.append(c.c_credit_lim);
            terminalMessage.append("\n New Cust-Balance: ");
            terminalMessage.append(c.c_balance);
            if (c.c_credit.equals("BC")) {
                if (c_data.length() > 50) {
                    terminalMessage.append("\n\n Cust-Data: " + c_data.substring(0, 50));
                    int data_chunks = c_data.length() > 200 ? 4 : c_data.length() / 50;
                    for (int n = 1; n < data_chunks; n++)
                        terminalMessage.append("\n            " + c_data.substring(n * 50, (n + 1) * 50));
                } else {
                    terminalMessage.append("\n\n Cust-Data: " + c_data);
                }
            }
            terminalMessage.append("\n+-----------------------------------------------------------------+\n\n");

            LOG.trace(terminalMessage.toString());
        }

        return null;
    }

    // attention duplicated code across trans... ok for now to maintain separate
    // prepared statements
    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, Connection conn) throws SQLException {

        payGetCust.setInt(1, c_w_id);
        payGetCust.setInt(2, c_d_id);
        payGetCust.setInt(3, c_id);
        ResultSet rs = payGetCust.executeQuery();
        if (!rs.next()) {
            throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
        }

        Customer c = TPCCUtil.newCustomerFromResults(rs);
        c.c_id = c_id;
        c.c_last = rs.getString("C_LAST");
        rs.close();
        return c;
    }

    // attention this code is repeated in other transacitons... ok for now to
    // allow for separate statements.
    public Customer getCustomerByName(int c_w_id, int c_d_id, String customerLastName) throws SQLException {
        ArrayList<Customer> customers = new ArrayList<Customer>();

        customerByName.setInt(1, c_w_id);
        customerByName.setInt(2, c_d_id);
        customerByName.setString(3, customerLastName);
        ResultSet rs = customerByName.executeQuery();
        if (LOG.isTraceEnabled()) LOG.trace("C_LAST=" + customerLastName + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id);

        while (rs.next()) {
            Customer c = TPCCUtil.newCustomerFromResults(rs);
            c.c_id = rs.getInt("C_ID");
            c.c_last = customerLastName;
            customers.add(c);
        }
        rs.close();

        if (customers.size() == 0) {
            throw new RuntimeException("C_LAST=" + customerLastName + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
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
