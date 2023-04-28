/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.tpcc.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Oorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OrderStatus extends TPCCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(OrderStatus.class);

    public SQLStmt ordStatGetNewestOrdSQL = new SQLStmt(
            "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D " +
            "  FROM " + TPCCConstants.TABLENAME_OPENORDER +
            " WHERE O_W_ID = ? " +
            "   AND O_D_ID = ? " +
            "   AND O_C_ID = ? " +
            " ORDER BY O_ID DESC LIMIT 1");

    public SQLStmt ordStatGetOrderLinesSQL = new SQLStmt(
            "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D " +
            "  FROM " + TPCCConstants.TABLENAME_ORDERLINE +
            " WHERE OL_O_ID = ?" +
            "   AND OL_D_ID = ?" +
            "   AND OL_W_ID = ?");

    public SQLStmt payGetCustSQL = new SQLStmt(
            "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " +
            "       C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, " +
            "       C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?");

    public SQLStmt customerByNameSQL = new SQLStmt(
            "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " +
            "       C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, " +
            "       C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_LAST = ? " +
            " ORDER BY C_FIRST");

    public void run(Connection conn, Random gen, int w_id, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) throws SQLException {

        int d_id = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int y = TPCCUtil.randomNumber(1, 100, gen);

        boolean c_by_name;
        String c_last = null;
        int c_id = -1;

        if (y <= 60) {
            c_by_name = true;
            c_last = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
        } else {
            c_by_name = false;
            c_id = TPCCUtil.getCustomerID(gen);
        }


        Customer c;

        if (c_by_name) {
            c = getCustomerByName(w_id, d_id, c_last, conn);
        } else {
            c = getCustomerById(w_id, d_id, c_id, conn);
        }


        Oorder o = getOrderDetails(conn, w_id, d_id, c);

        // retrieve the order lines for the most recent order
        List<String> orderLines = getOrderLines(conn, w_id, d_id, o.o_id, c);

        if (LOG.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("\n");
            sb.append("+-------------------------- ORDER-STATUS -------------------------+\n");
            sb.append(" Date: ");
            sb.append(TPCCUtil.getCurrentTime());
            sb.append("\n\n Warehouse: ");
            sb.append(w_id);
            sb.append("\n District:  ");
            sb.append(d_id);
            sb.append("\n\n Customer:  ");
            sb.append(c.c_id);
            sb.append("\n   Name:    ");
            sb.append(c.c_first);
            sb.append(" ");
            sb.append(c.c_middle);
            sb.append(" ");
            sb.append(c.c_last);
            sb.append("\n   Balance: ");
            sb.append(c.c_balance);
            sb.append("\n\n");
            if (o.o_id == -1) {
                sb.append(" Customer has no orders placed.\n");
            } else {
                sb.append(" Order-Number: ");
                sb.append(o.o_id);
                sb.append("\n    Entry-Date: ");
                sb.append(o.o_entry_d);
                sb.append("\n    Carrier-Number: ");
                sb.append(o.o_carrier_id);
                sb.append("\n\n");
                if (orderLines.size() != 0) {
                    sb.append(" [Supply_W - Item_ID - Qty - Amount - Delivery-Date]\n");
                    for (String orderLine : orderLines) {
                        sb.append(" ");
                        sb.append(orderLine);
                        sb.append("\n");
                    }
                } else {
                    LOG.trace(" This Order has no Order-Lines.\n");
                }
            }
            sb.append("+-----------------------------------------------------------------+\n\n");
            LOG.trace(sb.toString());
        }


    }

    private Oorder getOrderDetails(Connection conn, int w_id, int d_id, Customer c) throws SQLException {
        try (PreparedStatement ordStatGetNewestOrd = this.getPreparedStatement(conn, ordStatGetNewestOrdSQL)) {


            // find the newest order for the customer
            // retrieve the carrier & order date for the most recent order.

            ordStatGetNewestOrd.setInt(1, w_id);
            ordStatGetNewestOrd.setInt(2, d_id);
            ordStatGetNewestOrd.setInt(3, c.c_id);

            try (ResultSet rs = ordStatGetNewestOrd.executeQuery()) {

                if (!rs.next()) {
                    String msg = String.format("No order records for CUSTOMER [C_W_ID=%d, C_D_ID=%d, C_ID=%d]", w_id, d_id, c.c_id);

                    throw new RuntimeException(msg);
                }
                Oorder o = new Oorder();
                o.o_id=rs.getInt("O_ID");
                o.o_carrier_id = rs.getInt("O_CARRIER_ID");
                o.o_entry_d = rs.getTimestamp("O_ENTRY_D");
                return o;
            }
        }
    }

    private List<String> getOrderLines(Connection conn, int w_id, int d_id, int o_id, Customer c) throws SQLException {
        List<String> orderLines = new ArrayList<>();

        try (PreparedStatement ordStatGetOrderLines = this.getPreparedStatement(conn, ordStatGetOrderLinesSQL)) {
            ordStatGetOrderLines.setInt(1, o_id);
            ordStatGetOrderLines.setInt(2, d_id);
            ordStatGetOrderLines.setInt(3, w_id);

            try (ResultSet rs = ordStatGetOrderLines.executeQuery()) {

                while (rs.next()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("[");
                    sb.append(rs.getLong("OL_SUPPLY_W_ID"));
                    sb.append(" - ");
                    sb.append(rs.getLong("OL_I_ID"));
                    sb.append(" - ");
                    sb.append(rs.getLong("OL_QUANTITY"));
                    sb.append(" - ");
                    sb.append(TPCCUtil.formattedDouble(rs.getDouble("OL_AMOUNT")));
                    sb.append(" - ");
                    if (rs.getTimestamp("OL_DELIVERY_D") != null) {
                        sb.append(rs.getTimestamp("OL_DELIVERY_D"));
                    } else {
                        sb.append("99-99-9999");
                    }
                    sb.append("]");
                    orderLines.add(sb.toString());
                }
            }


            if (orderLines.isEmpty()) {
                String msg = String.format("Order record had no order line items [C_W_ID=%d, C_D_ID=%d, C_ID=%d, O_ID=%d]", w_id, d_id, c.c_id, o_id);
                LOG.trace(msg);
            }
        }

        return orderLines;
    }

    // attention duplicated code across trans... ok for now to maintain separate
    // prepared statements
    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, Connection conn) throws SQLException {

        try (PreparedStatement payGetCust = this.getPreparedStatement(conn, payGetCustSQL)) {

            payGetCust.setInt(1, c_w_id);
            payGetCust.setInt(2, c_d_id);
            payGetCust.setInt(3, c_id);

            try (ResultSet rs = payGetCust.executeQuery()) {

                if (!rs.next()) {
                    String msg = String.format("Failed to get CUSTOMER [C_W_ID=%d, C_D_ID=%d, C_ID=%d]", c_w_id, c_d_id, c_id);

                    throw new RuntimeException(msg);
                }

                Customer c = TPCCUtil.newCustomerFromResults(rs);
                c.c_id = c_id;
                c.c_last = rs.getString("C_LAST");
                return c;
            }
        }
    }

    // attention this code is repeated in other transacitons... ok for now to
    // allow for separate statements.
    public Customer getCustomerByName(int c_w_id, int c_d_id, String c_last, Connection conn) throws SQLException {
        ArrayList<Customer> customers = new ArrayList<>();

        try (PreparedStatement customerByName = this.getPreparedStatement(conn, customerByNameSQL)) {

            customerByName.setInt(1, c_w_id);
            customerByName.setInt(2, c_d_id);
            customerByName.setString(3, c_last);

            try (ResultSet rs = customerByName.executeQuery()) {
                while (rs.next()) {
                    Customer c = TPCCUtil.newCustomerFromResults(rs);
                    c.c_id = rs.getInt("C_ID");
                    c.c_last = c_last;
                    customers.add(c);
                }
            }
        }

        if (customers.size() == 0) {
            String msg = String.format("Failed to get CUSTOMER [C_W_ID=%d, C_D_ID=%d, C_LAST=%s]", c_w_id, c_d_id, c_last);

            throw new RuntimeException(msg);
        }

        // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
        // that counts starting from 1.
        int index = customers.size() / 2;
        if (customers.size() % 2 == 0) {
            index -= 1;
        }
        return customers.get(index);
    }


}



