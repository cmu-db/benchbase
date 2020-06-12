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
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Random;

public class NewOrder extends TPCCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(NewOrder.class);

    public final SQLStmt stmtGetCustSQL = new SQLStmt(
            "SELECT C_DISCOUNT, C_LAST, C_CREDIT" +
                    "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
                    " WHERE C_W_ID = ? " +
                    "   AND C_D_ID = ? " +
                    "   AND C_ID = ?");

    public final SQLStmt stmtGetWhseSQL = new SQLStmt(
            "SELECT W_TAX " +
                    "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE +
                    " WHERE W_ID = ?");

    public final SQLStmt stmtGetDistSQL = new SQLStmt(
            "SELECT D_NEXT_O_ID, D_TAX " +
                    "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
                    " WHERE D_W_ID = ? AND D_ID = ? FOR UPDATE");

    public final SQLStmt stmtInsertNewOrderSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER +
                    " (NO_O_ID, NO_D_ID, NO_W_ID) " +
                    " VALUES ( ?, ?, ?)");

    public final SQLStmt stmtUpdateDistSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_DISTRICT +
                    "   SET D_NEXT_O_ID = D_NEXT_O_ID + 1 " +
                    " WHERE D_W_ID = ? " +
                    "   AND D_ID = ?");

    public final SQLStmt stmtInsertOOrderSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER +
                    " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?)");

    public final SQLStmt stmtGetItemSQL = new SQLStmt(
            "SELECT I_PRICE, I_NAME , I_DATA " +
                    "  FROM " + TPCCConstants.TABLENAME_ITEM +
                    " WHERE I_ID = ?");

    public final SQLStmt stmtGetStockSQL = new SQLStmt(
            "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " +
                    "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" +
                    "  FROM " + TPCCConstants.TABLENAME_STOCK +
                    " WHERE S_I_ID = ? " +
                    "   AND S_W_ID = ? FOR UPDATE");

    public final SQLStmt stmtUpdateStockSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_STOCK +
                    "   SET S_QUANTITY = ? , " +
                    "       S_YTD = S_YTD + ?, " +
                    "       S_ORDER_CNT = S_ORDER_CNT + 1, " +
                    "       S_REMOTE_CNT = S_REMOTE_CNT + ? " +
                    " WHERE S_I_ID = ? " +
                    "   AND S_W_ID = ?");

    public final SQLStmt stmtInsertOrderLineSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE +
                    " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) " +
                    " VALUES (?,?,?,?,?,?,?,?,?)");




    public void run(Connection conn, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) throws SQLException {


        //initializing all prepared statements


        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int customerID = TPCCUtil.getCustomerID(gen);

        int numItems = TPCCUtil.randomNumber(5, 15, gen);
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
                }
                while (supplierWarehouseIDs[i] == terminalWarehouseID
                        && numWarehouses > 1);
                allLocal = 0;
            }
            orderQuantities[i] = TPCCUtil.randomNumber(1, 10, gen);
        }

        // we need to cause 1% of the new orders to be rolled back.
        if (TPCCUtil.randomNumber(1, 100, gen) == 1) {
            itemIDs[numItems - 1] = TPCCConfig.INVALID_ITEM_ID;
        }


        newOrderTransaction(terminalWarehouseID, districtID,
                customerID, numItems, allLocal, itemIDs,
                supplierWarehouseIDs, orderQuantities, conn);

    }


    private void newOrderTransaction(int w_id, int d_id, int c_id,
                                     int o_ol_cnt, int o_all_local, int[] itemIDs,
                                     int[] supplierWarehouseIDs, int[] orderQuantities, Connection conn)
            throws SQLException {
        float i_price;
        int d_next_o_id;
        int o_id;
        int s_quantity;
        String s_dist_01;
        String s_dist_02;
        String s_dist_03;
        String s_dist_04;
        String s_dist_05;
        String s_dist_06;
        String s_dist_07;
        String s_dist_08;
        String s_dist_09;
        String s_dist_10;
        String ol_dist_info = null;

        int ol_supply_w_id;
        int ol_i_id;
        int ol_quantity;
        int s_remote_cnt_increment;
        float ol_amount;

        try (PreparedStatement stmtGetCust = this.getPreparedStatement(conn, stmtGetCustSQL);
        PreparedStatement  stmtGetWhse = this.getPreparedStatement(conn, stmtGetWhseSQL);
        PreparedStatement stmtGetDist = this.getPreparedStatement(conn, stmtGetDistSQL);
        PreparedStatement stmtInsertNewOrder = this.getPreparedStatement(conn, stmtInsertNewOrderSQL);
        PreparedStatement stmtUpdateDist = this.getPreparedStatement(conn, stmtUpdateDistSQL);
        PreparedStatement stmtInsertOOrder = this.getPreparedStatement(conn, stmtInsertOOrderSQL);
        PreparedStatement stmtGetItem = this.getPreparedStatement(conn, stmtGetItemSQL);
        PreparedStatement stmtGetStock = this.getPreparedStatement(conn, stmtGetStockSQL);
        PreparedStatement stmtUpdateStock = this.getPreparedStatement(conn, stmtUpdateStockSQL);
        PreparedStatement stmtInsertOrderLine = this.getPreparedStatement(conn, stmtInsertOrderLineSQL)) {


           try {
               stmtGetCust.setInt(1, w_id);
               stmtGetCust.setInt(2, d_id);
               stmtGetCust.setInt(3, c_id);
               try (ResultSet rs = stmtGetCust.executeQuery()) {
                   if (!rs.next()) {
                       throw new RuntimeException("C_D_ID=" + d_id
                               + " C_ID=" + c_id + " not found!");
                   }
               }

               stmtGetWhse.setInt(1, w_id);
               try (ResultSet rs = stmtGetWhse.executeQuery()) {
                   if (!rs.next()) {
                       throw new RuntimeException("W_ID=" + w_id + " not found!");
                   }
               }

               stmtGetDist.setInt(1, w_id);
               stmtGetDist.setInt(2, d_id);
               try (ResultSet rs = stmtGetDist.executeQuery()) {
                   if (!rs.next()) {
                       throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id
                               + " not found!");
                   }
                   d_next_o_id = rs.getInt("D_NEXT_O_ID");
               }

               //woonhak, need to change order because of foreign key constraints
               //update next_order_id first, but it might doesn't matter
               stmtUpdateDist.setInt(1, w_id);
               stmtUpdateDist.setInt(2, d_id);
               int result = stmtUpdateDist.executeUpdate();
               if (result == 0) {
                   throw new RuntimeException(
                           "Error!! Cannot update next_order_id on district for D_ID="
                                   + d_id + " D_W_ID=" + w_id);
               }

               o_id = d_next_o_id;

               // woonhak, need to change order, because of foreign key constraints
               //[[insert ooder first
               stmtInsertOOrder.setInt(1, o_id);
               stmtInsertOOrder.setInt(2, d_id);
               stmtInsertOOrder.setInt(3, w_id);
               stmtInsertOOrder.setInt(4, c_id);
               stmtInsertOOrder.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
               stmtInsertOOrder.setInt(6, o_ol_cnt);
               stmtInsertOOrder.setInt(7, o_all_local);
               stmtInsertOOrder.executeUpdate();
               //insert ooder first]]
               /*TODO: add error checking */

               stmtInsertNewOrder.setInt(1, o_id);
               stmtInsertNewOrder.setInt(2, d_id);
               stmtInsertNewOrder.setInt(3, w_id);
               stmtInsertNewOrder.executeUpdate();
               /*TODO: add error checking */


               for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                   ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
                   ol_i_id = itemIDs[ol_number - 1];
                   ol_quantity = orderQuantities[ol_number - 1];
                   stmtGetItem.setInt(1, ol_i_id);
                   try (ResultSet rs = stmtGetItem.executeQuery()) {
                       if (!rs.next()) {
                           // This is (hopefully) an expected error: this is an
                           // expected new order rollback
                           throw new UserAbortException(
                                   "EXPECTED new order rollback: I_ID=" + ol_i_id
                                           + " not found!");
                       }

                       i_price = rs.getFloat("I_PRICE");
                   }



                   stmtGetStock.setInt(1, ol_i_id);
                   stmtGetStock.setInt(2, ol_supply_w_id);
                   try (ResultSet rs = stmtGetStock.executeQuery()) {
                       if (!rs.next()) {
                           throw new RuntimeException("I_ID=" + ol_i_id
                                   + " not found!");
                       }
                       s_quantity = rs.getInt("S_QUANTITY");
                       s_dist_01 = rs.getString("S_DIST_01");
                       s_dist_02 = rs.getString("S_DIST_02");
                       s_dist_03 = rs.getString("S_DIST_03");
                       s_dist_04 = rs.getString("S_DIST_04");
                       s_dist_05 = rs.getString("S_DIST_05");
                       s_dist_06 = rs.getString("S_DIST_06");
                       s_dist_07 = rs.getString("S_DIST_07");
                       s_dist_08 = rs.getString("S_DIST_08");
                       s_dist_09 = rs.getString("S_DIST_09");
                       s_dist_10 = rs.getString("S_DIST_10");
                   }

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


                   switch (d_id) {
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
                   stmtInsertOrderLine.setDouble(8, ol_amount);
                   stmtInsertOrderLine.setString(9, ol_dist_info);
                   stmtInsertOrderLine.addBatch();

               }

               stmtInsertOrderLine.executeBatch();
               stmtUpdateStock.executeBatch();

           } catch (UserAbortException userEx) {
               LOG.debug("Caught an expected error in New Order");
               throw userEx;
           } finally {
               if (stmtInsertOrderLine != null) {
                   stmtInsertOrderLine.clearBatch();
               }
               if (stmtUpdateStock != null) {
                   stmtUpdateStock.clearBatch();
               }
           }

       }

    }

}
