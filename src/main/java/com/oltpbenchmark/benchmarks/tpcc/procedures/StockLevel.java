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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

public class StockLevel extends TPCCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(StockLevel.class);

    public SQLStmt stockGetDistOrderIdSQL = new SQLStmt(
            "SELECT D_NEXT_O_ID " +
            "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
            " WHERE D_W_ID = ? " +
            "   AND D_ID = ?");

    public SQLStmt stockGetCountStockSQL = new SQLStmt(
            "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT " +
            " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " + TPCCConstants.TABLENAME_STOCK +
            " WHERE OL_W_ID = ?" +
            " AND OL_D_ID = ?" +
            " AND OL_O_ID < ?" +
            " AND OL_O_ID >= ?" +
            " AND S_W_ID = ?" +
            " AND S_I_ID = OL_I_ID" +
            " AND S_QUANTITY < ?");

    public void run(Connection conn, Random gen, int w_id, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) throws SQLException {

        int threshold = TPCCUtil.randomNumber(10, 20, gen);
        int d_id = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);

        int o_id = getOrderId(conn, w_id, d_id);

        int stock_count = getStockCount(conn, w_id, threshold, d_id, o_id);

        if (LOG.isTraceEnabled()) {
            String terminalMessage = "\n+-------------------------- STOCK-LEVEL --------------------------+" +
                                     "\n Warehouse: " +
                                     w_id +
                                     "\n District:  " +
                                     d_id +
                                     "\n\n Stock Level Threshold: " +
                                     threshold +
                                     "\n Low Stock Count:       " +
                                     stock_count +
                                     "\n+-----------------------------------------------------------------+\n\n";
            LOG.trace(terminalMessage);
        }


    }

    private int getOrderId(Connection conn, int w_id, int d_id) throws SQLException {
        try (PreparedStatement stockGetDistOrderId = this.getPreparedStatement(conn, stockGetDistOrderIdSQL)) {
            stockGetDistOrderId.setInt(1, w_id);
            stockGetDistOrderId.setInt(2, d_id);

            try (ResultSet rs = stockGetDistOrderId.executeQuery()) {

                if (!rs.next()) {
                    throw new RuntimeException("D_W_ID=" + w_id + " D_ID=" + d_id + " not found!");
                }
                return rs.getInt("D_NEXT_O_ID");
            }
        }

    }

    private int getStockCount(Connection conn, int w_id, int threshold, int d_id, int o_id) throws SQLException {
        try (PreparedStatement stockGetCountStock = this.getPreparedStatement(conn, stockGetCountStockSQL)) {
            stockGetCountStock.setInt(1, w_id);
            stockGetCountStock.setInt(2, d_id);
            stockGetCountStock.setInt(3, o_id);
            stockGetCountStock.setInt(4, o_id - 20);
            stockGetCountStock.setInt(5, w_id);
            stockGetCountStock.setInt(6, threshold);

            try (ResultSet rs = stockGetCountStock.executeQuery()) {
                if (!rs.next()) {
                    String msg = String.format("Failed to get StockLevel result for COUNT query [W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, o_id);

                    throw new RuntimeException(msg);
                }

                return rs.getInt("STOCK_COUNT");
            }
        }
    }
}
