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


package com.oltpbenchmark.benchmarks.auctionmark.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;

import java.sql.*;

/**
 * NewPurchase
 * Description goes here...
 *
 * @author visawee
 */
public class NewPurchase extends Procedure {

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public final SQLStmt getItemMaxBid = new SQLStmt(
            "SELECT * FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID +
                    " WHERE imb_i_id = ? AND imb_u_id = ?"
    );

    public final SQLStmt getMaxBid = new SQLStmt(
            "SELECT * FROM " + AuctionMarkConstants.TABLENAME_ITEM_BID +
                    " WHERE imb_i_id = ? AND imb_u_id = ? " +
                    " ORDER BY ib_bid DESC LIMIT 1"
    );

    public final SQLStmt insertItemMaxBid = new SQLStmt(
            "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + " (" +
                    "imb_i_id, " +
                    "imb_u_id, " +
                    "imb_ib_id, " +
                    "imb_ib_i_id, " +
                    "imb_ib_u_id, " +
                    "imb_created, " +
                    "imb_updated " +
                    ") VALUES (" +
                    "?, " + // imb_i_id
                    "?, " + // imb_u_id
                    "?, " + // imb_ib_id
                    "?, " + // imb_ib_i_id
                    "?, " + // imb_ib_u_id
                    "?, " + // imb_created
                    "? " + // imb_updated
                    ")"
    );

    public final SQLStmt getItemInfo = new SQLStmt(
            "SELECT i_num_bids, i_current_price, i_end_date, " +
                    "       ib_id, ib_buyer_id, " +
                    "       u_balance " +
                    "  FROM " + AuctionMarkConstants.TABLENAME_ITEM + ", " +
                    AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + ", " +
                    AuctionMarkConstants.TABLENAME_ITEM_BID + ", " +
                    AuctionMarkConstants.TABLENAME_USERACCT +
                    " WHERE i_id = ? AND i_u_id = ? " +
                    "   AND imb_i_id = i_id AND imb_u_id = i_u_id " +
                    "   AND imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id " +
                    "   AND ib_buyer_id = u_id "
    );

    public final SQLStmt getBuyerInfo = new SQLStmt(
            "SELECT u_id, u_balance " +
                    "  FROM " + AuctionMarkConstants.TABLENAME_USERACCT +
                    " WHERE u_id = ? "
    );

    public final SQLStmt insertPurchase = new SQLStmt(
            "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_PURCHASE + " (" +
                    "ip_id," +
                    "ip_ib_id," +
                    "ip_ib_i_id," +
                    "ip_ib_u_id," +
                    "ip_date" +
                    ") VALUES(?,?,?,?,?)"
    );

    public final SQLStmt updateItem = new SQLStmt(
            "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM +
                    " SET i_status = " + ItemStatus.CLOSED.ordinal() + ", i_updated = ? " +
                    " WHERE i_id = ? AND i_u_id = ? "
    );

    public final SQLStmt updateUserItem = new SQLStmt(
            "UPDATE " + AuctionMarkConstants.TABLENAME_USERACCT_ITEM + " " +
                    "SET ui_ip_id = ?, " +
                    "    ui_ip_ib_id = ?, " +
                    "    ui_ip_ib_i_id = ?, " +
                    "    ui_ip_ib_u_id = ?" +
                    " WHERE ui_u_id = ? AND ui_i_id = ? AND ui_i_u_id = ?"
    );

    public final SQLStmt insertUserItem = new SQLStmt(
            "INSERT INTO " + AuctionMarkConstants.TABLENAME_USERACCT_ITEM + "(" +
                    "ui_u_id, " +
                    "ui_i_id, " +
                    "ui_i_u_id, " +
                    "ui_ip_id, " +
                    "ui_ip_ib_id, " +
                    "ui_ip_ib_i_id, " +
                    "ui_ip_ib_u_id, " +
                    "ui_created" +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    );

    public final SQLStmt updateUserBalance = new SQLStmt(
            "UPDATE " + AuctionMarkConstants.TABLENAME_USERACCT + " " +
                    "SET u_balance = u_balance + ? " +
                    " WHERE u_id = ?"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------

    public Object[] run(Connection conn, Timestamp[] benchmarkTimes,
                        String item_id, String seller_id, String ip_id, double buyer_credit) throws SQLException {
        final Timestamp currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);

        // HACK: Check whether we have an ITEM_MAX_BID record. If not, we'll insert one
        try (PreparedStatement getItemMaxBidStatement = this.getPreparedStatement(conn, getItemMaxBid, item_id, seller_id);
             ResultSet results = getItemMaxBidStatement.executeQuery()) {
            if (!results.next()) {
                try (PreparedStatement getMaxBidStatement = this.getPreparedStatement(conn, getMaxBid, item_id, seller_id);
                     ResultSet getMaxBidResults = getMaxBidStatement.executeQuery()) {
                    getMaxBidResults.next();

                    long bid_id = results.getLong(1);

                    try (PreparedStatement insertItemMaxBidStatement = this.getPreparedStatement(conn, insertItemMaxBid, item_id,
                            seller_id,
                            bid_id,
                            item_id,
                            seller_id,
                            currentTime,
                            currentTime)) {
                        insertItemMaxBidStatement.executeUpdate();
                    }
                }
            }
        }

        // Get the ITEM_MAX_BID record so that we know what we need to process
        // At this point we should always have an ITEM_MAX_BID record

        long i_num_bids;
        double i_current_price;
        Timestamp i_end_date;
        ItemStatus i_status;
        long ib_id;
        long ib_buyer_id;
        double u_balance;

        try (PreparedStatement stmt = this.getPreparedStatement(conn, getItemInfo, item_id, seller_id)) {
            try (ResultSet results = stmt.executeQuery()) {
                if (!results.next()) {
                    String msg = "No ITEM_MAX_BID is available record for item " + item_id;
                    throw new UserAbortException(msg);
                }
                int col = 1;
                i_num_bids = results.getLong(col++);
                i_current_price = results.getDouble(col++);
                i_end_date = results.getTimestamp(col++);
                i_status = ItemStatus.CLOSED;
                ib_id = results.getLong(col++);
                ib_buyer_id = results.getLong(col++);
                u_balance = results.getDouble(col);
            }
        }

        // Make sure that the buyer has enough money to cover this charge
        // We can add in a credit for the buyer's account
        if (i_current_price > (buyer_credit + u_balance)) {
            String msg = String.format("Buyer #%d does not have enough money in account to purchase Item #%s" +
                            "[maxBid=%.2f, balance=%.2f, credit=%.2f]",
                    ib_buyer_id, item_id, i_current_price, u_balance, buyer_credit);
            throw new UserAbortException(msg);
        }

        // Set item_purchase_id
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, insertPurchase, ip_id, ib_id, item_id, seller_id, currentTime)) {
            preparedStatement.executeUpdate();
        }


        // Update item status to close
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, updateItem, currentTime, item_id, seller_id)) {
            preparedStatement.executeUpdate();
        }

        // And update this the USERACT_ITEM record to link it to the new ITEM_PURCHASE record
        // If we don't have a record to update, just go ahead and create it
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, updateUserItem, ip_id, ib_id, item_id, seller_id,
                ib_buyer_id, item_id, seller_id)) {
            int updated = preparedStatement.executeUpdate();
            if (updated == 0) {
                try (PreparedStatement preparedStatement2 = this.getPreparedStatement(conn, insertUserItem, ib_buyer_id, item_id, seller_id,
                        ip_id, ib_id, item_id, seller_id,
                        currentTime)) {
                    preparedStatement2.executeUpdate();
                }
            }
        }
        // Decrement the buyer's account
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, updateUserBalance, -1 * (i_current_price) + buyer_credit, ib_buyer_id)) {
            preparedStatement.executeUpdate();
        }

        // And credit the seller's account
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, updateUserBalance, i_current_price, seller_id)) {
            preparedStatement.executeUpdate();
        }
        // Return a tuple of the item that we just updated
        return new Object[]{
                // ITEM ID
                item_id,
                // SELLER ID
                seller_id,
                // ITEM_NAME
                null,
                // CURRENT PRICE
                i_current_price,
                // NUM BIDS
                i_num_bids,
                // END DATE
                i_end_date,
                // STATUS
                i_status.ordinal(),
                // PURCHASE ID
                ip_id,
                // BID ID
                ib_id,
                // BUYER ID
                ib_buyer_id,
        };
    }
}