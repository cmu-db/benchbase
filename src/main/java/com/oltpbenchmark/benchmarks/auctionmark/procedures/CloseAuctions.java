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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * PostAuction
 *
 * @author pavlo
 * @author visawee
 */
public class CloseAuctions extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(CloseAuctions.class);

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public final SQLStmt getDueItems = new SQLStmt(
            "SELECT " + AuctionMarkConstants.ITEM_COLUMNS_STR +
                    " FROM " + AuctionMarkConstants.TABLENAME_ITEM + " " +
                    "WHERE (i_start_date BETWEEN ? AND ?) " +
                    "AND ? " +
                    "ORDER BY i_id ASC " +
                    "LIMIT " + AuctionMarkConstants.CLOSE_AUCTIONS_ITEMS_PER_ROUND
    );

    public final SQLStmt getMaxBid = new SQLStmt(
            "SELECT imb_ib_id, ib_buyer_id " +
                    "FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + ", " +
                    AuctionMarkConstants.TABLENAME_ITEM_BID +
                    " WHERE imb_i_id = ? AND imb_u_id = ? " +
                    "AND ib_id = imb_ib_id AND ib_i_id = imb_i_id AND ib_u_id = imb_u_id "
    );

    public final SQLStmt updateItemStatus = new SQLStmt(
            "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM + " " +
                    "SET i_status = ?, " +
                    "    i_updated = ? " +
                    "WHERE i_id = ? AND i_u_id = ? "
    );

    public final SQLStmt insertUserItem = new SQLStmt(
            "INSERT INTO " + AuctionMarkConstants.TABLENAME_USERACCT_ITEM + "(" +
                    "ui_u_id, " +
                    "ui_i_id, " +
                    "ui_i_u_id, " +
                    "ui_created" +
                    ") VALUES(?, ?, ?, ?)"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------

    public List<Object[]> run(Connection conn, Timestamp[] benchmarkTimes,
                              Timestamp startTime, Timestamp endTime) throws SQLException {
        final Timestamp currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);
        final boolean debug = LOG.isDebugEnabled();

        if (debug) {
            LOG.debug(String.format("startTime=%s, endTime=%s, currentTime=%s",
                    startTime, endTime, currentTime));
        }

        int closed_ctr = 0;
        int waiting_ctr = 0;
        int round = AuctionMarkConstants.CLOSE_AUCTIONS_ROUNDS;
        int col = -1;
        int param = -1;

        final List<Object[]> output_rows = new ArrayList<>();

        try (PreparedStatement dueItemsStmt = this.getPreparedStatement(conn, getDueItems);
             PreparedStatement maxBidStmt = this.getPreparedStatement(conn, getMaxBid)) {


            while (round-- > 0) {
                param = 1;
                dueItemsStmt.setTimestamp(param++, startTime);
                dueItemsStmt.setTimestamp(param++, endTime);
                dueItemsStmt.setInt(param++, ItemStatus.OPEN.ordinal());
                try (ResultSet dueItemsTable = dueItemsStmt.executeQuery()) {
                    boolean adv = dueItemsTable.next();
                    if (!adv) {
                        break;
                    }

                    output_rows.clear();
                    while (dueItemsTable.next()) {
                        col = 1;
                        long itemId = dueItemsTable.getLong(col++);
                        long sellerId = dueItemsTable.getLong(col++);
                        String i_name = dueItemsTable.getString(col++);
                        double currentPrice = dueItemsTable.getDouble(col++);
                        long numBids = dueItemsTable.getLong(col++);
                        Timestamp endDate = dueItemsTable.getTimestamp(col++);
                        ItemStatus itemStatus = ItemStatus.get(dueItemsTable.getLong(col++));
                        Long bidId = null;
                        Long buyerId = null;

                        if (debug) {
                            LOG.debug(String.format("Getting max bid for itemId=%d / sellerId=%d", itemId, sellerId));
                        }


                        // Has bid on this item - set status to WAITING_FOR_PURCHASE
                        // We'll also insert a new USER_ITEM record as needed
                        // We have to do this extra step because H-Store doesn't have good support in the
                        // query optimizer for LEFT OUTER JOINs
                        if (numBids > 0) {
                            waiting_ctr++;

                            param = 1;
                            maxBidStmt.setLong(param++, itemId);
                            maxBidStmt.setLong(param++, sellerId);
                            try (ResultSet maxBidResults = maxBidStmt.executeQuery()) {
                                adv = maxBidResults.next();


                                col = 1;
                                bidId = maxBidResults.getLong(col++);
                                buyerId = maxBidResults.getLong(col++);
                                try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, insertUserItem, buyerId, itemId, sellerId, currentTime)) {
                                    preparedStatement.executeUpdate();
                                }

                                itemStatus = ItemStatus.WAITING_FOR_PURCHASE;
                            }
                        }
                        // No bid on this item - set status to CLOSED
                        else {
                            closed_ctr++;
                            itemStatus = ItemStatus.CLOSED;
                        }

                        // Update Status!
                        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, updateItemStatus, itemStatus.ordinal(), currentTime, itemId, sellerId)) {
                            preparedStatement.executeUpdate();
                        }
                        if (debug) {
                            LOG.debug(String.format("Updated Status for Item %d => %s", itemId, itemStatus));
                        }

                        Object[] row = new Object[]{
                                itemId,               // i_id
                                sellerId,             // i_u_id
                                i_name,               // i_name
                                currentPrice,         // i_current_price
                                numBids,              // i_num_bids
                                endDate,              // i_end_date
                                itemStatus.ordinal(), // i_status
                                bidId,                // imb_ib_id
                                buyerId               // ib_buyer_id
                        };
                        output_rows.add(row);
                    }
                }
            }


        }

        if (debug) {
            LOG.debug(String.format("Updated Auctions - Closed=%d / Waiting=%d", closed_ctr, waiting_ctr));
        }

        return (output_rows);
    }
}