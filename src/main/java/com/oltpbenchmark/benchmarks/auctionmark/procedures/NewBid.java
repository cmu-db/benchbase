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
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemId;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * NewBid
 *
 * @author pavlo
 * @author visawee
 */
public class NewBid extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(NewBid.class);

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public final SQLStmt getItem = new SQLStmt(
            "SELECT i_initial_price, i_current_price, i_num_bids, i_end_date, i_status " +
                    "FROM " + AuctionMarkConstants.TABLENAME_ITEM + " " +
                    "WHERE i_id = ? AND i_u_id = ? "
    );

    public final SQLStmt getMaxBidId = new SQLStmt(
            "SELECT MAX(ib_id) " +
                    "  FROM " + AuctionMarkConstants.TABLENAME_ITEM_BID +
                    " WHERE ib_i_id = ? AND ib_u_id = ? "
    );

    public final SQLStmt getItemMaxBid = new SQLStmt(
            "SELECT imb_ib_id, ib_bid, ib_max_bid, ib_buyer_id " +
                    "  FROM " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + ", " +
                    AuctionMarkConstants.TABLENAME_ITEM_BID +
                    " WHERE imb_i_id = ? AND imb_u_id = ? " +
                    "   AND imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id "
    );

    public final SQLStmt updateItem = new SQLStmt(
            "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM +
                    "   SET i_num_bids = i_num_bids + 1, " +
                    "       i_current_price = ?, " +
                    "       i_updated = ? " +
                    " WHERE i_id = ? AND i_u_id = ? "
    );

    public final SQLStmt updateItemMaxBid = new SQLStmt(
            "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID +
                    "   SET imb_ib_id = ?, " +
                    "       imb_ib_i_id = ?, " +
                    "       imb_ib_u_id = ?, " +
                    "       imb_updated = ? " +
                    " WHERE imb_i_id = ? " +
                    "   AND imb_u_id = ?"
    );

    public final SQLStmt updateBid = new SQLStmt(
            "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM_BID +
                    "   SET ib_bid = ?, " +
                    "       ib_max_bid = ?, " +
                    "       ib_updated = ? " +
                    " WHERE ib_id = ? " +
                    "   AND ib_i_id = ? " +
                    "   AND ib_u_id = ? "
    );

    public final SQLStmt insertItemBid = new SQLStmt(
            "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_BID + " (" +
                    "ib_id, " +
                    "ib_i_id, " +
                    "ib_u_id, " +
                    "ib_buyer_id, " +
                    "ib_bid, " +
                    "ib_max_bid, " +
                    "ib_created, " +
                    "ib_updated " +
                    ") VALUES (" +
                    "?, " + // ib_id
                    "?, " + // ib_i_id
                    "?, " + // ib_u_id
                    "?, " + // ib_buyer_id
                    "?, " + // ib_bid
                    "?, " + // ib_max_bid
                    "?, " + // ib_created
                    "? " + // ib_updated
                    ")"
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

    public Object[] run(Connection conn, Timestamp[] benchmarkTimes,
                        String item_id, String seller_id, String buyer_id, double newBid, Timestamp estimatedEndDate) throws SQLException {
        final Timestamp currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);

        LOG.debug(String.format("Attempting to place new bid on Item %s [buyer=%s, bid=%.2f]",
                item_id, buyer_id, newBid));



        // Check to make sure that we can even add a new bid to this item
        // If we fail to get back an item, then we know that the auction is closed
        double i_initial_price;
        double i_current_price;
        long i_num_bids;
        Timestamp i_end_date;
        ItemStatus i_status;

        try (PreparedStatement stmt = this.getPreparedStatement(conn, getItem, item_id, seller_id)) {
            try (ResultSet results = stmt.executeQuery()) {
                if (!results.next()) {
                    throw new UserAbortException("Invalid item " + item_id);
                }
                int col = 1;
                i_initial_price = results.getDouble(col++);
                i_current_price = results.getDouble(col++);
                i_num_bids = results.getLong(col++);
                i_end_date = results.getTimestamp(col++);
                i_status = ItemStatus.get(results.getLong(col));

            }
        }

        long newBidId = 0;
        String newBidMaxBuyerId = buyer_id;



        // If we existing bids, then we need to figure out whether we are the new highest
        // bidder or if the existing one just has their max_bid bumped up
        if (i_num_bids > 0) {
            // Get the next ITEM_BID id for this item
            LOG.debug("Retrieving ITEM_MAX_BID information for {}", ItemId.toString(item_id));
            try (PreparedStatement stmt = this.getPreparedStatement(conn, getMaxBidId, item_id, seller_id)) {
                try (ResultSet results = stmt.executeQuery()) {
                    results.next();

                    newBidId = results.getLong(1) + 1;
                }
            }

            // Get the current max bid record for this item
            long currentBidId;
            double currentBidAmount;
            double currentBidMax;
            String currentBuyerId;
            try (PreparedStatement stmt = this.getPreparedStatement(conn, getItemMaxBid, item_id, seller_id)) {
                try (ResultSet results = stmt.executeQuery()) {
                    results.next();

                    int col = 1;
                    currentBidId = results.getLong(col++);
                    currentBidAmount = results.getDouble(col++);
                    currentBidMax = results.getDouble(col++);
                    currentBuyerId = results.getString(col);
                }
            }

            boolean updateMaxBid = false;
            // Check whether this bidder is already the max bidder
            // This means we just need to increase their current max bid amount without
            // changing the current auction price
            if (buyer_id.equals(currentBuyerId)) {
                if (newBid < currentBidMax) {
                    String msg = String.format("%s is already the highest bidder for Item %s but is trying to " +
                                    "set a new max bid %.2f that is less than current max bid %.2f",
                            buyer_id, item_id, newBid, currentBidMax);
                    LOG.debug(msg);
                    throw new UserAbortException(msg);
                }
                try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, updateBid, i_current_price,
                        newBid,
                        currentTime,
                        currentBidId,
                        item_id,
                        seller_id)) {
                    preparedStatement.executeUpdate();
                }
                LOG.debug(String.format("Increasing the max bid the highest bidder %s from %.2f to %.2f for Item %s",
                        buyer_id, currentBidMax, newBid, item_id));
            }
            // Otherwise check whether this new bidder's max bid is greater than the current max
            else {
                // The new maxBid trumps the existing guy, so our the buyer_id for this txn becomes the new
                // winning bidder at this time. The new current price is one step above the previous
                // max bid amount 
                if (newBid > currentBidMax) {
                    i_current_price = Math.min(newBid, currentBidMax + (i_initial_price * AuctionMarkConstants.ITEM_BID_PERCENT_STEP));

                    // Defer the update to ITEM_MAX_BID until after we insert our new ITEM_BID record
                    updateMaxBid = true;
                }
                // The current max bidder is still the current one
                // We just need to bump up their bid amount to be at least the bidder's amount
                // Make sure that we don't go over the the currentMaxBidMax, otherwise this would mean
                // that we caused the user to bid more than they wanted.
                else {
                    newBidMaxBuyerId = currentBuyerId;
                    i_current_price = Math.min(currentBidMax, newBid + (i_initial_price * AuctionMarkConstants.ITEM_BID_PERCENT_STEP));

                    try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, updateBid, i_current_price,
                            i_current_price,
                            currentTime,
                            currentBidId,
                            item_id,
                            seller_id)) {
                        preparedStatement.executeUpdate();
                    }
                    LOG.debug(String.format("Keeping the existing highest bidder of Item %s as %s but updating current price from %.2f to %.2f",
                            item_id, buyer_id, currentBidAmount, i_current_price));
                }

                // Always insert an new ITEM_BID record even if BuyerId doesn't become
                // the new highest bidder. We also want to insert a new record even if
                // the BuyerId already has ITEM_BID record, because we want to maintain
                // the history of all the bid attempts
                try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, insertItemBid, newBidId,
                        item_id,
                        seller_id,
                        buyer_id,
                        i_current_price,
                        newBid,
                        currentTime,
                        currentTime)) {
                    preparedStatement.executeUpdate();
                }
                try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, updateItem, i_current_price,
                        currentTime,
                        item_id,
                        seller_id)) {
                    preparedStatement.executeUpdate();
                }

                // This has to be done after we insert the ITEM_BID record to make sure
                // that the HSQLDB test cases work
                if (updateMaxBid) {
                    try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, updateItemMaxBid, newBidId,
                            item_id,
                            seller_id,
                            currentTime,
                            item_id,
                            seller_id)) {
                        preparedStatement.executeUpdate();
                    }
                    LOG.debug(String.format("Changing new highest bidder of Item %s to %s [newMaxBid=%.2f > currentMaxBid=%.2f]",
                            item_id, buyer_id, newBid, currentBidMax));
                }
            }
        }
        // There is no existing max bid record, therefore we can just insert ourselves
        else {
            try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, insertItemBid, newBidId,
                    item_id,
                    seller_id,
                    buyer_id,
                    i_initial_price,
                    newBid,
                    currentTime,
                    currentTime)) {
                preparedStatement.executeUpdate();
            }
            try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, insertItemMaxBid, item_id,
                    seller_id,
                    newBidId,
                    item_id,
                    seller_id,
                    currentTime,
                    currentTime)) {
                preparedStatement.executeUpdate();
            }
            try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, updateItem, i_current_price,
                    currentTime,
                    item_id,
                    seller_id)) {
                preparedStatement.execute();
            }
            LOG.debug(String.format("Creating the first bid record for Item %s and setting %s as highest bidder at %.2f",
                    item_id, buyer_id, i_current_price));
        }

        // Return back information about the current state of the item auction
        return new Object[]{
                // ITEM_ID
                item_id,
                // SELLER_ID
                seller_id,
                // ITEM_NAME
                null, // ignore
                // CURRENT PRICE
                i_current_price,
                // NUM BIDS
                i_num_bids + 1,
                // END DATE
                i_end_date,
                // STATUS
                i_status.ordinal(),
                // MAX BID ID
                newBidId,
                // MAX BID BUYER_ID
                newBidMaxBuyerId,
        };
    }
}
