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


package com.oltpbenchmark.benchmarks.auctionmark.procedures;

import java.sql.Connection;
import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemStatus;

/**
 * PostAuction
 * @author pavlo
 * @author visawee
 */
public class CloseAuctions extends Procedure {
    private static final Logger LOG = Logger.getLogger(CloseAuctions.class);

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
    
    /**
     * @param item_ids - Item Ids
     * @param seller_ids - Seller Ids
     * @param bid_ids - ItemBid Ids
     * @return
     */
    public List<Object[]> run(Connection conn, Timestamp benchmarkTimes[],
                              Timestamp startTime, Timestamp endTime) throws SQLException {
        final Timestamp currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);
        final boolean debug = LOG.isDebugEnabled();

//        int orig_isolation = conn.getTransactionIsolation();
//        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        
        if (debug)
            LOG.debug(String.format("startTime=%s, endTime=%s, currentTime=%s",
                                    startTime, endTime, currentTime));

        int closed_ctr = 0;
        int waiting_ctr = 0;
        int round = AuctionMarkConstants.CLOSE_AUCTIONS_ROUNDS;
        int updated = -1;
        int col = -1;
        int param = -1;
        
        PreparedStatement dueItemsStmt = this.getPreparedStatement(conn, getDueItems);
        ResultSet dueItemsTable = null;
        PreparedStatement maxBidStmt = this.getPreparedStatement(conn, getMaxBid);
        ResultSet maxBidResults = null;
        
        final List<Object[]> output_rows = new ArrayList<Object[]>();
        while (round-- > 0) {
            param = 1;
            dueItemsStmt.setTimestamp(param++, startTime);
            dueItemsStmt.setTimestamp(param++, endTime);
            dueItemsStmt.setInt(param++, ItemStatus.OPEN.ordinal());
            dueItemsTable = dueItemsStmt.executeQuery();
            boolean adv = dueItemsTable.next();
            if (adv == false) break;

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
                
                if (debug)
                    LOG.debug(String.format("Getting max bid for itemId=%d / sellerId=%d", itemId, sellerId));
                assert(itemStatus == ItemStatus.OPEN);
                
                // Has bid on this item - set status to WAITING_FOR_PURCHASE
                // We'll also insert a new USER_ITEM record as needed
                // We have to do this extra step because H-Store doesn't have good support in the
                // query optimizer for LEFT OUTER JOINs
                if (numBids > 0) {
                    waiting_ctr++;
                    
                    param = 1;
                    maxBidStmt.setLong(param++, itemId);
                    maxBidStmt.setLong(param++, sellerId);
                    maxBidResults = maxBidStmt.executeQuery();
                    adv = maxBidResults.next();
                    assert(adv);
                    
                    col = 1;
                    bidId = maxBidResults.getLong(col++);
                    buyerId = maxBidResults.getLong(col++);
                    updated = this.getPreparedStatement(conn, insertUserItem, buyerId, itemId, sellerId, currentTime).executeUpdate();
                    assert(updated == 1);
                    itemStatus = ItemStatus.WAITING_FOR_PURCHASE;
                    maxBidResults.close();
                }
                // No bid on this item - set status to CLOSED
                else {
                    closed_ctr++;
                    itemStatus = ItemStatus.CLOSED;
                }
                
                // Update Status!
                updated = this.getPreparedStatement(conn, updateItemStatus, itemStatus.ordinal(), currentTime, itemId, sellerId).executeUpdate();
                if (debug)
                    LOG.debug(String.format("Updated Status for Item %d => %s", itemId, itemStatus));
                
                Object row[] = new Object[] {
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
            } // WHILE
            dueItemsTable.close();
            if (round > 0) conn.commit();
        } // WHILE

        if (debug)
            LOG.debug(String.format("Updated Auctions - Closed=%d / Waiting=%d", closed_ctr, waiting_ctr));
//        conn.setTransactionIsolation(orig_isolation);
        return (output_rows);
    }
}