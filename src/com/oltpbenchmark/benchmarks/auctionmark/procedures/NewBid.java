/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package com.oltpbenchmark.benchmarks.auctionmark.procedures;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.Procedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.Date;

import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants.ItemStatus;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkProfile;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemId;
import com.oltpbenchmark.benchmarks.auctionmark.util.UserId;

/**
 * NewBid
 * @author pavlo
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 2",
    singlePartition = true
)
public class NewBid extends Procedure {
    private static final Logger LOG = Logger.getLogger(NewBid.class);

    // -----------------------------------------------------------------
    // STATIC MEMBERS
    // -----------------------------------------------------------------
    
    private static final VoltTable.ColumnInfo[] RESULT_COLS = {
        new VoltTable.ColumnInfo("i_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("i_u_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("i_num_bids", VoltType.BIGINT),
        new VoltTable.ColumnInfo("i_current_price", VoltType.FLOAT),
        new VoltTable.ColumnInfo("i_end_date", VoltType.TIMESTAMP),
        new VoltTable.ColumnInfo("ib_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ib_buyer_id", VoltType.BIGINT),
    };
    
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public final SQLStmt getItem = new SQLStmt(
        "SELECT i_initial_price, i_current_price, i_num_bids, i_end_date, i_status " +
          "FROM " + AuctionMarkConstants.TABLENAME_ITEM + " " + 
         "WHERE i_id = ? AND i_u_id = ? " //+
//         "  AND i_end_date > ? " +
//         "  AND i_status = " + ItemStatus.OPEN
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
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_BID + "(" +
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
        "? "  + // ib_updated
        ")"
    );

    public final SQLStmt insertItemMaxBid = new SQLStmt(
    	"INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + "(" +
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
        "? "  + // imb_updated
        ")"
    );

    public VoltTable run(Connection conn, Date benchmarkTimes[], long item_id, long seller_id, long buyer_id, double newBid, Date estimatedEndDate) {
        final Date currentTime = AuctionMarkProfile.getScaledTimestamp(benchmarkTimes[0], benchmarkTimes[1], new Date());
        final boolean debug = LOG.isDebugEnabled();
        if (debug) LOG.debug(String.format("Attempting to place new bid on Item %d [buyer=%d, bid=%.2f]", item_id, buyer_id, newBid));

        // Check to make sure that we can even add a new bid to this item
        // If we fail to get back an item, then we know that the auction is closed
        this.getPreparedStatement(conn, getItem, item_id, seller_id); // , currentTime);
        VoltTable[] results = voltExecuteSQL();
        assert (results.length == 1);
        if (results[0].getRowCount() == 0) {
            if (debug) 
                LOG.debug("The auction for item " + item_id + " has ended - " + currentTime);
            throw new VoltAbortException("Unable to bid on item: Auction has ended");
        }
        boolean advRow = results[0].advanceRow();
        assert (advRow);
        double i_initial_price = results[0].getDouble(0);
        double i_current_price = results[0].getDouble(1);
        long i_num_bids = results[0].getLong(2);
        Date i_end_date = results[0].getTimestampAsTimestamp(3);
        ItemStatus i_status = ItemStatus.get(results[0].getLong(4));
        long newBidId = 0;
        long newBidMaxBuyerId = buyer_id;
        
        if (i_end_date.compareTo(currentTime) < 0 || i_status != ItemStatus.OPEN) {
            if (debug)
                LOG.debug(String.format("The auction for item %d has ended [status=%s]\nCurrentTime:\t%s\nActualEndDate:\t%s\nEstimatedEndDate:\t%s",
                                        item_id, i_status, currentTime, i_end_date, estimatedEndDate));
            throw new VoltAbortException("Unable to bid on item: Auction has ended");
        }
        
        // If we existing bids, then we need to figure out whether we are the new highest
        // bidder or if the existing one just has their max_bid bumped up
        if (i_num_bids > 0) {
            if (debug) LOG.debug("Retrieving ITEM_MAX_BID information for " + ItemId.toString(item_id));
            this.getPreparedStatement(conn, getMaxBidId, item_id, seller_id);
            this.getPreparedStatement(conn, getItemMaxBid, item_id, seller_id);
            results = voltExecuteSQL();
            
            // Get the next ITEM_BID id for this item
            boolean advanceRow = results[0].advanceRow();
            assert (advanceRow);
            newBidId = results[0].getLong(0) + 1;
            assert(results[0].wasNull() == false);

            // Get the current max bid record for this item
            advanceRow = results[1].advanceRow();
            assert (advanceRow);
            long currentBidId = results[1].getLong(0);
            double currentBidAmount = results[1].getDouble(1);
            double currentBidMax = 0.0d;
            try {
                currentBidMax = results[1].getDouble(2);
            } catch (IllegalArgumentException ex) {
                SQLStmt last[] = voltLastQueriesExecuted();
                for (int i = 0; i < last.length; i++) {
                    LOG.error(last[i].getText() + "\n" +  results[i]);    
                } // FOR
                throw ex;
            }
            long currentBuyerId = results[1].getLong(3);
            assert((int)currentBidAmount == (int)i_current_price) : String.format("%.2f == %.2f", currentBidAmount, i_current_price);
            
            // Check whether this bidder is already the max bidder
            // This means we just need to increase their current max bid amount without
            // changing the current auction price
            if (buyer_id == currentBuyerId) {
                if (newBid < currentBidMax) {
                    String msg = String.format("%s is already the highest bidder for Item %d but is trying to " +
                                               "set a new max bid %.2f that is less than current max bid %.2f",
                                               buyer_id, item_id, newBid, currentBidMax);
                    if (debug) LOG.debug(msg);
                    throw new VoltAbortException(msg);
                }
                this.getPreparedStatement(conn, updateBid, i_current_price, newBid, currentTime, currentBidId, item_id, seller_id);
                if (debug) LOG.debug(String.format("Increasing the max bid the highest bidder %s from %.2f to %.2f for Item %d",
                                                   buyer_id, currentBidMax, newBid, item_id));
            }
            // Otherwise check whether this new bidder's max bid is greater than the current max
            else {
                // The new maxBid trumps the existing guy, so our the buyer_id for this txn becomes the new
                // winning bidder at this time. The new current price is one step above the previous
                // max bid amount 
                if (newBid > currentBidMax) {
                    i_current_price = Math.min(newBid, currentBidMax + (i_initial_price * AuctionMarkConstants.ITEM_BID_PERCENT_STEP));
                    assert(i_current_price > currentBidMax);
                    this.getPreparedStatement(conn, updateItemMaxBid, newBidId, item_id, seller_id, currentTime, item_id, seller_id);
                    if (debug) LOG.debug(String.format("Changing new highest bidder of Item %d to %s [newMaxBid=%.2f > currentMaxBid=%.2f]",
                                                        item_id, UserId.toString(buyer_id), newBid, currentBidMax));

                }
                // The current max bidder is still the current one
                // We just need to bump up their bid amount to be at least the bidder's amount
                // Make sure that we don't go over the the currentMaxBidMax, otherwise this would mean
                // that we caused the user to bid more than they wanted.
                else {
                    newBidMaxBuyerId = currentBuyerId;
                    i_current_price = Math.min(currentBidMax, newBid + (i_initial_price * AuctionMarkConstants.ITEM_BID_PERCENT_STEP));
                    assert(i_current_price >= newBid) : String.format("%.2f > %.2f", i_current_price, newBid);
                    this.getPreparedStatement(conn, updateBid, i_current_price, i_current_price, currentTime, currentBidId, item_id, seller_id);
                    if (debug) LOG.debug(String.format("Keeping the existing highest bidder of Item %d as %s but updating current price from %.2f to %.2f",
                                                       item_id, buyer_id, currentBidAmount, i_current_price));
                }
            
                // Always insert an new ITEM_BID record even if BuyerId doesn't become
                // the new highest bidder. We also want to insert a new record even if
                // the BuyerId already has ITEM_BID record, because we want to maintain
                // the history of all the bid attempts
                this.getPreparedStatement(conn, insertItemBid, newBidId, item_id, seller_id, buyer_id,
                                            i_current_price, newBid, currentTime, currentTime);
                this.getPreparedStatement(conn, updateItem, i_current_price, currentTime, item_id, seller_id);
            }
        }
        // There is no existing max bid record, therefore we can just insert ourselves
        else {
            this.getPreparedStatement(conn, insertItemBid, newBidId, item_id, seller_id, buyer_id, i_initial_price, newBid, currentTime, currentTime);
            this.getPreparedStatement(conn, insertItemMaxBid, item_id, seller_id, newBidId, item_id, seller_id, currentTime, currentTime);
            this.getPreparedStatement(conn, updateItem, i_current_price, currentTime, item_id, seller_id);
            if (debug) LOG.debug(String.format("Creating the first bid record for Item %d and setting %s as highest bidder at %.2f",
                                               item_id, buyer_id, i_current_price));
        }
        
        // Fire off all of our queue
        // We don't have to worry about checking whether the auction ended before we could update it
        // because we use the timestamp of when the txn started, not when we updated it
        results = voltExecuteSQL(true);
        assert (results.length > 0);
       
        // Return back information about the current state of the item auction
        VoltTable ret = new VoltTable(RESULT_COLS);
        ret.addRow(new Object[] {
            // ITEM_ID
            item_id,
            // SELLER_ID
            seller_id,
            // NUM BIDS
            i_num_bids + 1,
            // CURRENT PRICE
            i_current_price,
            // END DATE
            i_end_date,
            // MAX BID ID
            newBidId,
            // MAX BID BUYER_ID
            newBidMaxBuyerId,
        });
        return ret;
    }
}
