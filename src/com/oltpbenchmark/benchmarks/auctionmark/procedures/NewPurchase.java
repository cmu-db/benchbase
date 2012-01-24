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

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants.ItemStatus;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;

/**
 * NewPurchase
 * Description goes here...
 * @author visawee
 */
public class NewPurchase extends Procedure {
    
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt getItemInfo = new SQLStmt(
        "SELECT i_num_bids, i_current_price, i_end_date, " +
        "       ib_id, ib_buyer_id, " +
        "       u_balance " +
		"  FROM " + AuctionMarkConstants.TABLENAME_ITEM + ", " +
		            AuctionMarkConstants.TABLENAME_ITEM_MAX_BID + ", " +
		            AuctionMarkConstants.TABLENAME_ITEM_BID + ", " +
		            AuctionMarkConstants.TABLENAME_USER +
        " WHERE i_id = ? AND i_u_id = ? AND i_status = " + ItemStatus.WAITING_FOR_PURCHASE.ordinal() +
        "   AND imb_i_id = i_id AND imb_u_id = i_u_id " +
        "   AND imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id " +
        "   AND ib_buyer_id = u_id "
    );

    public final SQLStmt getBuyerInfo = new SQLStmt(
        "SELECT u_id, u_balance " +
        "  FROM " + AuctionMarkConstants.TABLENAME_USER +
        " WHERE u_id = ? "
    );
    
    public final SQLStmt insertPurchase = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_PURCHASE + "(" +
        	"ip_id," +
        	"ip_ib_id," +
        	"ip_ib_i_id," +  
        	"ip_ib_u_id," +  
        	"ip_date" +     
        ") VALUES(?,?,?,?,?)"
    );
    
    public final SQLStmt updateItem = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM + " " +
        	"SET i_status = " + ItemStatus.CLOSED.ordinal() + ", " +
        	"    i_updated = ? " +
        "WHERE i_id = ? AND i_u_id = ? "
    );    
    
    public final SQLStmt updateUserItem = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USER_ITEM + " " +
           "SET ui_ip_id = ?, " +
           "    ui_ip_ib_id = ?, " +
           "    ui_ip_ib_i_id = ?, " +
           "    ui_ip_ib_u_id = ?" +
        " WHERE ui_u_id = ? AND ui_i_id = ? AND ui_i_u_id = ?"
    );
    
    public final SQLStmt updateUserBalance = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USER + " " +
           "SET u_balance = u_balance + ? " + 
        " WHERE u_id = ?"
    );
    
    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public Object[] run(Connection conn, Date benchmarkTimes[],
                        long item_id, long seller_id, double buyer_credit) throws SQLException {
        final Date currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);
        
        PreparedStatement stmt = null;
        ResultSet results = null;
        int updated;
        
        // Get the ITEM_MAX_BID record so that we know what we need to process
        stmt = this.getPreparedStatement(conn, getItemInfo, item_id, seller_id);
        results = stmt.executeQuery();
        if (results.next() == false) {
            throw new UserAbortException("No ITEM_MAX_BID is available record for item " + item_id);
        }
        int col = 1;
        long i_num_bids = results.getLong(col++);
        double i_current_price = results.getDouble(col++);
        Date i_end_date = results.getDate(col++);
        ItemStatus i_status = ItemStatus.CLOSED;
        long ib_id = results.getLong(col++);
        long ib_buyer_id = results.getLong(col++);
        double u_balance = results.getDouble(col++);
        
        // Make sure that the buyer has enough money to cover this charge
        // We can add in a credit for the buyer's account
        if (i_current_price > (buyer_credit + u_balance)) {
            throw new UserAbortException(String.format("Buyer does not have enough money in account to purchase item " +
                                                       "[maxBid=%.2f, balance=%.2f, credit=%.2f]",
                                                       i_current_price, u_balance, buyer_credit));
        }

        // Set item_purchase_id
        long ip_id = AuctionMarkUtil.getUniqueElementId(item_id, 1);

        // Insert a new purchase
        // System.err.println(String.format("NewPurchase: ip_id=%d, ib_bid=%.2f, item_id=%d, seller_id=%d", ip_id, ib_bid, item_id, seller_id));
        updated = this.getPreparedStatement(conn, insertPurchase, ip_id, ib_id, item_id, seller_id, currentTime).executeUpdate();
        assert(updated == 1);
        
        // Update item status to close
        updated = this.getPreparedStatement(conn, updateItem, currentTime, item_id, seller_id).executeUpdate();
        assert(updated == 1);
        
        // And update this the USER_ITEM record to link it to the new ITEM_PURCHASE record
        updated = this.getPreparedStatement(conn, updateUserItem, ip_id, ib_id, item_id, seller_id, ib_buyer_id, item_id, seller_id).executeUpdate();
        assert(updated == 1);
        
        // Decrement the buyer's account 
        updated = this.getPreparedStatement(conn, updateUserBalance, -1*(i_current_price) + buyer_credit, ib_buyer_id).executeUpdate();
        assert(updated == 1);
        
        // And credit the seller's account
        this.getPreparedStatement(conn, updateUserBalance, i_current_price, seller_id).executeUpdate();
        assert(updated == 1);
        
        // Return a tuple of the item that we just updated
        return new Object[] {
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