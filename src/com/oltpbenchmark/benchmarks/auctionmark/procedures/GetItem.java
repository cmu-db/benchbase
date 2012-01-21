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
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants.ItemStatus;

/**
 * Get Item Information
 * Returns all of the attributes for a particular item
 * @author pavlo
 */
public class GetItem extends Procedure {

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt getItem = new SQLStmt(
        "SELECT i_id, i_u_id, i_initial_price, i_current_price, i_num_bids, i_end_date, i_status " +
          "FROM " + AuctionMarkConstants.TABLENAME_ITEM + " " + 
         "WHERE i_id = ? AND i_u_id = ? AND i_status = " + ItemStatus.OPEN.ordinal()
    );
    
    public final SQLStmt getUser = new SQLStmt(
        "SELECT u_id, u_rating, u_created, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name " +
        "  FROM " + AuctionMarkConstants.TABLENAME_USER + ", " +
                    AuctionMarkConstants.TABLENAME_REGION +
        " WHERE u_id = ? AND u_r_id = r_id"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public ResultSet[] run(Connection conn, Date benchmarkTimes[], long item_id, long seller_id) throws SQLException {
        PreparedStatement item_stmt = this.getPreparedStatement(conn, getItem, item_id, seller_id);
        ResultSet item_results = item_stmt.executeQuery();
        
        PreparedStatement user_stmt = this.getPreparedStatement(conn, getUser, seller_id);
        ResultSet user_results = user_stmt.executeQuery();
        
        return (new ResultSet[]{ item_results, user_results });
    }
    
}
