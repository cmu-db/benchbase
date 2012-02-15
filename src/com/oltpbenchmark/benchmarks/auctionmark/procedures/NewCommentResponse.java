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
import java.sql.Timestamp;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;

/**
 * NewCommentResponse
 * @author pavlo
 * @author visawee
 */
public class NewCommentResponse extends Procedure {
	
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt updateComment = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM_COMMENT + " " +
        	"SET ic_response = ?, " +
        	"    ic_updated = ? " +
        "WHERE ic_id = ? AND ic_i_id = ? AND ic_u_id = ? "
    );
    
    public final SQLStmt updateUser = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USERACCT + " " +
           "SET u_comments = u_comments - 1, " +
           "    u_updated = ? " +
        " WHERE u_id = ?"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public void run(Connection conn, Timestamp benchmarkTimes[],
                    long item_id, long seller_id, long comment_id, String response) throws SQLException {
        final Timestamp currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);
        this.getPreparedStatement(conn, updateComment, response, currentTime, comment_id, item_id, seller_id).executeUpdate();
        this.getPreparedStatement(conn, updateUser, currentTime, seller_id).executeUpdate();
        return;
    }	
}