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

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.Procedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.types.Date;

import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkProfile;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;

/**
 * NewComment
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 2",
    singlePartition = true
)
public class NewComment extends Procedure{
	
    // -----------------------------------------------------------------
    // STATIC MEMBERS
    // -----------------------------------------------------------------
    
    private static final ColumnInfo[] RESULT_COLS = {
        new VoltTable.ColumnInfo("ic_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ic_i_id", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ic_u_id", VoltType.BIGINT),
    };
    
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt getMaxItemCommentId = new SQLStmt(
        "SELECT MAX(ic_id) " + 
        "  FROM " + AuctionMarkConstants.TABLENAME_ITEM_COMMENT + 
        " WHERE ic_i_id = ? AND ic_u_id = ?"
    );
	
    public final SQLStmt insertItemComment = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_ITEM_COMMENT + "(" +
        	"ic_id," +
        	"ic_i_id," +
        	"ic_u_id," +
        	"ic_buyer_id," +
        	"ic_question, " +
        	"ic_created," +
        	"ic_updated " +
        ") VALUES (?,?,?,?,?,?,?)"
    );
    
    public final SQLStmt updateUser = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USER + " " +
           "SET u_comments = u_comments + 1, " +
           "    u_updated = ? " +
        " WHERE u_id = ?"
    );
	
    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public VoltTable run(Connection conn, Date benchmarkTimes[], long item_id, long seller_id, long buyer_id, String question) {
        final Date currentTime = AuctionMarkProfile.getScaledTimestamp(benchmarkTimes[0], benchmarkTimes[1], new Date());
    	long ic_id;
    	
        // Set comment_id
        this.getPreparedStatement(conn, getMaxItemCommentId, item_id, seller_id);
        VoltTable[] results = voltExecuteSQL();
        assert (1 == results.length);
        if (0 == results[0].getRowCount()) {
            ic_id = 0;
        } else {
            boolean adv = results[0].advanceRow();
            assert(adv);
            ic_id = results[0].getLong(0) + 1;
        }

        this.getPreparedStatement(conn, insertItemComment, ic_id, item_id, seller_id, buyer_id, question, currentTime, currentTime);
        this.getPreparedStatement(conn, updateUser, currentTime, seller_id);
        voltExecuteSQL();

        // Return new ic_id
        VoltTable ret = new VoltTable(RESULT_COLS);
        ret.addRow(ic_id, item_id, seller_id);
        return ret;
    }	
	
}