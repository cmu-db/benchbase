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

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.util.AuctionMarkUtil;

public class NewFeedback extends Procedure {
    private static final Logger LOG = Logger.getLogger(NewFeedback.class);
    
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt getMaxFeedback = new SQLStmt(
        "SELECT MAX(uf_id) " + 
        "  FROM " + AuctionMarkConstants.TABLENAME_USER_FEEDBACK + " " + 
        " WHERE uf_i_id = ? AND uf_u_id = ?"
    );
	
    public final SQLStmt insertFeedback = new SQLStmt(
        "INSERT INTO " + AuctionMarkConstants.TABLENAME_USER_FEEDBACK + "( " +
        	"uf_id," +
        	"uf_u_id," +
        	"uf_i_id," +
        	"uf_i_u_id," +
        	"uf_from_id," +
        	"uf_rating," +
        	"uf_date," +
        	"uf_sattr0" +
        ") VALUES (" +
            "?," + // UF_ID
            "?," + // UF_U_ID
            "?," + // UF_I_ID
            "?," + // UF_I_U_ID
            "?," + // UF_FROM_ID
            "?," + // UF_RATING
            "?," + // UF_DATE
            "?"  + // UF_SATTR0
        ")"
    );
    
    public final SQLStmt updateUser = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USER + " " +
           "SET u_rating = u_rating + ?, " +
           "    u_updated = ? " +
        " WHERE u_id = ?"
    );
    
    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public long run(Connection conn, Date benchmarkTimes[],
                    long i_id, long seller_id, long buyer_id, long rating, String comment) throws SQLException {
        final Date currentTime = AuctionMarkUtil.getProcTimestamp(benchmarkTimes);
        final boolean debug = LOG.isDebugEnabled();
        if (debug) LOG.debug("NewFeedback::: selecting max feedback");

        // Set comment_id
        long if_id = 0;
        PreparedStatement stmt = this.getPreparedStatement(conn, getMaxFeedback, i_id, seller_id);
        ResultSet results = stmt.executeQuery();
        if (results.next()) {
            if_id = results.getLong(1) + 1;
        }

        if (debug) LOG.debug("NewFeedback::: if_id = " + if_id);
        this.getPreparedStatement(conn, insertFeedback, if_id, seller_id, i_id, seller_id, buyer_id, rating, currentTime, comment).executeUpdate();
        this.getPreparedStatement(conn, updateUser, rating, currentTime, seller_id).executeUpdate();
        if (debug) LOG.debug("NewFeedback::: feedback inserted ");

        // Return new if_id
        return (if_id);
    }
}