/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *                                                                         *
 *  Modifications by:                                                      *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
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
package com.oltpbenchmark.benchmarks.tatp.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tatp.TATPConstants;

public class InsertCallForwarding extends Procedure {

    public final SQLStmt getSubscriber = new SQLStmt(
        "SELECT s_id FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE sub_nbr = ?"
    );

    public final SQLStmt getSpecialFacility = new SQLStmt(
        "SELECT sf_type FROM " + TATPConstants.TABLENAME_SPECIAL_FACILITY + " WHERE s_id = ?"
    );

    public final SQLStmt insertCallForwarding = new SQLStmt(
        "INSERT INTO " + TATPConstants.TABLENAME_CALL_FORWARDING + " VALUES (?, ?, ?, ?, ?)"
    );
     
    public long run(Connection conn, String sub_nbr, byte sf_type, byte start_time, byte end_time, String numberx) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getSubscriber);
        stmt.setString(1, sub_nbr);
        ResultSet results = stmt.executeQuery();
        assert(results != null);
        long s_id=-1;
        if(results.next())
        {
            s_id = results.getLong(1);
        }
        assert s_id!=-1; 
        results.close();
        stmt = this.getPreparedStatement(conn, getSpecialFacility);
        stmt.setLong(1, s_id);
        ResultSet results2 = stmt.executeQuery();
        assert(results2 != null);
        results2.close();
         
        // Inserting a new CALL_FORWARDING record only succeeds 30% of the time
        stmt = this.getPreparedStatement(conn, insertCallForwarding);
        stmt.setLong(1, s_id);
        stmt.setByte(2, sf_type);
        stmt.setByte(3, start_time);
        stmt.setByte(4, end_time);
        stmt.setString(5, numberx);
        int rows_updated = -1;
        
        try {
            rows_updated = stmt.executeUpdate();
        } catch (SQLException ex) {
            throw new UserAbortException("Failed to insert a row in " + TATPConstants.TABLENAME_CALL_FORWARDING);
        }
        return (rows_updated);
    }
}