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

public class GetNewDestination extends Procedure {

     public final SQLStmt getNewDestination = new SQLStmt(
         "SELECT cf.numberx " +
         "  FROM " + TATPConstants.TABLENAME_SPECIAL_FACILITY + " sf, " +
         "       " + TATPConstants.TABLENAME_CALL_FORWARDING + " cf " +
         " WHERE sf.s_id = ? " +
         "   AND sf.sf_type = ? " +
         "   AND sf.is_active = 1 " +
         "   AND cf.s_id = sf.s_id " +
         "   AND cf.sf_type = sf.sf_type " +
         "   AND cf.start_time <= ? " +
         "   AND cf.end_time > ?"
     );

     public void run(Connection conn, long s_id, byte sf_type, byte start_time, byte end_time) throws SQLException {
    	 PreparedStatement stmt = this.getPreparedStatement(conn, getNewDestination);
    	 stmt.setLong(1, s_id);
    	 stmt.setByte(2, sf_type);
    	 stmt.setByte(3, start_time);
    	 stmt.setByte(4, end_time);
    	 ResultSet results = stmt.executeQuery();
    	 assert(results != null);
    	 results.close();
     }
}
