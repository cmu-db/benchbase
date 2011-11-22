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
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tatp.TATPConstants;

public class UpdateSubscriberData extends Procedure {

    public final SQLStmt updateSubscriber = new SQLStmt(
        "UPDATE " + TATPConstants.TABLENAME_SUBSCRIBER + " SET bit_1 = ? WHERE s_id = ?"
    );

    public final SQLStmt updateSpecialFacility = new SQLStmt(
        "UPDATE " + TATPConstants.TABLENAME_SPECIAL_FACILITY + " SET data_a = ? WHERE s_id = ? AND sf_type = ?"
    );

    public long run(Connection conn, long s_id, byte bit_1, short data_a, byte sf_type) throws SQLException {
    	PreparedStatement stmt = this.getPreparedStatement(conn, updateSubscriber);
    	stmt.setByte(1, bit_1);
    	stmt.setLong(2, s_id);
    	int updated = stmt.executeUpdate();
    	assert(updated == 1);
    	
    	stmt = this.getPreparedStatement(conn, updateSpecialFacility);
    	stmt.setShort(1, data_a);
    	stmt.setLong(2, s_id);
    	stmt.setByte(3, sf_type);
    	updated = stmt.executeUpdate();
    	if (updated != 0) {
    	    throw new UserAbortException("Failed to update a row in " + TATPConstants.TABLENAME_SPECIAL_FACILITY);
    	}
    	return (updated);
    }
}