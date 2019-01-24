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


package com.oltpbenchmark.benchmarks.tatp.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tatp.TATPConstants;

public class DeleteCallForwarding extends Procedure {

	public final SQLStmt getSubscriber = new SQLStmt(
		"SELECT s_id FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE sub_nbr = ?"
	);

    public final SQLStmt updateCallForwarding = new SQLStmt(
        "DELETE FROM " + TATPConstants.TABLENAME_CALL_FORWARDING + 
        " WHERE s_id = ? AND sf_type = ? AND start_time = ?"
    );

    public long run(Connection conn, String sub_nbr, byte sf_type, byte start_time) throws SQLException {
    	PreparedStatement stmt = this.getPreparedStatement(conn, getSubscriber);
    	stmt.setString(1, sub_nbr);
    	ResultSet results = stmt.executeQuery();
    	assert(results != null);
    	long s_id=-1;
    	if(results.next())
    	{
    	    s_id = results.getLong(1);
    	}
    	results.close();
    	assert s_id!=-1; 
        stmt = this.getPreparedStatement(conn, updateCallForwarding);
        stmt.setLong(1, s_id);
        stmt.setByte(2, sf_type);
        stmt.setByte(3, start_time);
        int rows_updated = stmt.executeUpdate();
        if (rows_updated == 0) {
            throw new UserAbortException("Failed to delete a row in " + TATPConstants.TABLENAME_CALL_FORWARDING);
        }
        return (rows_updated);
    }
}