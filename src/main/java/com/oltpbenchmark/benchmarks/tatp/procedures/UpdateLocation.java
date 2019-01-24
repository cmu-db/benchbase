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

public class UpdateLocation extends Procedure {

    public final SQLStmt getSubscriber = new SQLStmt(
        "SELECT s_id FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE sub_nbr = ?"
    );
    
    public final SQLStmt updateSubscriber = new SQLStmt(
        "UPDATE " + TATPConstants.TABLENAME_SUBSCRIBER + " SET vlr_location = ? WHERE s_id = ?"
    );
    
    public long run(Connection conn, int location, String sub_nbr) throws SQLException {
    	PreparedStatement stmt = this.getPreparedStatement(conn, getSubscriber);
    	stmt.setString(1, sub_nbr);
    	ResultSet results = stmt.executeQuery();
    	assert(results != null);
    	
    	if (results.next()) {
    		long s_id = results.getLong(1);
    		results.close();
    		stmt = this.getPreparedStatement(conn, updateSubscriber);
    		stmt.setInt(1, location);
    		stmt.setLong(2, s_id);
    		return stmt.executeUpdate();
        }
    	results.close();
        return 0;
    }
}