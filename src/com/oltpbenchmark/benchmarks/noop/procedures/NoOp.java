/******************************************************************************
 *  Copyright 2016 by OLTPBenchmark Project                                   *
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

package com.oltpbenchmark.benchmarks.noop.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

/**
 * The actual NoOp implementation
 * @author pavlo
 * @author eric-haibin-lin
 */
public class NoOp extends Procedure {
    
    // The query only contains a semi-colon
    // That is enough for the DBMS to have to parse it and do something
    public final SQLStmt noopStmt = new SQLStmt("SELECT 1");
    
    public void run(Connection conn) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, noopStmt);
        ResultSet r = stmt.executeQuery();
        while (r.next()) {
            // Do nothing
        } // WHILE
        r.close();
    }

}
