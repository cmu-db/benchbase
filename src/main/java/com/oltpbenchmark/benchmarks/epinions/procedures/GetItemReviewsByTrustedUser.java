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

package com.oltpbenchmark.benchmarks.epinions.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class GetItemReviewsByTrustedUser extends Procedure {

	//FIXME: CARLO, does this make sense?
    public final SQLStmt getReview = new SQLStmt(
        "SELECT * FROM review r WHERE r.i_id=?"
    );
    
    public final SQLStmt getTrust = new SQLStmt(
        "SELECT * FROM trust t WHERE t.source_u_id=?"
    );
    
    public void run(Connection conn, long iid, long uid) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getReview);
        stmt.setLong(1, iid);
        ResultSet r= stmt.executeQuery();
        while (r.next()) {
            continue;
        }
        r.close();
        stmt = this.getPreparedStatement(conn, getTrust);
        stmt.setLong(1, uid);
        r= stmt.executeQuery();
        while (r.next()) {
            continue;
        }
        r.close();
    }
    
}
