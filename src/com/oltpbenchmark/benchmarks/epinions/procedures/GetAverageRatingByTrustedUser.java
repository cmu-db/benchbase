/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.epinions.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GetAverageRatingByTrustedUser extends Procedure {

    public final SQLStmt getAverageRating = new SQLStmt(
            "SELECT avg(rating) FROM review r, trust t WHERE r.u_id=t.target_u_id AND r.i_id=? AND t.source_u_id=?"
    );

    public void run(Connection conn, long iid, long uid) throws SQLException {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, getAverageRating)) {
            stmt.setLong(1, iid);
            stmt.setLong(2, uid);
            try (ResultSet r = stmt.executeQuery()) {
                while (r.next()) {
                    continue;
                }
            }
        }
    }

}
