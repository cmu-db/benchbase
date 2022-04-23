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

public class GetReviewsByUser extends Procedure {

    public final SQLStmt getReviewUser = new SQLStmt(
            "SELECT * FROM review r, useracct u WHERE u.u_id = r.u_id AND r.u_id=? " +
                    "ORDER BY rating DESC, r.creation_date DESC LIMIT 10"
    );

    public void run(Connection conn, long uid) throws SQLException {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, getReviewUser)) {
            stmt.setLong(1, uid);
            try (ResultSet r = stmt.executeQuery()) {
                while (r.next()) {
                    continue;
                }
            }
        }
    }
}
