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
import java.sql.SQLException;

public class UpdateTrustRating extends Procedure {

    public final SQLStmt updateTrust = new SQLStmt(
            "UPDATE trust SET trust = ? WHERE source_u_id=? AND target_u_id=?"
    );

    public void run(Connection conn, long source_uid, long target_uid, int trust) throws SQLException {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, updateTrust)) {
            stmt.setInt(1, trust);
            stmt.setLong(2, source_uid);
            stmt.setLong(3, target_uid);
            stmt.executeUpdate();
        }
    }
}
