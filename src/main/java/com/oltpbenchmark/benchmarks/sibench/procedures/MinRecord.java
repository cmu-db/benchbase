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

package com.oltpbenchmark.benchmarks.sibench.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.oltpbenchmark.benchmarks.sibench.SIConstants.TABLE_NAME;

public class MinRecord extends Procedure {
    public final SQLStmt minStmt = new SQLStmt("SELECT id FROM " + TABLE_NAME + " ORDER BY value ASC LIMIT 1");

    public int run(Connection conn) throws SQLException {
        int minId = 0;
        try (PreparedStatement stmt = this.getPreparedStatement(conn, minStmt); ResultSet r = stmt.executeQuery()) {
            while (r.next()) {
                minId = r.getInt(1);
            }
        }
        return minId;
    }
}
