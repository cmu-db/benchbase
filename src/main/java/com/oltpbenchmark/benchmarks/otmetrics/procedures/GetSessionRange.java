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

package com.oltpbenchmark.benchmarks.otmetrics.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class GetSessionRange extends Procedure {

    /**
     * We do it this way because not all JDBC drivers support using arrays to fill in var-length parameters
     */
    private final String baseSQL =
            "SELECT * FROM observations" +
            " WHERE source_id = ?" +
            "   AND session_id >= ?" +
            "   AND session_id <= ?" +
            "   AND type_id IN (%s)" +
            " ORDER BY created_time";
    public final SQLStmt RangeQuery1 = new SQLStmt(String.format(baseSQL, "?"));
    public final SQLStmt RangeQuery2 = new SQLStmt(String.format(baseSQL, "?, ?"));
    public final SQLStmt RangeQuery3 = new SQLStmt(String.format(baseSQL, "?, ?, ?"));

    public List<Object[]> run(Connection conn, int source_id, int session_low, int session_high, int type_ids[]) throws SQLException {
        final List<Object[]> finalResults = new ArrayList<>();

        PreparedStatement stmt;
        switch (type_ids.length) {
            case 1:
                stmt = this.getPreparedStatement(conn, RangeQuery1, source_id, session_low, session_high, type_ids[0]);
                break;
            case 2:
                stmt = this.getPreparedStatement(conn, RangeQuery2, source_id, session_low, session_high, type_ids[0], type_ids[1]);
                break;
            case 3:
                stmt = this.getPreparedStatement(conn, RangeQuery3, source_id, session_low, session_high, type_ids[0], type_ids[1], type_ids[2]);
                break;
            default:
                throw new RuntimeException("Unexpected type_id array length of " + type_ids.length);
        } // SWITCH
        assert(stmt != null);

        // Bombs away!
        try (ResultSet results = stmt.executeQuery()) {
            while (results.next()) {
                int cols = results.getMetaData().getColumnCount();
                Object[] arr = new Object[cols];
                for (int i = 0; i < cols; i++) {
                    arr[i] = results.getObject(i + 1).toString();
                }
                finalResults.add(arr);
            }
        }
        return (finalResults);
    }

}