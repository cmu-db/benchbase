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

import com.oltpbenchmark.benchmarks.otmetrics.OTMetricsConstants;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Insert New Observation Procedure
 *
 * @author Haonan Wang
 */
public class NewObservation extends Procedure {

    // SOURCE_ID, SESSION_ID, TYPE_ID, VALUE, CREATED_TIME
    public final SQLStmt newObservation = new SQLStmt(
            "INSERT INTO " + OTMetricsConstants.TABLENAME_OBSERVATIONS 
            + " (source_id, session_id, type_id, value, created_time) VALUES (?, ?, ?, ?, ?)"
    );

    public boolean run(Connection conn, int source_id, int session_id, int type_id, float value) throws SQLException {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, newObservation)) {
            stmt.setInt(1, source_id);
            stmt.setInt(2, session_id);
            stmt.setInt(3, type_id);
            stmt.setFloat(4, value);
            stmt.setDate(5, new java.sql.Date(System.currentTimeMillis()));
            return (stmt.execute());
        }
    }
}