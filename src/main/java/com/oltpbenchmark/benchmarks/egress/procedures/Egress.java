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

package com.oltpbenchmark.benchmarks.egress.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * The actual Egress implementation
 *
 * @author mbutrovich
 */
public class Egress extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(Egress.class);


    // There's no generic query for this benchmark so we leave it as an empty statement. Implement a DBMS-specific DDL
    // and dialect as needed.
    public final SQLStmt egressStmt = new SQLStmt(";");

    public void run(Connection conn, int tuple_bytes, int num_tuples) {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, egressStmt)) {
            stmt.setInt(1, tuple_bytes);
            stmt.setInt(2, num_tuples);
            stmt.executeQuery();
            // We don't care about the ResultSet.
        } catch (SQLException ex) {
            throw new RuntimeException(ex.getMessage() + ex.getCause());
        }
    }

}
