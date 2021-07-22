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

package com.oltpbenchmark.benchmarks.ycsb.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.oltpbenchmark.benchmarks.ycsb.YCSBConstants.TABLE_NAME;

public class UpdateRecord extends Procedure {

    public final SQLStmt updateAllStmt = new SQLStmt(
            "UPDATE " + TABLE_NAME + " SET FIELD1=?,FIELD2=?,FIELD3=?,FIELD4=?,FIELD5=?," +
                    "FIELD6=?,FIELD7=?,FIELD8=?,FIELD9=?,FIELD10=? WHERE YCSB_KEY=?"
    );

    public void run(Connection conn, int keyname, String[] vals) throws SQLException {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, updateAllStmt)) {

            stmt.setInt(11, keyname);
            for (int i = 0; i < vals.length; i++) {
                stmt.setString(i + 1, vals[i]);
            }
            stmt.executeUpdate();
        }
    }
}

