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

package com.oltpbenchmark.benchmarks.timeseries.procedures;

import com.oltpbenchmark.api.Procedure;

import java.sql.*;

public class GetData extends Procedure {

    public void run(Connection conn, String where[], String output[]) throws SQLException {
        // SQL Template
        String sql = String.format("SELECT %s FROM jungle WHERE %s",
                String.join(", ", output),
                String.join(" AND ", where));
        System.out.println(sql);

        try (Statement stmt = conn.createStatement(); ResultSet result = stmt.executeQuery(sql)) {

            // We should get the same # of output columns as specified in our output list
            ResultSetMetaData meta = result.getMetaData();
            assert(meta.getColumnCount() == output.length);

            while (result.next()) {
                for (int i = 1; i <= output.length; i++) {
                    if (meta.getColumnType(i) == Types.INTEGER) {
                        assert(result.getInt(i) >= 0);
                    }
                }
            }
        }
    }

}
