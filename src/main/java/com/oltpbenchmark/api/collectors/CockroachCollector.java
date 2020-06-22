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

package com.oltpbenchmark.api.collectors;

import java.sql.*;

public class CockroachCollector extends DBCollector {

    private static final String VERSION_SQL = "SELECT version();";

    private static final String PARAMETERS_SQL = "SHOW ALL;";

    public CockroachCollector(String oriDBUrl, String username, String password) {
        try (Connection conn = DriverManager.getConnection(oriDBUrl, username, password)) {
            try (Statement s = conn.createStatement()) {

                // Collect DBMS version
                try (ResultSet out = s.executeQuery(VERSION_SQL)) {
                    if (out.next()) {
                        this.version = out.getString(1);
                    }
                }

                // Collect DBMS parameters
                try (ResultSet out = s.executeQuery(PARAMETERS_SQL)) {
                    while (out.next()) {
                        dbParameters.put(out.getString("variable"), out.getString("value"));
                    }
                }

            }
        } catch (SQLException e) {
            LOG.error("Error while collecting DB parameters: {}", e.getMessage());
        }
    }
}
