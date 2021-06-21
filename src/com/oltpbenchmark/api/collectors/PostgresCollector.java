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

import com.oltpbenchmark.util.JSONUtil;

import java.sql.*;
import java.util.*;

public class PostgresCollector extends DBCollector {

    private static final String VERSION_SQL = "SELECT version();";

    private static final String PARAMETERS_SQL = "SHOW ALL;";

    private static final String[] PG_STAT_VIEWS = {
            "pg_stat_archiver", "pg_stat_bgwriter", "pg_stat_database",
            "pg_stat_database_conflicts", "pg_stat_user_tables", "pg_statio_user_tables",
            "pg_stat_user_indexes", "pg_statio_user_indexes"
    };

    private final Map<String, List<Map<String, String>>> pgMetrics = new HashMap<>();

    public PostgresCollector(String oriDBUrl, String username, String password) {

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
                        dbParameters.put(out.getString("name"), out.getString("setting"));
                    }
                }

                // Collect DBMS internal metrics
                for (String viewName : PG_STAT_VIEWS) {
                    try (ResultSet out = s.executeQuery("SELECT * FROM " + viewName)) {
                        pgMetrics.put(viewName, getMetrics(out));
                    } catch (SQLException ex) {
                        LOG.error("Error while collecting DB metric view: {}", ex.getMessage());
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error("Error while collecting DB parameters: {}", e.getMessage());
        }
    }

    @Override
    public boolean hasMetrics() {
        return (!pgMetrics.isEmpty());
    }

    @Override
    public String collectMetrics() {
        return JSONUtil.format(JSONUtil.toJSONString(pgMetrics));
    }

    private List<Map<String, String>> getMetrics(ResultSet out) throws SQLException {
        ResultSetMetaData metadata = out.getMetaData();
        int numColumns = metadata.getColumnCount();
        String[] columnNames = new String[numColumns];
        for (int i = 0; i < numColumns; ++i) {
            columnNames[i] = metadata.getColumnName(i + 1).toLowerCase();
        }

        List<Map<String, String>> metrics = new ArrayList<>();
        while (out.next()) {
            Map<String, String> metricMap = new TreeMap<>();
            for (int i = 0; i < numColumns; ++i) {
                metricMap.put(columnNames[i], out.getString(i + 1));
            }
            metrics.add(metricMap);
        }
        return metrics;
    }
}
