/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.api.collectors;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class PostgresCollector extends DBCollector {

    private static final Logger LOG = Logger.getLogger(PostgresCollector.class);

    private static final String PARAMETERS_SQL = "SHOW ALL";

    private static final String METRICS_SQL = "SELECT * FROM %s";

    private static final String[] METRICS_VIEWS = {
            "pg_stat_archiver",
            "pg_stat_bgwriter",
            "pg_stat_database",
            "pg_stat_database_conflicts",
            "pg_stat_user_tables",
            "pg_statio_user_tables",
            "pg_stat_user_indexes",
            "pg_statio_user_indexes"
    };

    public PostgresCollector(String dbUrl, String dbUsername, String dbPassword) {
        super(dbUrl, dbUsername, dbPassword);
    }

    @Override
    public String collectParameters() {
        Map<String, String> parameters = null;
        try {
            parameters = getKeyValueResults(PARAMETERS_SQL);
        } catch (SQLException ex) {
            LOG.warn("Error collecting DB parameters: " + ex.getMessage());
        }
        return toJSONString(parameters);
    }

    @Override
    public String collectMetrics() {
        Connection conn = null;
        Map<String, List<Map<String, String>>> metrics = null;
        try {
            conn = this.makeConnection();
            metrics = new HashMap<String, List<Map<String, String>>>();
            for (String viewName : METRICS_VIEWS) {
                try {
                    metrics.put(viewName, getColumnResults(conn, String.format(METRICS_SQL, viewName)));
                } catch (SQLException ex) {
                    if (LOG.isDebugEnabled())
                        LOG.warn("Error collecting DB metric view: " + ex.getMessage());
                }
            }
        } catch (SQLException ex) {
            LOG.warn("Error collecting DB metrics: " + ex.getMessage());
        } finally {
            closeConnection(conn);
        }
        return toJSONString(metrics);
    }

}
