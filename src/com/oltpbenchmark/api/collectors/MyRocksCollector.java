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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class MyRocksCollector extends DBCollector {

    private static final Logger LOG = Logger.getLogger(MyRocksCollector.class);

    private static final String PARAMETERS_SQL = "SHOW GLOBAL VARIABLES";

    private static final String CF_PARAMETERS_SQL = "SELECT * FROM information_schema.rocksdb_cf_options ORDER BY cf_name, option_type";

    private static final String METRICS_SQL = "SHOW GLOBAL STATUS";

    private static final String DB_METRICS_SQL = "SELECT * FROM information_schema.db_statistics WHERE db = '%s'";

    private static final String TABLE_METRICS_SQL = "SELECT * FROM information_schema.table_statistics WHERE table_schema = '%s'";

    private static final String INDEX_METRICS_SQL = "SELECT * FROM information_schema.statistics WHERE TABLE_SCHEMA = '%s'";

    private static final String METRICS_VIEWS_SQL = "SELECT * FROM information_schema.%s";

    private static final String[] METRICS_VIEWS = {
            "index_statistics",
            "rocksdb_cfstats",
            "rocksdb_compaction_stats",
            "rocksdb_dbstats",
            "rocksdb_perf_context_global",
            "rocksdb_perf_context"
    };

    public MyRocksCollector(String dbUrl, String dbUsername, String dbPassword) {
        super(dbUrl, dbUsername, dbPassword);
    }

    @Override
    public String collectParameters() {
        Map<String, String> parameters = null;
        Connection conn = null;
        try {
            // Collect global parameters (inherited from MySQL)
            parameters = getKeyValueResults(PARAMETERS_SQL);

            // Collect MyRocks Column Family parameters
            conn = this.makeConnection();
            Statement s = conn.createStatement();
            ResultSet out = s.executeQuery(CF_PARAMETERS_SQL);
            String name;
            while (out.next()) {
                name = String.format("cf_option: cf_name=%s, type=%s", out.getString(1).toLowerCase(),
                        out.getString(2).toLowerCase());
                parameters.put(name, out.getString(3));
            }
        } catch (SQLException ex) {
            LOG.warn("Error collecting DB parameters: " + ex.getMessage());
        } finally {
            closeConnection(conn);
        }
        return toJSONString(parameters);
    }

    @Override
    public String collectMetrics() {
        Map<String, List<Map<String, String>>> metrics = new HashMap<String, List<Map<String, String>>>();

        Connection conn = null;
        try {
            // Collect global metrics (inherited from MySQL)
            List<Map<String, String>> list = new ArrayList<Map<String, String>>();
            list.add(getKeyValueResults(METRICS_SQL));
            metrics.put("internal_metrics", list);

            conn = this.makeConnection();

            // Collect db-, table-, and index-level metrics (inherited from MySQL)
            String dbName = getDatabaseName(conn);
            metrics.put("db_statistics", getColumnResults(conn, String.format(DB_METRICS_SQL, dbName)));
            metrics.put("table_statistics", getColumnResults(conn, String.format(TABLE_METRICS_SQL, dbName)));
            metrics.put("index_statistics", getColumnResults(conn, String.format(INDEX_METRICS_SQL, dbName)));

            // Collect myrocks-specific metrics
            for (String viewName : METRICS_VIEWS) {
                try {
                    metrics.put(viewName, getColumnResults(conn, String.format(METRICS_VIEWS_SQL, viewName)));
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

    private String getDatabaseName(Connection conn) {
        String dbName = null;
        try {
            Statement s = conn.createStatement();
            ResultSet out = s.executeQuery("SELECT database()");
            if (out.next()) {
                dbName = out.getString(1);
            }
        } catch (SQLException ex) {
        }
        if (dbName == null) {
            String[] parts = this.dbUrl.split("/");
            parts = parts[parts.length - 1].split("\\?");
            dbName = parts[0];
        }
        return dbName;
    }

}
