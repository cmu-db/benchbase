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

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.JSONUtil;

public abstract class DBCollector {

    private static final Logger LOG = Logger.getLogger(DBCollector.class);

    protected static final String EMPTY_JSON = "{}";

    protected final String dbUrl;

    protected final String dbUsername;

    protected final String dbPassword;

    public DBCollector(String dbUrl, String dbUsername, String dbPassword) {
        this.dbUrl = dbUrl;
        this.dbUsername = dbUsername;
        this.dbPassword = dbPassword;
    }

    public abstract String collectParameters();

    public abstract String collectMetrics();

    public void writeParameters(PrintStream out) {
        out.println(collectParameters());
    }

    public void writeMetrics(PrintStream out) {
        out.println(collectMetrics());
    }

    public String getVersion() {
        String version;
        Connection conn = null;
        try {
            conn = this.makeConnection();
            DatabaseMetaData meta = conn.getMetaData();
            int majorVersion = meta.getDatabaseMajorVersion();
            int minorVersion = meta.getDatabaseMinorVersion();
            version = String.format("%s.%s", majorVersion, minorVersion);
        } catch (SQLException ex) {
            version = "";
        } finally {
            closeConnection(conn);
        }
        return version;
    }

    protected Connection makeConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(this.dbUrl, this.dbUsername, this.dbPassword);
        Catalog.setSeparator(conn);
        return conn;
    }

    protected Map<String, String> getKeyValueResults(String sql) throws SQLException {
        Map<String, String> results = null;
        Connection conn = null;
        try {
            conn = this.makeConnection();
            results = getKeyValueResults(conn, sql);
        } finally {
            closeConnection(conn);
        }
        return results;
    }

    public static DBCollector createCollector(WorkloadConfiguration workConf) {
        return createCollector(
                workConf.getDBType(),
                workConf.getDBConnection(),
                workConf.getDBUsername(),
                workConf.getDBPassword());
    }

    public static DBCollector createCollector(DatabaseType dbType, String dbUrl, String dbUsername, String dbPassword) {
        DBCollector collector;

        switch (dbType) {
        case MEMSQL: // Uses MySQLCollector
        case MYSQL: {
            collector = new MySQLCollector(dbUrl, dbUsername, dbPassword);
            break;
        }
        case MYROCKS: {
            collector = new MyRocksCollector(dbUrl, dbUsername, dbPassword);
            break;
        }
        case POSTGRES: {
            collector = new PostgresCollector(dbUrl, dbUsername, dbPassword);
            break;
        }
        default:
            collector = new NoopCollector(dbUrl, dbUsername, dbPassword);
        }
        return collector;
    }

    protected static Map<String, String> getKeyValueResults(Connection conn, String sql) throws SQLException {
        Map<String, String> results = new TreeMap<String, String>();
        Statement s = conn.createStatement();
        ResultSet out = s.executeQuery(sql);
        while (out.next()) {
            results.put(out.getString(1).toLowerCase(), out.getString(2));
        }
        return results;
    }

    protected static List<Map<String, String>> getColumnResults(Connection conn, String sql) throws SQLException {
        Statement s = conn.createStatement();
        ResultSet out = s.executeQuery(sql);

        // Get column names
        ResultSetMetaData metadata = out.getMetaData();
        int numColumns = metadata.getColumnCount();
        String[] columnNames = new String[numColumns];
        for (int i = 0; i < numColumns; ++i) {
            columnNames[i] = metadata.getColumnName(i + 1).toLowerCase();
        }

        // Create a column name --> value map for each row
        List<Map<String, String>> results = new ArrayList<Map<String, String>>();
        while (out.next()) {
            Map<String, String> columnResult = new TreeMap<String, String>();
            for (int i = 0; i < numColumns; ++i) {
                columnResult.put(columnNames[i], out.getString(i + 1));
            }
            results.add(columnResult);
        }
        return results;
    }

    protected static void closeConnection(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException ex) {
            if (LOG.isDebugEnabled())
                LOG.warn("Error closing connection");
        }
    }

    protected static <T> String toJSONString(T object) {
        return (object == null) ? EMPTY_JSON : JSONUtil.format(JSONUtil.toJSONString(object));
    }

}
