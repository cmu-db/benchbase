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

import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.util.JSONUtil;

import org.apache.log4j.Logger;

import java.sql.*;

public class PostgresCollector extends DBCollector {
    private static final Logger LOG = Logger.getLogger(PostgresCollector.class);
    
    private static final String VERSION_SQL = "SELECT version();";
    
    private static final String PARAMETERS_SQL = "SHOW ALL;";
    
    private static final String ARCHIVER_SQL = "SELECT * FROM pg_stat_archiver;";
    
    private static final String BGWRITER_SQL = "SELECT * FROM pg_stat_bgwriter;";
    
    private static final String DATABASE_SQL = "SELECT * FROM pg_stat_database;";
    
    private static final String DATABASE_CONFLICTS_SQL = "SELECT * FROM pg_stat_database_conflicts;";
    
    private static final String TABLE_SQL = "SELECT * FROM pg_stat_all_tables;";
    
    private static final String TABLE_IO_SQL = "SELECT * FROM pg_statio_all_tables;";
    
    private static final String INDEX_SQL = "SELECT * FROM pg_stat_all_indexes;";
    
    private static final String INDEX_IO_SQL = "SELECT * FROM pg_statio_all_indexes;";

    public PostgresCollector(String oriDBUrl, String username, String password) {
        try {
            Connection conn = DriverManager.getConnection(oriDBUrl, username, password);
            Catalog.setSeparator(conn);
            Statement s = conn.createStatement();
            
            // Collect DBMS version
            ResultSet out = s.executeQuery(VERSION_SQL);
            if (out.next()) {
            	this.version.append(out.getString(1));
            }
            
            // Collect DBMS parameters
            out = s.executeQuery(PARAMETERS_SQL);
            while(out.next()) {
                dbParameters.put(out.getString("name"), out.getString("setting"));
            }
            
            // Collect DBMS internal metrics
        } catch (SQLException e) {
            LOG.debug("Error while collecting DB parameters: " + e.getMessage());
        }
    }
    
    @Override
    public boolean hasMetrics() {
    	return (dbMetrics.isEmpty() == false);
    }
    
    @Override
    public String collectMetrics() {
    	return JSONUtil.format(JSONUtil.toJSONString(dbMetrics));
    }
}
