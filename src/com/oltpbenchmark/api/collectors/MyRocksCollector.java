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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.oltpbenchmark.catalog.Catalog;

public class MyRocksCollector extends DBCollector {
    private static final Logger LOG = Logger.getLogger(MyRocksCollector.class);

    private static final String VERSION_SQL = "SELECT @@GLOBAL.version;";

    private static final String PARAMETERS_SQL = "SHOW VARIABLES;";

    private static final String METRICS_SQL = "SHOW STATUS";

    private static final String CF_OPTIONS = "select * from information_schema.rocksdb_cf_options order by cf_name, option_type;";
 
    private static final String CF_STATS= "select * from information_schema.rocksdb_cfstats;";


    private static final String COMPACTION_STATS= "select * from information_schema.rocksdb_compaction_stats;";


    private static final String ROCKSDB_DBSTATS= "select * from information_schema.rocksdb_dbstats;";

    private static final String PERF_CONTEXT_GLOBAL= "select * from information_schema.rocksdb_perf_context_global;";
    
    public MyRocksCollector(String oriDBUrl, String username, String password) {
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
                dbParameters.put(out.getString(1).toLowerCase(), out.getString(2));
            }

            // Collect MyRocks Column Family parameters
            out = s.executeQuery(CF_OPTIONS);
            while(out.next()){
                dbParameters.put("cf_option: " + "cf_name=" + out.getString(1).toLowerCase() + " , " + "type=" + out.getString(2).toLowerCase(), out.getString(3));
            }

            // Collect DBMS internal metrics
            out = s.executeQuery(METRICS_SQL);
            while (out.next()) {
            	dbMetrics.put(out.getString(1).toLowerCase(), out.getString(2));
            }

            // Collect MyRocks Column Family statistics 
            out = s.executeQuery(CF_STATS);
            while(out.next()){
                dbMetrics.put("cf_stats: "+ "cf_name=" + out.getString(1).toLowerCase() + " , " + "type="+ out.getString(2).toLowerCase(), out.getString(3));
            }            

            // Collect Compaction Statistics
            out = s.executeQuery(COMPACTION_STATS);
            while(out.next()){
                dbMetrics.put("cf_compaction_stats: "+ "cf_name=" + out.getString(1).toLowerCase() + " , " + "level=" + out.getString(2).toLowerCase() + " , " +"type=" + out.getString(3).toLowerCase(), out.getString(4));
            }            

            // Collect DB statistics
            out = s.executeQuery(ROCKSDB_DBSTATS);
            while (out.next()) {
            	dbMetrics.put(out.getString(1).toLowerCase(), out.getString(2));
            }    

            // Collect Global Performance Context Statistics 
            out = s.executeQuery(PERF_CONTEXT_GLOBAL);
            while (out.next()) {
            	dbMetrics.put(out.getString(1).toLowerCase(), out.getString(2));
            }
            // Collect Perf Context table? 
            // index, db, table statistic ? 



        } catch (SQLException e) {
            LOG.error("Error while collecting DB parameters: " + e.getMessage());
        }
    }
}
