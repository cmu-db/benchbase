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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import com.oltpbenchmark.util.JSONUtil;
import java.sql.ResultSetMetaData;
import org.apache.log4j.Logger;
import com.oltpbenchmark.catalog.Catalog;

public class MyRocksCollector extends DBCollector {
    private static final Logger LOG = Logger.getLogger(MyRocksCollector.class);

    private static final String VERSION_SQL = "SELECT @@GLOBAL.version;";

    private static final String PARAMETERS_SQL = "SHOW VARIABLES;";

    private static final String METRICS_SQL = "SHOW STATUS";

    private static final String CF_OPTIONS = "select * from information_schema.rocksdb_cf_options order by cf_name, option_type;";

    private final Map<String, List<Map<String, String>>> myroMetrics;

    private static final String[] MYRO_STAT_VIEWS = {"rocksdb_cfstats","rocksdb_compaction_stats","rocksdb_dbstats","rocksdb_perf_context_global", "index_statistics","rocksdb_perf_context"};

    private static final String DB_STATS = "select * from information_schema.db_statistics where db = ";

    private static final String TABLE_STATS = "select * from information_schema.table_statistics where table_schema = ";
 
    private static final String INDEX_STATS = "SELECT *  FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ";

    public MyRocksCollector(String oriDBUrl, String username, String password) {

        myroMetrics = new HashMap<String, List<Map<String, String>>>();

        try {
            Connection conn = DriverManager.getConnection(oriDBUrl, username, password);
            Catalog.setSeparator(conn);
            Statement s = conn.createStatement();
         
            // Collect DBMS version
            ResultSet out = s.executeQuery(VERSION_SQL);
            if (out.next()) {
            	this.version.append(out.getString(1));
            }
      
            // Get currenct oltpbench database
            String dbname = "";
            out = s.executeQuery("select database()");
            if(out.next()){
                dbname = out.getString(1);
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
            myroMetrics.put("internal_metrics", getMetrics(out));
                   
            // Collect metrics from information_schema 
            for (String viewName : MYRO_STAT_VIEWS) {
            	out = s.executeQuery("select * from information_schema." + viewName + ";");
            	myroMetrics.put(viewName, getMetrics(out));
            }          
 
            // Collect db statistics
            out = s.executeQuery(DB_STATS + "\"" + dbname + "\""+ ";");
            myroMetrics.put("db_statistics", getMetrics(out));
           
            // Collect table statistics  
            out = s.executeQuery(TABLE_STATS + "\"" + dbname + "\""+ ";");
            myroMetrics.put("table_statistics", getMetrics(out));

            // Collect index statistics  
            out = s.executeQuery(INDEX_STATS + "\"" + dbname + "\""+ ";");
            myroMetrics.put("index_statistics", getMetrics(out));

        } catch (SQLException e) {
            LOG.error("Error while collecting DB parameters: " + e.getMessage());
        }
    }

    @Override
    public String collectMetrics() {
    	return JSONUtil.format(JSONUtil.toJSONString(myroMetrics));
    }  
}
