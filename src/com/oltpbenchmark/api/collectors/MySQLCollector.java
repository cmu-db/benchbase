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

public class MySQLCollector extends DBCollector {
    private static final Logger LOG = Logger.getLogger(MySQLCollector.class);

    private final String versionSQL = "SELECT @@GLOBAL.version;";

    private final String parametersSQL = "SHOW VARIABLES;";

    private final String metricsSQL = "SHOW STATUS";

    public MySQLCollector(String oriDBUrl, String username, String password) {
        try {
            Connection conn = DriverManager.getConnection(oriDBUrl, username, password);
            Catalog.setSeparator(conn);
            Statement s = conn.createStatement();

            // Collect DBMS version
            ResultSet out = s.executeQuery(versionSQL);
            if (out.next()) {
            	this.version.append(out.getString(1));
            }

            // Collect DBMS parameters
            out = s.executeQuery(parametersSQL);
            while(out.next()) {
                dbParameters.put(out.getString(1).toLowerCase(), out.getString(2));
            }

            // Collect DBMS internal metrics
            out = s.executeQuery(metricsSQL);
            while (out.next()) {
            	dbMetrics.put(out.getString(1).toLowerCase(), out.getString(2));
            }
        } catch (SQLException e) {
            LOG.warn("Error while collecting DB parameters: " + e.getMessage());
        }
    }
}
