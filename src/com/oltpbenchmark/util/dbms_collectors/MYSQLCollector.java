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

package com.oltpbenchmark.util.dbms_collectors;

import com.oltpbenchmark.catalog.Catalog;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Map;

class MYSQLCollector extends DBCollector {
    private static final Logger LOG = Logger.getLogger(MYSQLCollector.class);
    private static final String VERSION = "VERSION";

    public MYSQLCollector(String oriDBUrl, String username, String password) {
        String dbUrl = oriDBUrl.substring(0, oriDBUrl.lastIndexOf('/'));
        dbUrl = dbUrl + "/information_schema";
        try {
            Connection conn = DriverManager.getConnection(dbUrl, username, password);
            Catalog.setSeparator(conn);
            Statement s = conn.createStatement();
            ResultSet out = s.executeQuery("SELECT * FROM GLOBAL_VARIABLES;");
            while(out.next()) {
                dbConf.put(out.getString("VARIABLE_NAME"), out.getString("VARIABLE_VALUE"));
            }
        } catch (SQLException e) {
            LOG.debug("Error while collecting DB parameters: " + e.getMessage());
        }
    }

    @Override
    public String collectVersion() {
        String dbVersion = dbConf.get(VERSION);
        int verIdx = dbVersion.indexOf('-');
        if (verIdx >= 0)
	        dbVersion = dbVersion.substring(0, verIdx);
        return dbVersion;
    }
}
