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

import java.sql.SQLException;
import java.util.Map;

import org.apache.log4j.Logger;

public class MySQLCollector extends DBCollector {

    private static final Logger LOG = Logger.getLogger(MySQLCollector.class);

    private static final String PARAMETERS_SQL = "SHOW GLOBAL VARIABLES";

    private static final String METRICS_SQL = "SHOW GLOBAL STATUS";

    public MySQLCollector(String dbUrl, String dbUsername, String dbPassword) {
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
        Map<String, String> metrics = null;
        try {
            metrics = getKeyValueResults(METRICS_SQL);
        } catch (SQLException ex) {
            LOG.warn("Error collecting DB metrics: " + ex.getMessage());
        }
        return toJSONString(metrics);
    }

}
