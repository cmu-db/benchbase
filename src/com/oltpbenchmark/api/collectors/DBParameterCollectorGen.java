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

public class DBParameterCollectorGen {
    public static DBParameterCollector getCollector(String dbType, String dbUrl, String username, String password) {
        String db = dbType.toLowerCase();
        if (db.equals("mysql") || db.equals("memsql")) {
            return new MySQLCollector(dbUrl, username, password);
        } else if (db.equals("myrocks")) {
            return new MyRocksCollector(dbUrl, username, password);
	}
	else if (db.equals("postgres")) {
            return new PostgresCollector(dbUrl, username, password);
        } else {
            return new DBCollector();
        }
    }
}
