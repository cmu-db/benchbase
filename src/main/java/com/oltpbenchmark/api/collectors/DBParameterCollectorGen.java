/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.api.collectors;

import com.oltpbenchmark.types.DatabaseType;

public class DBParameterCollectorGen {
    public static DBParameterCollector getCollector(DatabaseType dbType, String dbUrl, String username, String password) {
        switch (dbType) {

            case MYSQL:
            case MARIADB:
                return new MySQLCollector(dbUrl, username, password);
            case POSTGRES:
                return new PostgresCollector(dbUrl, username, password);
            case COCKROACHDB:
                return new CockroachCollector(dbUrl, username, password);
            default:
                return new DBCollector();
        }
    }
}
