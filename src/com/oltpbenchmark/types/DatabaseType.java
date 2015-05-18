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

package com.oltpbenchmark.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum DatabaseType {

    DB2,
    MYSQL,
    POSTGRES,
    ORACLE,
    SQLSERVER,
    SQLITE,
    AMAZONRDS,
    HSTORE,
    SQLAZURE,
    ASSCLOWN,
    HSQLDB,
    H2,
    MONETDB,
    NUODB,
    TIMESTEN
    ;
    
    public static final DatabaseType TEST_TYPE = DatabaseType.HSQLDB; 
    
    protected static final Map<Integer, DatabaseType> idx_lookup = new HashMap<Integer, DatabaseType>();
    protected static final Map<String, DatabaseType> name_lookup = new HashMap<String, DatabaseType>();
    static {
        for (DatabaseType vt : EnumSet.allOf(DatabaseType.class)) {
            DatabaseType.idx_lookup.put(vt.ordinal(), vt);
            DatabaseType.name_lookup.put(vt.name().toUpperCase(), vt);
        }
    }
    
    public static DatabaseType get(String name) {
        DatabaseType ret = DatabaseType.name_lookup.get(name.toUpperCase());
        return (ret);
    }
}
