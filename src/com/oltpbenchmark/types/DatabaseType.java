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

/**
 * List of the database management systems that we support 
 * in the framework.
 * @author pavlo
 */
public enum DatabaseType {

    /**
     * Parameters:
     * (1) JDBC Driver String
     * (2) Should SQLUtil.getInserSQL escape table/col names
     * (3) Should SQLUtil.getInserSQL include col names
     */
    DB2("com.ibm.db2.jcc.DB2Driver", true, false),
    MYSQL("com.mysql.jdbc.Driver", true, false),
    POSTGRES("org.postgresql.Driver", false, false),
    ORACLE("oracle.jdbc.driver.OracleDriver", true, false),
    SQLSERVER("com.microsoft.sqlserver.jdbc.SQLServerDriver", true, false),
    SQLITE("org.sqlite.JDBC", true, false),
    AMAZONRDS(null, true, false),
    SQLAZURE(null, true, false),
    ASSCLOWN(null, true, false),
    HSQLDB("org.hsqldb.jdbcDriver", true, false),
    H2("org.h2.Driver", true, false),
    MONETDB("nl.cwi.monetdb.jdbc.MonetDriver", false, false),
    NUODB("com.nuodb.jdbc.Driver", true, false),
    TIMESTEN("com.timesten.jdbc.TimesTenDriver", true, false),
    PELOTON("org.postgresql.Driver", false, false)
    ;
    
    private DatabaseType(String driver, boolean escapeNames, boolean includeColNames) {
        this.driver = driver;
        this.escapeNames = escapeNames;
        this.includeColNames = includeColNames;
    }
    
    /**
     * This is the suggested driver string to use in the configuration xml
     * This corresponds to the <B>'driver'</b> attribute. 
     */
    private final String driver;
    
    /**
     * If this flag is set to true, then the framework will escape names in
     * the INSERT queries  
     */
    private final boolean escapeNames;
    
    /**
     * If this flag is set to true, then the framework will include the column names
     * when generating INSERT queries for loading data.  
     */
    private final boolean includeColNames;
    
    // ---------------------------------------------------------------
    // ACCESSORS
    // ----------------------------------------------------------------
    
    /**
     * Returns the suggested driver string to use for the given database type
     * @return
     */
    public String getSuggestedDriver() {
        return (this.driver);
    }
    
    /**
     * Returns true if the framework should escape the names of columns/tables when 
     * generating SQL to load in data for the target database type.
     * @return
     */
    public boolean shouldEscapeNames() {
        return (this.escapeNames);
    }
    
    /**
     * Returns true if the framework should include the names of columns when 
     * generating SQL to load in data for the target database type.
     * @return
     */
    public boolean shouldIncludeColumnNames() {
        return (this.includeColNames);
    }
    
    
    // ----------------------------------------------------------------
    // STATIC METHODS + MEMBERS
    // ----------------------------------------------------------------
    
    /**
     * This is the database type that we will use in our unit tests.
     * This should always be one of the embedded java databases
     */
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
