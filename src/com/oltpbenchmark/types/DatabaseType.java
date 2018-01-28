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
     * (2) Should SQLUtil.getInsertSQL escape table/col names
     * (3) Should SQLUtil.getInsertSQL include col names
     * (4) Does this DBMS support "real" transactions?
     */
    DB2("com.ibm.db2.jcc.DB2Driver", true, false, true),
    MYSQL("com.mysql.jdbc.Driver", true, false, true),
    MYROCKS("com.mysql.jdbc.Driver", true, false, true),
    POSTGRES("org.postgresql.Driver", false, false, true),
    ORACLE("oracle.jdbc.driver.OracleDriver", true, false, true),
    SQLSERVER("com.microsoft.sqlserver.jdbc.SQLServerDriver", true, false, true),
    SQLITE("org.sqlite.JDBC", true, false, true),
    AMAZONRDS(null, true, false, true),
    SQLAZURE(null, true, false, true),
    ASSCLOWN(null, true, false, true),
    HSQLDB("org.hsqldb.jdbcDriver", false, false, true),
    H2("org.h2.Driver", true, false, true),
    MONETDB("nl.cwi.monetdb.jdbc.MonetDriver", false, false, true),
    NUODB("com.nuodb.jdbc.Driver", true, false, true),
    TIMESTEN("com.timesten.jdbc.TimesTenDriver", true, false, true),
    CASSANDRA("com.github.adejanovski.cassandra.jdbc.CassandraDriver", true, true, false),
    PELOTON("org.postgresql.Driver", false, false, true)
    ;
    
    private DatabaseType(String driver,
                         boolean escapeNames,
                         boolean includeColNames,
                         boolean supportTxns) {
        this.driver = driver;
        this.escapeNames = escapeNames;
        this.includeColNames = includeColNames;
        this.supportTxns = supportTxns;
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
    
    /**
     * If this flag is set to true, then the framework will invoke the JDBC transaction
     * api to do various things during execution. This should only be disabled
     * if you know that the DBMS will throw an error when these commands are executed.
     * For example, the Cassandra JDBC driver (as of 2018) throws a "Not Implemented" exception
     * when the framework tries to set the isolation level.
     */
    private boolean supportTxns;
    
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
    
    /**
     * Returns true if the framework should use transactions when executing
     * any SQL queries on the target DBMS.
     * @return
     */
    public boolean shouldUseTransactions() {
        return (this.supportTxns);
    }
    
    // ----------------------------------------------------------------
    // STATIC METHODS + MEMBERS
    // ----------------------------------------------------------------
    
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
