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

package com.oltpbenchmark.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * List of the database management systems that we support
 * in the framework.
 *
 * @author pavlo
 */
public enum DatabaseType {

    AMAZONRDS(true, false),
    CASSANDRA(true, true),
    COCKROACHDB(false, false, true),
    DB2(true, false),
    H2(true, false),
    HSQLDB(false, false),
    POSTGRES(false, false, true),
    MARIADB(true, false),
    MONETDB(false, false),
    MYROCKS(true, false),
    MYSQL(true, false),
    NOISEPAGE(false, false),
    NUODB(true, false),
    ORACLE(true, false),
    SINGLESTORE(true, false),
    SPANNER(false, true),
    SQLAZURE(true, true, true),
    SQLITE(true, false),
    SQLSERVER(true, true, true),
    TIMESTEN(true, false),
    PHOENIX(true, true);


    DatabaseType(boolean escapeNames, boolean includeColNames, boolean loadNeedsUpdateColumnSequence) {
        this.escapeNames = escapeNames;
        this.includeColNames = includeColNames;
        this.loadNeedsUpdateColumnSequence = loadNeedsUpdateColumnSequence;
    }

    DatabaseType(boolean escapeNames, boolean includeColNames) {
        this(escapeNames, includeColNames, false);
    }

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
     * If this flag is set to true, the framework will attempt to update the
     * column sequence after loading data.
     */
    private final boolean loadNeedsUpdateColumnSequence;


    // ---------------------------------------------------------------
    // ACCESSORS
    // ----------------------------------------------------------------

    /**
     * @return True if the framework should escape the names of columns/tables when
     * generating SQL to load in data for the target database type.
     */
    public boolean shouldEscapeNames() {
        return (this.escapeNames);
    }

    /**
     * @return True if the framework should include the names of columns when
     * generating SQL to load in data for the target database type.
     */
    public boolean shouldIncludeColumnNames() {
        return (this.includeColNames);
    }

    /**
     * @return True if the framework should attempt to update the column
     * sequence after loading data.
     */
    public boolean shouldUpdateColumnSequenceAfterLoad() {
        return (this.loadNeedsUpdateColumnSequence);
    }

    // ----------------------------------------------------------------
    // STATIC METHODS + MEMBERS
    // ----------------------------------------------------------------

    protected static final Map<Integer, DatabaseType> idx_lookup = new HashMap<>();
    protected static final Map<String, DatabaseType> name_lookup = new HashMap<>();

    static {
        for (DatabaseType vt : EnumSet.allOf(DatabaseType.class)) {
            DatabaseType.idx_lookup.put(vt.ordinal(), vt);
            DatabaseType.name_lookup.put(vt.name().toUpperCase(), vt);
        }
    }

    public static DatabaseType get(String name) {
        return (DatabaseType.name_lookup.get(name.toUpperCase()));
    }
}
