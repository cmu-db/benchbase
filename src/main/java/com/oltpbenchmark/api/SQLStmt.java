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


package com.oltpbenchmark.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Wrapper Class for SQL Statements
 *
 * @author pavlo
 */
public final class SQLStmt {
    private static final Logger LOG = LoggerFactory.getLogger(SQLStmt.class);

    private static final Pattern SUBSTITUTION_PATTERN = Pattern.compile("\\?\\?");

    private String orig_sql;
    private String sql;

    /**
     * For each unique '??' that we encounter in the SQL for this Statement,
     * we will substitute it with the number of '?' specified in this array.
     */
    private final int[] substitutions;

    /**
     * Constructor
     *
     * @param sql
     * @param substitutions
     */
    public SQLStmt(String sql, int... substitutions) {
        this.substitutions = substitutions;
        this.setSQL(sql);
    }

    /**
     * Magic SQL setter!
     * Each occurrence of the pattern "??" will be replaced by a string
     * of repeated ?'s
     *
     * @param sql
     */
    public final void setSQL(String sql) {
        this.orig_sql = sql.trim();
        for (int ctr : this.substitutions) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < ctr; i++) {
                sb.append(i > 0 ? ", " : "").append("?");
            }
            Matcher m = SUBSTITUTION_PATTERN.matcher(sql);
            String replace = sb.toString();
            sql = m.replaceFirst(replace);
        }
        this.sql = sql;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Initialized SQL:\n{}", this.sql);
        }
    }

    public final String getSQL() {
        return (this.sql);
    }

    protected final String getOriginalSQL() {
        return (this.orig_sql);
    }

    @Override
    public String toString() {
        return "SQLStmt{" + this.sql + "}";
    }

}
