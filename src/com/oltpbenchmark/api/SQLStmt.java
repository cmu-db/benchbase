/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:    Carlo Curino <carlo.curino@gmail.com>
 *              Evan Jones <ej@evanjones.ca>
 *              DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 *              Andy Pavlo <pavlo@cs.brown.edu>
 *              CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *                  Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.api;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SQLStmt {

    private static final Pattern SUBSTITUTION_PATTERN = Pattern.compile("\\?\\?"); 
    
    private String orig_sql;
    private String sql;
    
    /**
     * For each unique '??' that we encounter in the SQL for this Statement,
     * we will substitute it with the number of '?' specified in this array. 
     */
    private final int substitutions[];
    
    /**
     * Constructor
     * 
     * @param sql
     * @param substitutions
     */
    public SQLStmt(String sql, int...substitutions) {
        this.substitutions = substitutions;
        this.setSQL(sql);
    }
    
    /**
     * Magic SQL setter!
     * Each occurrence of the pattern "??" will be replaced by a string
     * of repeated ?'s
     * @param sql
     * @param substitutions
     */
    protected final void setSQL(String sql) {
        this.orig_sql = sql;
        for (int ctr : this.substitutions) {
            assert(ctr > 0);
            String replace = "";
            for (int i = 0; i < ctr; i++) {
                replace += (i > 0 ? ", " : "") + "?";
            } // FOR
            Matcher m = SUBSTITUTION_PATTERN.matcher(sql);
            sql = m.replaceFirst(replace);
        } // FOR
        this.sql = sql;
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
