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
    
    private final String sql;
    
    /**
     * Constructor
     * Each occurence of the pattern "??" will be replaced by a string
     * of repeated ?'s
     * @param sql
     * @param substitutions
     */
    public SQLStmt(String sql, int...substitutions) {
        for (int ctr : substitutions) {
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

    @Override
    public String toString() {
        return "SQLStmt{" + this.sql + "}";
    }
    
}
