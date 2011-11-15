package com.oltpbenchmark.api;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SQLStmt {

    private static final Pattern SUBSTITUTION_PATTERN = Pattern.compile("\\?\\?"); 
    
    private final String sql;
    
    /**
     * Contructor
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
