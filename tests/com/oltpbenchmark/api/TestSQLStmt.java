package com.oltpbenchmark.api;

import junit.framework.TestCase;

public class TestSQLStmt extends TestCase {

    /**
     * testSubstitution
     */
    public void testSubstitution() throws Exception {
        int ctr = 25;
        SQLStmt stmt = new SQLStmt(
            "SELECT * FROM tweets WHERE uid IN (??)", ctr
        );
        
        String sql = stmt.getSQL();
        assertFalse(sql.isEmpty());
        assertFalse(sql.contains("\\?\\?"));
        
        System.err.println(stmt);
        
    }
    
}
