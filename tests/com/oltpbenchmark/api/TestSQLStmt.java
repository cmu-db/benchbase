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
    }
    
    /**
     * testSetSQL
     */
    public void testSetSQL() throws Exception {
        int expected = 99;
        SQLStmt stmt = new SQLStmt("SELECT * FROM tweets", expected);
        stmt.setSQL("SELECT * FROM tweets WHERE uid IN (??)");
        
        String sql = stmt.getSQL();
        assertFalse(sql.isEmpty());
        assertFalse(sql.contains("\\?\\?"));
        
        // Count the number of times '?' appears
        int actual = 0;
        for (int i = 0; i < sql.length(); i++) {
            if (sql.charAt(i) == '?') actual++;
        } // FOR
        assertEquals(expected, actual);
    }
    
}
