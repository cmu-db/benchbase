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
