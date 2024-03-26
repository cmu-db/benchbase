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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.oltpbenchmark.benchmarks.tatp.procedures.DeleteCallForwarding;
import com.oltpbenchmark.types.DatabaseType;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class TestProcedure {

  @Before
  public void setUp() throws Exception {}

  /** testGetProcedureName */
  @Test
  public void testGetProcedureName() throws Exception {
    DeleteCallForwarding proc = new DeleteCallForwarding();
    assertEquals(DeleteCallForwarding.class.getSimpleName(), proc.getProcedureName());
  }

  /** testGetStatements */
  @Test
  public void testGetStatements() throws Exception {
    Map<String, SQLStmt> stmts = Procedure.getStatements(new DeleteCallForwarding());
    assertNotNull(stmts);
    assertEquals(2, stmts.size());
    //        System.err.println(stmts);
  }

  /** testGetStatementsConstructor */
  @Test
  public void testGetStatementsConstructor() throws Exception {
    Procedure proc = new DeleteCallForwarding();
    proc.initialize(DatabaseType.POSTGRES);

    // Make sure that procedure handle has the same
    // SQLStmts as what we get back from the static method
    Map<String, SQLStmt> expected = Procedure.getStatements(proc);
    assertNotNull(expected);
    //        System.err.println("EXPECTED:" + expected);

    Map<String, SQLStmt> actual = proc.getStatements();
    assertNotNull(actual);
    //        System.err.println("ACTUAL:" + actual);

    assertEquals(expected.size(), actual.size());
    assertEquals(expected, actual);
  }
}
