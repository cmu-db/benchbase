package com.oltpbenchmark.api;

import java.util.Map;

import com.oltpbenchmark.benchmarks.tatp.procedures.DeleteCallForwarding;
import com.oltpbenchmark.types.DatabaseType;

import junit.framework.TestCase;

public class TestProcedure extends TestCase {

    @Override
    protected void setUp() throws Exception {
        
    }
    
    /**
     * testGetProcedureName
     */
    public void testGetProcedureName() throws Exception {
        DeleteCallForwarding proc = new DeleteCallForwarding();
        assertEquals(DeleteCallForwarding.class.getSimpleName(), proc.getProcedureName());
    }
    
    /**
     * testGetStatments
     */
    public void testGetStatments() throws Exception {
        Map<String, SQLStmt> stmts = Procedure.getStatments(new DeleteCallForwarding());
        assertNotNull(stmts);
        assertEquals(2, stmts.size());
        System.err.println(stmts);
    }
    
    /**
     * testGetStatmentsConstructor
     */
    public void testGetStatmentsConstructor() throws Exception {
        Procedure proc = new DeleteCallForwarding();
        proc.initialize(DatabaseType.POSTGRES);
        
        // Make sure that procedure handle has the same
        // SQLStmts as what we get back from the static method
        Map<String, SQLStmt> expected = Procedure.getStatments(proc);
        assertNotNull(expected);
        System.err.println("EXPECTED:" + expected);
        
        Map<String, SQLStmt> actual = proc.getStatments();
        assertNotNull(actual);
        System.err.println("ACTUAL:" + actual);
        
        assertEquals(expected.size(), actual.size());
        assertEquals(expected, actual);
        
        
    }
    
}
