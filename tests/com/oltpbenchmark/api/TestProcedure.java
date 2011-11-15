package com.oltpbenchmark.api;

import java.util.Map;

import com.oltpbenchmark.benchmarks.tatp.procedures.DeleteCallForwarding;

import junit.framework.TestCase;

public class TestProcedure extends TestCase {

    @Override
    protected void setUp() throws Exception {
        
    }
    
    /**
     * testGetStatments
     */
    public void testGetStatments() throws Exception {
        Procedure proc = new DeleteCallForwarding();
        assertNotNull(proc);
        
        Map<String, SQLStmt> stmts = Procedure.getStatments(proc);
        assertNotNull(stmts);
        assertEquals(2, stmts.size());
        
        System.err.println(stmts);
    }
    
}
