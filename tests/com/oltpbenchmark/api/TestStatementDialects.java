package com.oltpbenchmark.api;

import java.io.File;
import java.util.Collection;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.benchmarks.epinions.EpinionsBenchmark;
import com.oltpbenchmark.types.DatabaseType;

import junit.framework.TestCase;

public class TestStatementDialects extends TestCase {
    
//    static {
//      org.apache.log4j.PropertyConfigurator.configure("/home/pavlo/Documents/OLTPBenchmark/OLTPBenchmark/log4j.properties");
//    }
    
    private EpinionsBenchmark benchmark;
    private WorkloadConfiguration workConf;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        this.workConf = new WorkloadConfiguration();
        this.benchmark = new EpinionsBenchmark(this.workConf);
    }
    
    /**
     * testLoad
     */
    public void testLoad() throws Exception {
        for (DatabaseType dbType : DatabaseType.values()) {
            this.workConf.setDBType(dbType);
            File xmlFile = this.benchmark.getSQLDialect();
            assertNotNull(dbType.toString(), xmlFile);
            
            StatementDialects dialects = new StatementDialects(dbType, xmlFile);
            boolean ret = dialects.load();
            if (ret == false) continue;
            
            Collection<String> procNames = dialects.getProcedureNames();
            assertNotNull(dbType.toString(), procNames);
            assertFalse(dbType.toString(), procNames.isEmpty());
            
            for (String procName : procNames) {
                assertFalse(procName.isEmpty());
                Collection<String> stmtNames = dialects.getStatementNames(procName);
                assertNotNull(procName, stmtNames);
                assertFalse(procName, stmtNames.isEmpty());
                
                for (String stmtName : stmtNames) {
                    assertFalse(stmtName.isEmpty());
                    String stmtSQL = dialects.getSQL(procName, stmtName);
                    assertNotNull(stmtSQL);
                    assertFalse(stmtSQL.isEmpty());
                } // FOR (stmt)
            } // FOR (proc)
        } // FOR (dbtype)
    }
}
