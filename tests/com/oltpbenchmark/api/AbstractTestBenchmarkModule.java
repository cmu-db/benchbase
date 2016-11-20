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

import java.io.File;
import java.net.URL;

import org.apache.commons.io.IOUtils;

import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;

public abstract class AbstractTestBenchmarkModule<T extends BenchmarkModule> extends AbstractTestCase<T> {

    protected static final int NUM_TERMINALS = 10;
    
    /**
     * testGetDatabaseDDL
     */
    public void testGetDatabaseDDL() throws Exception {
        URL ddl = this.benchmark.getDatabaseDDL();
        assertNotNull(ddl);
        assertNotNull (IOUtils.toString(ddl));
    }

    /**
     * testCreateDatabase
     */
    public void testCreateDatabase() throws Exception {
        this.benchmark.createDatabase();

        // Make sure that we get back some tables
        Catalog catalog = this.benchmark.getCatalog();
        assertNotNull(catalog);
        assertFalse(catalog.getTables().isEmpty());

        // Just make sure that there are no empty tables
        for (Table catalog_tbl : catalog.getTables()) {
            assert (catalog_tbl.getColumnCount() > 0) : "Missing columns for " + catalog_tbl;
            System.err.println(catalog_tbl);
        } // FOR
    }
    
    /**
     * testGetTransactionType
     */
    public void testGetTransactionType() throws Exception {
        int id = 1;
        for (Class<? extends Procedure> procClass: this.procClasses) {
            assertNotNull(procClass);
            String procName = procClass.getSimpleName();
            TransactionType txnType = this.benchmark.initTransactionType(procName, id++);
            assertNotNull(txnType);
            assertEquals(procClass, txnType.getProcedureClass());
            System.err.println(procClass + " -> " + txnType);
        } // FOR
    }
    
    /**
     * testGetTransactionTypeInvalidId
     */
    public void testGetTransactionTypeInvalidId() throws Exception {
        Class<? extends Procedure> procClass = this.procClasses.get(0);
        assertNotNull(procClass);
        String procName = procClass.getSimpleName();
        TransactionType txnType = null;
        try {
            txnType = this.benchmark.initTransactionType(procName, TransactionType.INVALID_ID);
        } catch (Throwable ex) {
            // Ignore
        }
        assertNull(txnType);
    }
    
    /**
     * testGetSQLDialect
     */
    public void testGetSQLDialect() throws Exception {
        File xmlFile = this.benchmark.getSQLDialect();
        if (xmlFile != null) {
            assertTrue(xmlFile.getAbsolutePath(), xmlFile.exists());
        }
    }
    
    /**
     * testLoadSQLDialect
     */
    public void testLoadSQLDialect() throws Exception {
        File xmlFile = this.benchmark.getSQLDialect();
        if (xmlFile == null) return;
        
        for (DatabaseType dbType : DatabaseType.values()) {
            this.workConf.setDBType(dbType);
            
            // Just make sure that we can load it
            StatementDialects dialects = new StatementDialects(dbType, xmlFile);
            dialects.load();
            
            for (String procName : dialects.getProcedureNames()) {
                for (String stmtName : dialects.getStatementNames(procName)) {
                    String sql = dialects.getSQL(procName, stmtName);
                    assertNotNull(sql);
                    assertFalse(sql.isEmpty());
                    // System.err.printf("%s.%s:\n%s\n\n", procName, stmtName, sql);
                } // FOR
            } // FOR
            
            // TODO: We should XSD to validate the SQL
        } // FOR (dbtype)
    }
    
    /**
     * testMakeWorkers
     */
//    public void testMakeWorkers() throws Exception {
//        this.workConf.setTerminals(NUM_TERMINALS);
//        List<Worker> workers = this.benchmark.makeWorkers(false);
//        assertNotNull(workers);
//        assertEquals(NUM_TERMINALS, workers.size());
//        assertNotNull(workers.get(0));
//    }
}
