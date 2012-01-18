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

import java.io.File;

import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;

public abstract class AbstractTestBenchmarkModule<T extends BenchmarkModule> extends AbstractTestCase<T> {

    protected static final int NUM_TERMINALS = 10;
    
    /**
     * testGetDatabaseDDL
     */
    public void testGetDatabaseDDL() throws Exception {
        File ddl = this.benchmark.getDatabaseDDL();
        assertNotNull(ddl);
        assert (ddl.exists());
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
            TransactionType txnType = this.benchmark.getTransactionType(procName, id++);
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
            txnType = this.benchmark.getTransactionType(procName, TransactionType.INVALID_ID);
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
            boolean ret = dialects.load();
            
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
