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

import com.oltpbenchmark.catalog.AbstractCatalog;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;

import java.io.InputStream;
import java.util.Collection;

public abstract class AbstractTestBenchmarkModule<T extends BenchmarkModule> extends AbstractTestCase<T> {

    protected static final int NUM_TERMINALS = 10;

    /**
     * testGetDatabaseDDLPath
     */
    public void testGetDatabaseDDLPath() throws Exception {
        String ddlPath = this.benchmark.getDatabaseDDLPath(this.workConf.getDatabaseType());
        assertNotNull(ddlPath);
        try (InputStream stream = this.getClass().getResourceAsStream(ddlPath)) {
            assertNotNull(stream);
        }
    }

    /**
     * testCreateDatabase
     */
    public void testCreateDatabase() throws Exception {
        this.benchmark.createDatabase();

        // Make sure that we get back some tables
        this.benchmark.refreshCatalog();
        AbstractCatalog catalog = this.benchmark.getCatalog();
        assertNotNull(catalog);
        assertFalse(catalog.getTables().isEmpty());

        // Just make sure that there are no empty tables
        for (Table catalog_tbl : catalog.getTables()) {
            assert (catalog_tbl.getColumnCount() > 0) : "Missing columns for " + catalog_tbl;
//            System.err.println(catalog_tbl);
        } // FOR
    }

    /**
     * testGetTransactionType
     */
    public void testGetTransactionType() throws Exception {
        int id = 1;
        for (Class<? extends Procedure> procClass : this.procClasses) {
            assertNotNull(procClass);
            String procName = procClass.getSimpleName();
            TransactionType txnType = this.benchmark.initTransactionType(procName, id++, 0, 0);
            assertNotNull(txnType);
            assertEquals(procClass, txnType.getProcedureClass());
//            System.err.println(procClass + " -> " + txnType);
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
            txnType = this.benchmark.initTransactionType(procName, TransactionType.INVALID_ID, 0, 0);
        } catch (Throwable ex) {
            // Ignore
        }
        assertNull(txnType);
    }


    /**
     * testSetSQLDialect
     */
    public void testSetSQLDialect() throws Exception {
        for (DatabaseType dbType : DatabaseType.values()) {

            StatementDialects dialects = new StatementDialects(this.workConf.getBenchmarkName(), dbType);


            for (Procedure proc : this.benchmark.getProcedures().values()) {
                if (dialects.getProcedureNames().contains(proc.getProcedureName())) {
                    // Need a new proc because the dialect gets loaded in BenchmarkModule::getProcedureName
                    Procedure testProc = ClassUtil.newInstance(proc.getClass().getName(), new Object[0], new Class<?>[0]);
                    assertNotNull(testProc);
                    testProc.initialize(dbType);
                    testProc.loadSQLDialect(dialects);

                    Collection<String> dialectStatementNames = dialects.getStatementNames(testProc.getProcedureName());

                    for (String statementName : dialectStatementNames) {
                        SQLStmt stmt = testProc.getStatements().get(statementName);
                        assertNotNull(stmt);
                        String dialectSQL = dialects.getSQL(testProc.getProcedureName(), statementName);
                        assertEquals(dialectSQL, stmt.getOriginalSQL());
                    }
                }
            }

        }
    }

}
