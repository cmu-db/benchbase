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
import java.util.List;

public abstract class AbstractTestBenchmarkModule<T extends BenchmarkModule> extends AbstractTestCase<T> {


    public AbstractTestBenchmarkModule() {
        super(false, false);
    }

    @Override
    public List<String> ignorableTables() {
        return null;
    }

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
        }
    }

    /**
     * testGetTransactionType
     */
    public void testGetTransactionType() {
        int id = 1;
        for (Class<? extends Procedure> procClass : procedures()) {
            assertNotNull(procClass);
            TransactionType txnType = new TransactionType(id++, procClass, false, 0, 0);
            assertNotNull(txnType);
            assertEquals(procClass, txnType.getProcedureClass());
        }
    }

    /**
     * testSetSQLDialect
     */
    public void testSetSQLDialect() {
        for (DatabaseType dbType : DatabaseType.values()) {

            StatementDialects dialects = new StatementDialects(this.benchmark.getBenchmarkName(), dbType);


            for (Procedure proc : this.benchmark.getProcedures().values()) {
                if (dialects.getProcedures().contains(proc.getClass())) {
                    Procedure testProc = ClassUtil.newInstance(proc.getClass(), new Object[0], new Class<?>[0]);
                    assertNotNull(testProc);
                    testProc.initialize(dbType);
                    testProc.loadSQLDialect(dialects);

                    Collection<String> dialectStatementNames = dialects.getStatementNames(testProc.getClass());

                    for (String statementName : dialectStatementNames) {
                        SQLStmt stmt = testProc.getStatements().get(statementName);
                        assertNotNull(stmt);
                        String dialectSQL = dialects.getSQL(testProc.getClass(), statementName);
                        assertEquals(dialectSQL, stmt.getOriginalSQL());
                    }
                }
            }
        }
    }

}
