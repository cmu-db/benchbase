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

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractTestBenchmarkModule<T extends BenchmarkModule> extends AbstractTestCase<T> {

    protected static final int NUM_TERMINALS = 10;

    /**
     * testGetDatabaseDDLPath
     */
    public void testGetDatabaseDDLPath() throws Exception {
        String ddlPath = this.benchmark.getDatabaseDDLPath(this.workConf.getDatabaseType());
        assertNotNull(ddlPath);
        try (InputStream stream = this.getClass().getClassLoader().getResourceAsStream(ddlPath)) {
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
            TransactionType txnType = this.benchmark.initTransactionType(procName, id++);
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
            txnType = this.benchmark.initTransactionType(procName, TransactionType.INVALID_ID);
        } catch (Throwable ex) {
            // Ignore
        }
        assertNull(txnType);
    }

    /**
     * testGetSQLDialectPath
     */
    public void testGetSQLDialectPath() throws Exception {
        for (DatabaseType dbType : DatabaseType.values()) {
            String xmlFilePath = this.benchmark.getStatementDialects().getSQLDialectPath(dbType);
            if (xmlFilePath != null) {
                URL xmlUrl = this.getClass().getClassLoader().getResource(xmlFilePath);
                assertNotNull(xmlUrl);
                File xmlFile = new File(xmlUrl.toURI());
                assertTrue(xmlFile.getAbsolutePath(), xmlFile.exists());
            }
        }
    }

    /**
     * testLoadSQLDialect
     */
    public void testLoadSQLDialect() throws Exception {
        for (DatabaseType dbType : DatabaseType.values()) {
            this.workConf.setDatabaseType(dbType);

            // Just make sure that we can load it
            StatementDialects dialects = new StatementDialects(this.workConf);
            if (dialects.load()) {

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
    }


    /**
     * testDumpSQLDialect
     */
    public void testDumpSQLDialect() throws Exception {
        for (DatabaseType dbType : DatabaseType.values()) {
            this.workConf.setDatabaseType(dbType);

            StatementDialects dialects = new StatementDialects(this.workConf);
            if (dialects.load()) {
                String dump = dialects.export(dbType, this.benchmark.getProcedures().values());
                assertNotNull(dump);
                assertFalse(dump.isEmpty());
                Set<String> benchmarkProcedureNames = this.benchmark.getProcedures().values()
                    .stream()
                    .map(Procedure::getProcedureName)
                    .collect(Collectors.toSet());
                for (String procName : dialects.getProcedureNames()) {
                    if (benchmarkProcedureNames.contains(procName)) {
                        assertTrue(procName, dump.contains(procName));
                        for (String stmtName : dialects.getStatementNames(procName)) {
                            assertTrue(procName + "." + stmtName, dump.contains(stmtName));
                        }
                    }
                }
            }
        }
    }


    /**
     * testSetSQLDialect
     */
    public void testSetSQLDialect() throws Exception {
        for (DatabaseType dbType : DatabaseType.values()) {
            this.workConf.setDatabaseType(dbType);

            StatementDialects dialects = new StatementDialects(this.workConf);
            if (dialects.load()) {

                for (Procedure proc : this.benchmark.getProcedures().values()) {
                    if (dialects.getProcedureNames().contains(proc.getProcedureName())) {
                        // Need a new proc because the dialect gets loaded in BenchmarkModule::getProcedureName
                        Procedure testProc = ClassUtil.newInstance(proc.getClass().getName(),
                            new Object[0], new Class<?>[0]);
                        assertNotNull(testProc);
                        testProc.initialize(dbType);
                        testProc.loadSQLDialect(dialects);

                        Collection<String> dialectStatementNames = dialects.getStatementNames(
                            testProc.getProcedureName());

                        for (String statementName : dialectStatementNames) {
                            SQLStmt stmt = testProc.getStatements().get(statementName);
                            assertNotNull(stmt);
                            String dialectSQL = dialects.getSQL(testProc.getProcedureName(),
                                statementName);
                            assertEquals(dialectSQL, stmt.getOriginalSQL());
                        }
                    }
                }
            }
        }
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
