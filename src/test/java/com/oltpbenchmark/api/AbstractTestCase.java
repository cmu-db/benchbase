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

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.config.Database;
import com.oltpbenchmark.api.config.TransactionIsolation;
import com.oltpbenchmark.api.config.Workload;
import com.oltpbenchmark.catalog.AbstractCatalog;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;
import junit.framework.TestCase;
import org.hsqldb.jdbc.JDBCDriver;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.Server;
import org.hsqldb.server.ServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hsqldb.Database.CLOSEMODE_NORMAL;

public abstract class AbstractTestCase<T extends BenchmarkModule> extends TestCase {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    // -----------------------------------------------------------------

    /**
     * This is the database type that we will use in our unit tests.
     * This should always be one of the embedded java databases
     */
    private static final DatabaseType DB_TYPE = DatabaseType.HSQLDB;

    // -----------------------------------------------------------------

    private static final double DB_SCALE_FACTOR = 0.01;
    private static final int DB_TERMINALS = 1;

    private Server server = null;

    protected WorkloadConfiguration workConf;
    protected T benchmark;
    protected AbstractCatalog catalog;
    protected Connection conn;

    protected final boolean createDatabase;
    protected final boolean loadDatabase;

    private static final AtomicInteger portCounter = new AtomicInteger(9001);


    public AbstractTestCase(boolean createDatabase, boolean loadDatabase) {
        this.createDatabase = createDatabase;
        this.loadDatabase = loadDatabase;
    }

    public abstract List<Class<? extends Procedure>> procedures();

    public abstract Class<T> benchmarkClass();

    public abstract List<String> ignorableTables();

    @Override
    protected final void setUp() throws Exception {
        HsqlProperties props = new HsqlProperties();

        int port = portCounter.incrementAndGet();

        LOG.info("starting HSQLDB server for test [{}] on port [{}]", this.getName(), port);

        server = new Server();
        server.setProperties(props);
        server.setDatabasePath(0, "mem:benchbase;sql.syntax_mys=true");
        server.setDatabaseName(0, "benchbase");
        server.setAddress("localhost");
        server.setPort(port);
        server.setSilent(true);
        server.setLogWriter(null);
        server.start();

        String DB_CONNECTION = String.format("jdbc:hsqldb:hsql://%s:%d/%s", server.getAddress(), server.getPort(), server.getDatabaseName(0, true));


        Database database = new Database(DB_TYPE, JDBCDriver.class, DB_CONNECTION, null, null, TransactionIsolation.TRANSACTION_SERIALIZABLE, 128, 3);
        Workload workload = new Workload(benchmarkClass(), scaleFactor(), null, terminals(), null, null, null, null);


        TransactionTypes txnTypes = new TransactionTypes(new ArrayList<>());

        int id = 0;
        for (Class<? extends Procedure> procedureClass : procedures()) {
            TransactionType tt = new TransactionType(id++, procedureClass, false, 0, 0);
            txnTypes.add(tt);
        }

        this.workConf = new WorkloadConfiguration(database, workload, txnTypes, new ArrayList<>());

        this.benchmark = ClassUtil.newInstance(benchmarkClass(),
                new Object[]{this.workConf},
                new Class<?>[]{WorkloadConfiguration.class});
        assertNotNull(this.benchmark);

        this.conn = this.benchmark.makeConnection();
        assertNotNull(this.conn);

        this.benchmark.refreshCatalog();
        this.catalog = this.benchmark.getCatalog();
        assertNotNull(this.catalog);

        if (createDatabase) {
            try {
                this.benchmark.createDatabase();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                cleanupServer();
                fail("createDatabase() failed");
            }
        }

        if (loadDatabase) {
            try {
                this.benchmark.loadDatabase();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                cleanupServer();
                fail("loadDatabase() failed");
            }
        }

        try {
            postCreateDatabaseSetup();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            cleanupServer();
            fail("postCreateDatabaseSetup() failed");
        }
    }

    protected double scaleFactor() {
        return DB_SCALE_FACTOR;
    }

    protected int terminals() {
        return DB_TERMINALS;
    }

    protected void postCreateDatabaseSetup() throws IOException {

    }


    @Override
    protected final void tearDown() throws Exception {

        if (this.conn != null) {
            this.conn.close();
        }

        cleanupServer();
    }

    private void cleanupServer() {
        if (server != null) {

            LOG.trace("shutting down catalogs...");
            server.shutdownCatalogs(CLOSEMODE_NORMAL);

            LOG.trace("stopping server...");
            server.stop();

            while (server.getState() != ServerConstants.SERVER_STATE_SHUTDOWN) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignore) {
                }
            }

            LOG.trace("shutting down server...");
            server.shutdown();

        }
    }
}
