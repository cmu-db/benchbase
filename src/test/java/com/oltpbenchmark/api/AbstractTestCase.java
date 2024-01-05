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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.catalog.AbstractCatalog;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.hsqldb.Database;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.Server;
import org.hsqldb.server.ServerConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTestCase<T extends BenchmarkModule> {

  protected final Logger LOG = LoggerFactory.getLogger(getClass());

  // -----------------------------------------------------------------

  /**
   * This is the database type that we will use in our unit tests. This should always be one of the
   * embedded java databases
   */
  private static final DatabaseType DB_TYPE = DatabaseType.HSQLDB;

  // -----------------------------------------------------------------

  protected static final double DB_SCALE_FACTOR = 0.01;

  private Server server = null;

  protected WorkloadConfiguration workConf;
  protected T benchmark;
  protected AbstractCatalog catalog;
  protected Connection conn;

  protected final boolean createDatabase;
  protected final boolean loadDatabase;
  protected final String ddlOverridePath;

  private static final AtomicInteger portCounter = new AtomicInteger(9001);
  private static final int MAX_PORT_NUMBER = 65535;

  public AbstractTestCase(boolean createDatabase, boolean loadDatabase) {
    this.benchmark = null;
    this.createDatabase = createDatabase;
    this.loadDatabase = loadDatabase;
    this.ddlOverridePath = null;
  }

  public AbstractTestCase(boolean createDatabase, boolean loadDatabase, String ddlOverridePath) {
    this.benchmark = null;
    this.createDatabase = createDatabase;
    this.loadDatabase = loadDatabase;
    this.ddlOverridePath = ddlOverridePath;
  }

  public abstract List<Class<? extends Procedure>> procedures();

  public abstract Class<T> benchmarkClass();

  public abstract List<String> ignorableTables();

  @Rule public TestName name = new TestName();

  @Before
  public final void setUp() throws Exception {
    HsqlProperties props = new HsqlProperties();
    // props.setProperty("server.remote_open", true);

    int port = findAvailablePort();

    LOG.info("starting HSQLDB server for test [{}] on port [{}]", name.getMethodName(), port);

    server = new Server();
    server.setProperties(props);
    server.setDatabasePath(0, "mem:benchbase;sql.syntax_mys=true");
    server.setDatabaseName(0, "benchbase");
    server.setAddress("localhost");
    server.setPort(port);
    server.setSilent(true);
    server.setLogWriter(null);
    server.start();

    this.workConf = new WorkloadConfiguration();

    String DB_CONNECTION =
        String.format("jdbc:hsqldb:hsql://localhost:%d/benchbase", server.getPort());

    this.workConf.setTransTypes(proceduresToTransactionTypes(procedures()));
    this.workConf.setDatabaseType(DB_TYPE);
    this.workConf.setUrl(DB_CONNECTION);
    this.workConf.setScaleFactor(DB_SCALE_FACTOR);
    this.workConf.setTerminals(1);
    this.workConf.setBatchSize(128);
    this.workConf.setBenchmarkName(
        BenchmarkModule.convertBenchmarkClassToBenchmarkName(benchmarkClass()));
    this.workConf.setDDLPath(this.ddlOverridePath);

    customWorkloadConfiguration(this.workConf);

    this.benchmark =
        ClassUtil.newInstance(
            benchmarkClass(),
            new Object[] {this.workConf},
            new Class<?>[] {WorkloadConfiguration.class});
    assertNotNull(this.benchmark);

    // HACK: calling this a second time is a cheap no-op for most benchmark
    // tests, but actually ensures that the procedures list is populated
    // for the TestTemplatedWorker test which doesn't know its procedures
    // until after the benchmark is initialized and the config is loaded.
    var proceedures = this.procedures();
    assertNotNull(proceedures);
    if (!(this instanceof TestDDLOverride)) {
      assertFalse(proceedures.isEmpty());
    }

    this.conn = this.benchmark.makeConnection();
    assertNotNull(this.conn);

    this.benchmark.refreshCatalog();
    this.catalog = this.benchmark.getCatalog();
    assertNotNull(this.catalog);

    if (createDatabase) {
      this.createDatabase();
    }

    if (loadDatabase) {
      this.loadDatabase();
    }

    try {
      postCreateDatabaseSetup();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      cleanupServer();
      fail("postCreateDatabaseSetup() failed");
    }
  }

  private int findAvailablePort() throws IOException {
    while (true) {
      int port = portCounter.incrementAndGet();

      if (port > MAX_PORT_NUMBER) {
        throw new IOException("No available port found up to " + MAX_PORT_NUMBER);
      }

      try (ServerSocket testSocket = new ServerSocket(port)) {
        assert testSocket != null;
        return port;
      } catch (BindException e) {
        // This port is already in use. Continue to next port.
        LOG.warn("Port {} is already in use. Trying next port.", port);
      }
    }
  }

  protected TransactionTypes proceduresToTransactionTypes(
      List<Class<? extends Procedure>> procedures) {
    TransactionTypes txnTypes = new TransactionTypes(new ArrayList<>());

    int id = 0;
    for (Class<? extends Procedure> procedureClass : procedures) {
      TransactionType tt = new TransactionType(procedureClass, id++, false, 0, 0);
      txnTypes.add(tt);
    }

    return txnTypes;
  }

  protected void createDatabase() {
    try {
      this.benchmark.createDatabase();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      cleanupServer();
      fail("createDatabase() failed");
    }
  }

  protected void loadDatabase() {
    try {
      this.benchmark.loadDatabase();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      cleanupServer();
      fail("loadDatabase() failed");
    }
  }

  protected void customWorkloadConfiguration(WorkloadConfiguration workConf) {}

  protected void postCreateDatabaseSetup() throws IOException {}

  @After
  public final void tearDown() throws Exception {

    if (this.conn != null) {
      this.conn.close();
    }

    cleanupServer();
  }

  protected void cleanupServer() {
    if (server != null) {

      LOG.trace("shutting down catalogs...");
      server.shutdownCatalogs(Database.CLOSEMODE_NORMAL);

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
