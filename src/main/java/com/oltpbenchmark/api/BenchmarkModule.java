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
import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.ScriptRunner;
import com.oltpbenchmark.util.ThreadUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * Base class for all benchmark implementations
 */
public abstract class BenchmarkModule {
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkModule.class);

    /**
     * Each benchmark must put their all of the DBMS-specific DDLs
     * in this directory.
     */
    public static final String DDLS_DIR = "ddls";


    /**
     * Each dialect xml file  must put their all of the DBMS-specific DIALECTs
     * in this directory.
     */
    public static final String DIALECTS_DIR = "dialects";


    /**
     * The workload configuration for this benchmark invocation
     */
    protected final WorkloadConfiguration workConf;

    /**
     * These are the variations of the Procedure's Statment SQL
     */
    protected final StatementDialects dialects;

    /**
     * Database Catalog
     */
    protected final Catalog catalog;

    /**
     * Supplemental Procedures
     */
    private final Set<Class<? extends Procedure>> supplementalProcedures = new HashSet<>();

    /**
     * A single Random object that should be re-used by all a benchmark's components
     */
    private final Random rng = new Random();

    /**
     * Whether to use verbose output messages
     *
     * @deprecated
     */
    protected boolean verbose;

    private HikariDataSource dataSource = null;

    public BenchmarkModule(WorkloadConfiguration workConf, boolean withCatalog) {


        this.workConf = workConf;
        this.catalog = (withCatalog ? new Catalog(this) : null);
        this.dialects = new StatementDialects(workConf);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(workConf.getDBConnection());
        config.setUsername(workConf.getDBUsername());
        config.setPassword(workConf.getDBPassword());

        dataSource = new HikariDataSource(config);
    }

    // --------------------------------------------------------------------------
    // DATABASE CONNETION
    // --------------------------------------------------------------------------

    /**
     * @return
     * @throws SQLException
     */
    public final Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    // --------------------------------------------------------------------------
    // IMPLEMENTING CLASS INTERFACE
    // --------------------------------------------------------------------------

    /**
     * @param verbose
     * @return
     * @throws IOException
     */
    protected abstract List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException;

    /**
     * Each BenchmarkModule needs to implement this method to load a sample
     * dataset into the database. The Connection handle will already be
     * configured for you, and the base class will commit+close it once this
     * method returns
     *
     * @return TODO
     * @throws SQLException TODO
     */
    protected abstract Loader<? extends BenchmarkModule> makeLoaderImpl() throws SQLException;

    /**
     * @param txns
     * @return
     */
    protected abstract Package getProcedurePackageImpl();

    // --------------------------------------------------------------------------
    // PUBLIC INTERFACE
    // --------------------------------------------------------------------------

    /**
     * Return the Random generator that should be used by all this benchmark's components
     */
    public Random rng() {
        return (this.rng);
    }

    /**
     * @return
     */
    public String getDatabaseDDLPath() {
        return (this.getDatabaseDDLPath(this.workConf.getDBType()));
    }

    /**
     * Return the URL handle to the DDL used to load the benchmark's database
     * schema.
     *
     * @param conn
     * @throws SQLException
     */
    public String getDatabaseDDLPath(DatabaseType db_type) {

        List<String> names = new ArrayList<>();
        if (db_type != null) {
            names.add("ddl-" + db_type.name().toLowerCase() + ".sql");
        }

        names.add("ddl.sql");

        for (String fileName : names) {

            final String path = "benchmarks" + File.separator + getBenchmarkName() + File.separator + fileName;

            try (InputStream stream = this.getClass().getClassLoader().getResourceAsStream(path)) {

                if (stream != null) {
                    return path;
                }

            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }


        return null;
    }


    public final List<Worker<? extends BenchmarkModule>> makeWorkers(boolean verbose) throws IOException {
        return (this.makeWorkersImpl(verbose));
    }

    /**
     * Create the Benchmark Database
     * This is the main method used to create all the database
     * objects (e.g., table, indexes, etc) needed for this benchmark
     */
    public final void createDatabase() {
        try (Connection conn = this.getConnection()) {
            this.createDatabase(this.workConf.getDBType(), conn);
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to create the %s database", getBenchmarkName()), ex);
        }
    }

    /**
     * Create the Benchmark Database
     * This is the main method used to create all the database
     * objects (e.g., table, indexes, etc) needed for this benchmark
     */
    public final void createDatabase(DatabaseType dbType, Connection conn) throws SQLException {
        try {
            String ddlPath = this.getDatabaseDDLPath(dbType);

            ScriptRunner runner = new ScriptRunner(conn, true, true);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Executing script [{}] for database type [{}]", ddlPath, dbType);
            }

            runner.runScript(ddlPath);
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to create the %s database", getBenchmarkName()), ex);
        }
    }

    /**
     * Run a scriptPath on a Database
     */
    public final void runScript(String scriptPath) {
        try (Connection conn = this.getConnection()) {
            ScriptRunner runner = new ScriptRunner(conn, true, true);
            runner.runScript(scriptPath);
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to run: %s", scriptPath), ex);
        } catch (IOException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to open: %s", scriptPath), ex);
        }
    }

    /**
     * Invoke this benchmark's database loader
     */
    public final void loadDatabase() {

        try {
            Loader<? extends BenchmarkModule> loader = this.makeLoaderImpl();
            if (loader != null) {


                // PAVLO: 2016-12-23
                // We are going to eventually migrate everything over to use the
                // same API for creating multi-threaded loaders. For now we will support
                // both. So if createLoaderTheads() returns null, we will use the old load()
                // method.
                List<? extends LoaderThread> loaderThreads = loader.createLoaderThreads();
                int maxConcurrent = workConf.getLoaderThreads();

                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Starting %d %s.LoaderThreads [maxConcurrent=%d]", loaderThreads.size(), loader.getClass().getSimpleName(), maxConcurrent));
                }

                ThreadUtil.runNewPool(loaderThreads, maxConcurrent);


                if (loader.getTableCounts().isEmpty() == false) {
                    LOG.info("Table Counts:\n{}", loader.getTableCounts());
                }

            }
        } catch (SQLException ex) {
            String msg = String.format("Unexpected error when trying to load the %s database", getBenchmarkName());
            throw new RuntimeException(msg, ex);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Finished loading the %s database", this.getBenchmarkName().toUpperCase()));
        }
    }

    /**
     * @param DB_CONN
     * @throws SQLException
     */
    public final void clearDatabase() {
        try (Connection conn = this.getConnection()) {
            Loader<? extends BenchmarkModule> loader = this.makeLoaderImpl();
            if (loader != null) {
                conn.setAutoCommit(false);
                loader.unload(conn, this.catalog);
                conn.commit();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to delete the %s database", getBenchmarkName()), ex);
        }
    }

    // --------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------

    /**
     * Return the unique identifier for this benchmark
     */
    public final String getBenchmarkName() {
        return this.workConf.getBenchmarkName();
    }

    /**
     * Return the database's catalog
     */
    public final Catalog getCatalog() {
        return (this.catalog);
    }

    /**
     * Get the catalog object for the given table name
     *
     * @param tableName
     * @return
     */
    public Table getTableCatalog(String tableName) {
        Table catalog_tbl = this.catalog.getTable(tableName.toUpperCase());

        return (catalog_tbl);
    }

    /**
     * Return the StatementDialects loaded for this benchmark
     */
    public final StatementDialects getStatementDialects() {
        return (this.dialects);
    }

    @Override
    public final String toString() {
        return getBenchmarkName();
    }


    /**
     * Initialize a TransactionType handle for the get procedure name and id
     * This should only be invoked a start-up time
     *
     * @param procName
     * @param id
     * @return
     */

    public final TransactionType initTransactionType(String procName, int id) {
        if (id == TransactionType.INVALID_ID) {
            LOG.error(String.format("Procedure %s.%s cannot use the reserved id '%d' for %s", getBenchmarkName(), procName, id, TransactionType.INVALID.getClass().getSimpleName()));
            return null;
        }

        Package pkg = this.getProcedurePackageImpl();

        String fullName = pkg.getName() + "." + procName;
        Class<? extends Procedure> procClass = (Class<? extends Procedure>) ClassUtil.getClass(fullName);

        return new TransactionType(procClass, id);
    }

    public final WorkloadConfiguration getWorkloadConfiguration() {
        return (this.workConf);
    }

    /**
     * Return a mapping from TransactionTypes to Procedure invocations
     *
     * @param txns
     * @param pkg
     * @return
     */
    public Map<TransactionType, Procedure> getProcedures() {
        Map<TransactionType, Procedure> proc_xref = new HashMap<>();
        TransactionTypes txns = this.workConf.getTransTypes();

        if (txns != null) {
            for (Class<? extends Procedure> procClass : this.supplementalProcedures) {
                TransactionType txn = txns.getType(procClass);
                if (txn == null) {
                    txn = new TransactionType(procClass, procClass.hashCode(), true);
                    txns.add(txn);
                }
            } // FOR

            for (TransactionType txn : txns) {
                Procedure proc = ClassUtil.newInstance(txn.getProcedureClass(), new Object[0], new Class<?>[0]);
                proc.initialize(this.workConf.getDBType());
                proc_xref.put(txn, proc);
                proc.loadSQLDialect(this.dialects);
            } // FOR
        }
        if (proc_xref.isEmpty()) {
            LOG.warn("No procedures defined for {}", this);
        }
        return (proc_xref);
    }

    /**
     * @param procClass
     */
    public final void registerSupplementalProcedure(Class<? extends Procedure> procClass) {
        this.supplementalProcedures.add(procClass);
    }

}
