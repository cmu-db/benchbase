/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package com.oltpbenchmark.api;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.SQLUtil;
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
     * The workload configuration for this benchmark invocation
     */
    protected final WorkloadConfiguration workConf;

    /**
     * These are the variations of the Procedure's Statement SQL
     */
    protected final StatementDialects dialects;

    /**
     * Supplemental Procedures
     */
    private final Set<Class<? extends Procedure>> supplementalProcedures = new HashSet<>();

    /**
     * A single Random object that should be re-used by all a benchmark's components
     */
    private final Random rng = new Random();

    private final HikariDataSource dataSource;

    private Catalog catalog = null;

    public BenchmarkModule(WorkloadConfiguration workConf) {

        this.workConf = workConf;
        this.dialects = new StatementDialects(workConf);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(workConf.getUrl());
        config.setUsername(workConf.getUsername());
        config.setPassword(workConf.getPassword());
        config.setMaximumPoolSize(workConf.getPoolSize());

        dataSource = new HikariDataSource(config);
    }

    // --------------------------------------------------------------------------
    // DATABASE CONNECTION
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
     * @return
     * @throws IOException
     */
    protected abstract List<Worker<? extends BenchmarkModule>> makeWorkersImpl() throws IOException;

    /**
     * Each BenchmarkModule needs to implement this method to load a sample
     * dataset into the database. The Connection handle will already be
     * configured for you, and the base class will commit+close it once this
     * method returns
     *
     * @return TODO
     */
    protected abstract Loader<? extends BenchmarkModule> makeLoaderImpl();

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
     * Return the URL handle to the DDL used to load the benchmark's database
     * schema.
     *
     * @param db_type
     * @throws SQLException
     */
    public String getDatabaseDDLPath(DatabaseType db_type) {

        List<String> names = new ArrayList<>();
        if (db_type != null) {
            names.add("ddl-" + db_type.name().toLowerCase() + ".sql");
        }

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


    public final List<Worker<? extends BenchmarkModule>> makeWorkers() throws IOException {
        return (this.makeWorkersImpl());
    }

    /**
     * Create the Benchmark Database
     * This is the main method used to create all the database
     * objects (e.g., table, indexes, etc) needed for this benchmark
     */
    public final void createDatabase() {
        this.createDatabase(this.workConf.getDatabaseType());
    }

    /**
     * Create the Benchmark Database
     * This is the main method used to create all the database
     * objects (e.g., table, indexes, etc) needed for this benchmark
     */
    public final void createDatabase(DatabaseType dbType) {
        try (Connection conn = this.getConnection()) {
            String ddlPath = this.getDatabaseDDLPath(dbType);

            ScriptRunner runner = new ScriptRunner(conn, true, true);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Executing script [{}] for database type [{}]", ddlPath, dbType);
            }

            runner.runScript(ddlPath);

            this.catalog = SQLUtil.getCatalog(dbType, conn);

        } catch (Exception ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to create the %s database", getBenchmarkName()), ex);
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


                if (!loader.getTableCounts().isEmpty()) {
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

    public final void clearDatabase() {

        try (Connection conn = this.getConnection()) {
            this.catalog = SQLUtil.getCatalog(this.getWorkloadConfiguration().getDatabaseType(), conn);

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

        if (catalog == null) {
            throw new RuntimeException("getCatalog() has been called before create database");
        }

        return this.catalog;
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
     * @return
     */
    public Map<TransactionType, Procedure> getProcedures() {
        Map<TransactionType, Procedure> proc_xref = new HashMap<>();
        TransactionTypes txns = this.workConf.getTransTypes();

        if (txns != null) {
            for (Class<? extends Procedure> procClass : this.supplementalProcedures) {
                TransactionType txn = txns.getType(procClass);
                if (txn == null) {
                    txn = new TransactionType(procClass, procClass.hashCode());
                    txns.add(txn);
                }
            }

            for (TransactionType txn : txns) {
                Procedure proc = ClassUtil.newInstance(txn.getProcedureClass(), new Object[0], new Class<?>[0]);
                proc.initialize(this.workConf.getDatabaseType());
                proc_xref.put(txn, proc);
                proc.loadSQLDialect(this.dialects);
            }
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
