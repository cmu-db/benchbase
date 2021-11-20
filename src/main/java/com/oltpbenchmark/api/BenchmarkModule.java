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
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkBenchmark;
import com.oltpbenchmark.benchmarks.chbenchmark.CHBenCHmark;
import com.oltpbenchmark.benchmarks.epinions.EpinionsBenchmark;
import com.oltpbenchmark.benchmarks.hyadapt.HYADAPTBenchmark;
import com.oltpbenchmark.benchmarks.noop.NoOpBenchmark;
import com.oltpbenchmark.benchmarks.resourcestresser.ResourceStresserBenchmark;
import com.oltpbenchmark.benchmarks.seats.SEATSBenchmark;
import com.oltpbenchmark.benchmarks.sibench.SIBenchmark;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankBenchmark;
import com.oltpbenchmark.benchmarks.tatp.TATPBenchmark;
import com.oltpbenchmark.benchmarks.tpcc.TPCCBenchmark;
import com.oltpbenchmark.benchmarks.tpcds.TPCDSBenchmark;
import com.oltpbenchmark.benchmarks.tpch.TPCHBenchmark;
import com.oltpbenchmark.benchmarks.twitter.TwitterBenchmark;
import com.oltpbenchmark.benchmarks.voter.VoterBenchmark;
import com.oltpbenchmark.benchmarks.wikipedia.WikipediaBenchmark;
import com.oltpbenchmark.benchmarks.ycsb.YCSBBenchmark;
import com.oltpbenchmark.catalog.AbstractCatalog;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.ScriptRunner;
import com.oltpbenchmark.util.ThreadUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
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

    private AbstractCatalog catalog = null;

    public BenchmarkModule(WorkloadConfiguration workConf) {
        this.workConf = workConf;
        this.dialects = new StatementDialects(workConf);
    }

    // --------------------------------------------------------------------------
    // DATABASE CONNECTION
    // --------------------------------------------------------------------------

    public final Connection makeConnection() throws SQLException {

        if (StringUtils.isEmpty(workConf.getUsername())) {
            return DriverManager.getConnection(workConf.getUrl());
        } else {
            return DriverManager.getConnection(
                    workConf.getUrl(),
                    workConf.getUsername(),
                    workConf.getPassword());
        }
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

    private String convertBenchmarkClassToBenchmarkName() {
        return convertBenchmarkClassToBenchmarkName(this.getClass());
    }

    protected static <T> String convertBenchmarkClassToBenchmarkName(Class<T> clazz) {
        if (clazz == AuctionMarkBenchmark.class) {
            return "auctionmark";
        } else if (clazz == CHBenCHmark.class) {
            return "chbenchmark";
        } else if (clazz == EpinionsBenchmark.class) {
            return "epinions";
        } else if (clazz == HYADAPTBenchmark.class) {
            return "hyadapt";
        } else if (clazz == NoOpBenchmark.class) {
            return "noop";
        } else if (clazz == ResourceStresserBenchmark.class) {
            return "resourcestresser";
        } else if (clazz == SEATSBenchmark.class) {
            return "seats";
        } else if (clazz == SIBenchmark.class) {
            return "sibench";
        } else if (clazz == SmallBankBenchmark.class) {
            return "smallbank";
        } else if (clazz == TATPBenchmark.class) {
            return "tatp";
        } else if (clazz == TPCCBenchmark.class) {
            return "tpcc";
        } else if (clazz == TPCDSBenchmark.class) {
            return "tpcds";
        } else if (clazz == TPCHBenchmark.class) {
            return "tpch";
        } else if (clazz == TwitterBenchmark.class) {
            return "twitter";
        } else if (clazz == VoterBenchmark.class) {
            return "voter";
        } else if (clazz == WikipediaBenchmark.class) {
            return "wikipedia";
        } else if (clazz == YCSBBenchmark.class) {
            return "ycsb";
        }

        throw new RuntimeException("Sorry, this is a hack. You need to add your new benchmark class here.");
    }

    /**
     * Return the URL handle to the DDL used to load the benchmark's database
     * schema.
     *
     * @param db_type
     */
    public String getDatabaseDDLPath(DatabaseType db_type) {

        // The order matters!
        List<String> names = new ArrayList<>();
        if (db_type != null) {
            names.add("ddl-" + db_type.name().toLowerCase() + ".sql");
        }
        names.add("ddl-generic.sql");

        for (String fileName : names) {
            final String benchmarkName = getBenchmarkName();
            final String path = "benchmarks" + File.separator + benchmarkName + File.separator + fileName;

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

    public final void refreshCatalog() throws SQLException {
        if (this.catalog != null) {
            try {
                this.catalog.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        try (Connection conn = this.makeConnection()) {
            this.catalog = SQLUtil.getCatalog(this, this.getWorkloadConfiguration().getDatabaseType(), conn);
        }
    }

    /**
     * Create the Benchmark Database
     * This is the main method used to create all the database
     * objects (e.g., table, indexes, etc) needed for this benchmark
     */
    public final void createDatabase() throws SQLException, IOException {
        try (Connection conn = this.makeConnection()) {
            this.createDatabase(this.workConf.getDatabaseType(), conn);
        }
    }

    /**
     * Create the Benchmark Database
     * This is the main method used to create all the database
     * objects (e.g., table, indexes, etc) needed for this benchmark
     */
    public final void createDatabase(DatabaseType dbType, Connection conn) throws SQLException, IOException {

            String ddlPath = this.getDatabaseDDLPath(dbType);
            ScriptRunner runner = new ScriptRunner(conn, true, true);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Executing script [{}] for database type [{}]", ddlPath, dbType);
            }

            runner.runScript(ddlPath);

    }


    /**
     * Invoke this benchmark's database loader
     */
    public final Loader<? extends BenchmarkModule> loadDatabase() throws SQLException, InterruptedException {
        Loader<? extends BenchmarkModule> loader;

        loader = this.makeLoaderImpl();
        if (loader != null) {


            try {
                List<LoaderThread> loaderThreads = loader.createLoaderThreads();
                int maxConcurrent = workConf.getLoaderThreads();

                ThreadUtil.runLoaderThreads(loaderThreads, maxConcurrent);

                if (!loader.getTableCounts().isEmpty()) {
                    LOG.debug("Table Counts:\n{}", loader.getTableCounts());
                }
            } finally {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Finished loading the %s database", this.getBenchmarkName().toUpperCase()));
                }
            }
        }

        return loader;
    }

    public final void clearDatabase() throws SQLException {

        try (Connection conn = this.makeConnection()) {
            Loader<? extends BenchmarkModule> loader = this.makeLoaderImpl();
            if (loader != null) {
                conn.setAutoCommit(false);
                loader.unload(conn, this.catalog);
                conn.commit();
            }
        }
    }

    // --------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------

    /**
     * Return the unique identifier for this benchmark
     */
    public final String getBenchmarkName() {
        String workConfName = this.workConf.getBenchmarkName();
        return workConfName != null ? workConfName : convertBenchmarkClassToBenchmarkName();
    }

    /**
     * Return the database's catalog
     */
    public final AbstractCatalog getCatalog() {

        if (catalog == null) {
            throw new RuntimeException("getCatalog() has been called before refreshCatalog()");
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
    @SuppressWarnings("unchecked")
    public final TransactionType initTransactionType(String procName, int id) {
        if (id == TransactionType.INVALID_ID) {
            throw new RuntimeException(String.format("Procedure %s.%s cannot use the reserved id '%d' for %s", getBenchmarkName(), procName, id, TransactionType.INVALID.getClass().getSimpleName()));
        }

        Package pkg = this.getProcedurePackageImpl();

        String fullName = pkg.getName() + "." + procName;
        Class<? extends Procedure> procClass = (Class<? extends Procedure>) ClassUtil.getClass(fullName);

        return new TransactionType(procClass, id, false);
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
                    txn = new TransactionType(procClass, procClass.hashCode(), true);
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
