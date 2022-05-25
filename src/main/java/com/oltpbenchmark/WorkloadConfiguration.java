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


package com.oltpbenchmark;

import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.config.Database;
import com.oltpbenchmark.api.config.TransactionIsolation;
import com.oltpbenchmark.api.config.Workload;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ThreadUtil;

import java.sql.Driver;
import java.util.List;

public class WorkloadConfiguration {

    private final Database database;
    private final Workload workload;
    private final TransactionTypes transactionTypes;
    private final List<Phase> phases;
    private final int loaderThreads = ThreadUtil.availableProcessors();
    private WorkloadState workloadState;


    public WorkloadConfiguration(Database database, Workload workload, TransactionTypes transactionTypes, List<Phase> phases) {
        this.database = database;
        this.workload = workload;
        this.transactionTypes = transactionTypes;
        this.phases = phases;
    }

    public WorkloadState getWorkloadState() {
        return workloadState;
    }

    public DatabaseType getDatabaseType() {
        return database.type();
    }

    public String getUrl() {
        return database.url();
    }

    public String getUsername() {
        return database.username();
    }

    public String getPassword() {
        return database.password();
    }

    public Class<? extends Driver> getDriverClass() {
        return database.driverClass();
    }

    public int getBatchSize() {
        return database.batchSize() != null ? database.batchSize() : 128;
    }

    public int getMaxRetries() {
        return database.retries() != null ? database.retries() : 3;
    }

    /**
     * Initiate a new benchmark and workload state
     */
    public void initializeState(BenchmarkState benchmarkState) {
        this.workloadState = new WorkloadState(benchmarkState, phases, getTerminals());
    }

    /**
     * The number of loader threads that the framework is allowed to use.
     *
     * @return
     */
    public int getLoaderThreads() {
        return this.loaderThreads;
    }


    /**
     * Return the scale factor of the database size
     *
     * @return
     */
    public double getScaleFactor() {
        return workload.scaleFactor() != null ? workload.scaleFactor() : 1;
    }

    public double getSelectivity() {
        return workload.selectivity() != null ? workload.selectivity() : -1;
    }

    /**
     * Return the number of phases specified in the config file
     *
     * @return
     */
    public int getNumberOfPhases() {
        return phases.size();
    }

    public int getTerminals() {
        return workload.terminals();
    }

    public TransactionTypes getTransactionTypes() {
        return transactionTypes;
    }

    public List<Phase> getPhases() {
        return phases;
    }

    public TransactionIsolation getIsolationMode() {
        return database.transactionIsolation();
    }

    public String getTraceFile1() {
        return workload.traceFile1();
    }

    public String getTraceFile2() {
        return workload.traceFile2();
    }

    public Integer getFieldSize() {
        return workload.fieldSize();
    }

}
