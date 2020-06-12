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
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ThreadUtil;
import org.apache.commons.configuration2.XMLConfiguration;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class WorkloadConfiguration {

    private final List<Phase> works = new ArrayList<>();
    private DatabaseType db_type;
    private String benchmarkName;
    private String db_connection;
    private String db_username;
    private String db_password;
    private String db_driver;
    private int db_pool_size;
    private int db_batch_size;
    private double scaleFactor = 1.0;
    private double selectivity = -1.0;
    private int terminals;
    private int loaderThreads = ThreadUtil.availableProcessors();
    private int numTxnTypes;
    private TraceReader traceReader = null;
    private XMLConfiguration xmlConfig = null;
    private WorkloadState workloadState;
    private int numberOfPhases = 0;
    private TransactionTypes transTypes = null;
    private int isolationMode = Connection.TRANSACTION_SERIALIZABLE;
    private String dataDir = null;

    public String getBenchmarkName() {
        return benchmarkName;
    }

    public void setBenchmarkName(String benchmarkName) {
        this.benchmarkName = benchmarkName;
    }

    public TraceReader getTraceReader() {
        return traceReader;
    }

    public void setTraceReader(TraceReader traceReader) {
        this.traceReader = traceReader;
    }

    public WorkloadState getWorkloadState() {
        return workloadState;
    }

    /**
     * Initiate a new benchmark and workload state
     */
    public WorkloadState initializeState(BenchmarkState benchmarkState) {

        workloadState = new WorkloadState(benchmarkState, works, terminals, traceReader);
        return workloadState;
    }

    public void addWork(int time, int warmup, int rate, List<Double> weights, boolean rateLimited, boolean disabled, boolean serial, boolean timed, int active_terminals, Phase.Arrival arrival) {
        works.add(new Phase(benchmarkName, numberOfPhases, time, warmup, rate, weights, rateLimited, disabled, serial, timed, active_terminals, arrival));
        numberOfPhases++;
    }

    public int getDBBatchSize() {
        return db_batch_size;
    }

    public void setDBBatchSize(int db_batch_size) {
        this.db_batch_size = db_batch_size;
    }

    public DatabaseType getDBType() {
        return db_type;
    }

    public void setDBType(DatabaseType dbType) {
        db_type = dbType;
    }

    public String getDBConnection() {
        return db_connection;
    }

    public void setDBConnection(String database) {
        this.db_connection = database;
    }

    /**
     * The number of loader threads that the framework is allowed to use.
     *
     * @return
     */
    public int getLoaderThreads() {
        return this.loaderThreads;
    }

    public void setLoaderThreads(int loaderThreads) {
        this.loaderThreads = loaderThreads;
    }

    public int getNumTxnTypes() {
        return numTxnTypes;
    }

    public void setNumTxnTypes(int numTxnTypes) {
        this.numTxnTypes = numTxnTypes;
    }

    public String getDBUsername() {
        return db_username;
    }

    public void setDBUsername(String username) {
        this.db_username = username;
    }

    public String getDBPassword() {
        return this.db_password;
    }

    public void setDBPassword(String password) {
        this.db_password = password;
    }

    public double getSelectivity() {
        return this.selectivity;
    }

    public void setSelectivity(double selectivity) {
        this.selectivity = selectivity;
    }

    public String getDBDriver() {
        return this.db_driver;
    }

    public void setDBDriver(String driver) {
        this.db_driver = driver;
    }

    public int getDBPoolSize() {
        return this.db_pool_size;
    }

    public void setDBPoolSize(int poolSize) {
        this.db_pool_size = poolSize;
    }

    /**
     * Return the scale factor of the database size
     *
     * @return
     */
    public double getScaleFactor() {
        return this.scaleFactor;
    }

    /**
     * Set the scale factor for the database
     * A value of 1 means the default size.
     * A value greater than 1 means the database is larger
     * A value less than 1 means the database is smaller
     *
     * @param scaleFactor
     */
    public void setScaleFactor(double scaleFactor) {
        this.scaleFactor = scaleFactor;
    }

    /**
     * Return the number of phases specified in the config file
     *
     * @return
     */
    public int getNumberOfPhases() {
        return this.numberOfPhases;
    }

    /**
     * Return the directory in which we can find the data files (for example, CSV
     * files) for loading the database.
     */
    public String getDataDir() {
        return this.dataDir;
    }

    /**
     * Set the directory in which we can find the data files (for example, CSV
     * files) for loading the database.
     */
    public void setDataDir(String dir) {
        this.dataDir = dir;
    }

    /**
     * A utility method that init the phaseIterator and dialectMap
     */
    public void init() {
        try {
            Class.forName(this.db_driver);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException("Failed to initialize JDBC driver '" + this.db_driver + "'", ex);
        }
    }

    public int getTerminals() {
        return terminals;
    }

    public void setTerminals(int terminals) {
        this.terminals = terminals;
    }

    public TransactionTypes getTransTypes() {
        return transTypes;
    }

    public void setTransTypes(TransactionTypes transTypes) {
        this.transTypes = transTypes;
    }

    public List<Phase> getAllPhases() {
        return works;
    }

    public XMLConfiguration getXmlConfig() {
        return xmlConfig;
    }

    public void setXmlConfig(XMLConfiguration xmlConfig) {
        this.xmlConfig = xmlConfig;
    }

    public int getIsolationMode() {
        return isolationMode;
    }

    public void setIsolationMode(String mode) {
        switch (mode) {
            case "TRANSACTION_SERIALIZABLE":
                this.isolationMode = Connection.TRANSACTION_SERIALIZABLE;
                break;
            case "TRANSACTION_READ_COMMITTED":
                this.isolationMode = Connection.TRANSACTION_READ_COMMITTED;
                break;
            case "TRANSACTION_REPEATABLE_READ":
                this.isolationMode = Connection.TRANSACTION_REPEATABLE_READ;
                break;
            case "TRANSACTION_READ_UNCOMMITTED":
                this.isolationMode = Connection.TRANSACTION_READ_UNCOMMITTED;
                break;
        }
    }

    public String getIsolationString() {
        if (this.isolationMode == Connection.TRANSACTION_SERIALIZABLE) {
            return "TRANSACTION_SERIALIZABLE";
        } else if (this.isolationMode == Connection.TRANSACTION_READ_COMMITTED) {
            return "TRANSACTION_READ_COMMITTED";
        } else if (this.isolationMode == Connection.TRANSACTION_REPEATABLE_READ) {
            return "TRANSACTION_REPEATABLE_READ";
        } else if (this.isolationMode == Connection.TRANSACTION_READ_UNCOMMITTED) {
            return "TRANSACTION_READ_UNCOMMITTED";
        } else {
            return "TRANSACTION_SERIALIZABLE";
        }
    }

    @Override
    public String toString() {
        return "WorkloadConfiguration{" +
                "works=" + works +
                ", db_type=" + db_type +
                ", benchmarkName='" + benchmarkName + '\'' +
                ", db_connection='" + db_connection + '\'' +
                ", db_username='" + db_username + '\'' +
                ", db_password='" + db_password + '\'' +
                ", db_driver='" + db_driver + '\'' +
                ", db_pool_size=" + db_pool_size +
                ", db_batch_size=" + db_batch_size +
                ", scaleFactor=" + scaleFactor +
                ", selectivity=" + selectivity +
                ", terminals=" + terminals +
                ", loaderThreads=" + loaderThreads +
                ", numTxnTypes=" + numTxnTypes +
                ", traceReader=" + traceReader +
                ", xmlConfig=" + xmlConfig +
                ", workloadState=" + workloadState +
                ", numberOfPhases=" + numberOfPhases +
                ", transTypes=" + transTypes +
                ", isolationMode=" + isolationMode +
                ", dataDir='" + dataDir + '\'' +
                '}';
    }
}
