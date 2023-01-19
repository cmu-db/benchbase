package com.oltpbenchmark.api.collectors.monitoring;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import com.oltpbenchmark.BenchmarkState;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.api.collectors.monitoring.proto.PerfEventLog;
import com.oltpbenchmark.api.collectors.monitoring.proto.QueryEventLog;
import com.oltpbenchmark.api.collectors.monitoring.proto.QueryInfoLog;
import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.SQLUtil;

import org.apache.commons.lang3.StringUtils;

public abstract class DatabaseMonitor extends Monitor {
    protected enum DatabaseState {
        READY, INVALID, TEST
    };

    protected DatabaseState currentState = DatabaseState.INVALID;
    protected String OUTPUT_DIR = "results/monitor";
    protected String CSV_DELIMITER = ",";
    protected int fileCounter = 1;
    protected final int fileTimeDiff;

    protected WorkloadConfiguration conf;
    protected Connection conn;
    protected final QueryEventLog.Builder queryEventLogBuilder;
    protected final PerfEventLog.Builder perfEventLogBuilder;
    protected final QueryInfoLog.Builder queryInfoLogBuilder;

    /**
     * Builds the connection to the DBMS using the same connection details as
     * the benchmarking environment.
     *
     * @param conf
     * @return
     * @throws SQLException
     */
    private final Connection makeConnection() throws SQLException {
        if (StringUtils.isEmpty(conf.getUsername())) {
            return DriverManager.getConnection(conf.getUrl());
        } else {
            return DriverManager.getConnection(
                    conf.getUrl(),
                    conf.getUsername(),
                    conf.getPassword());
        }
    }

    /**
     * Convenience function to try and reconnect if the connection had failed.
     *
     * Note: since these are read-only queries, in some clustered environments,
     * its possible that we make a connection to stale primary if there's a race
     * between a failover event and the connection.
     * In that case, we may be reading the metrics from the wrong server.
     * There isn't a great solution to this without periodically disconnecting,
     * which can cause it's own overheads.
     */
    protected void checkForReconnect() {
        try {
            if (!this.conf.getNewConnectionPerTxn() && this.conn != null) {
                this.conn.close();
                this.conn = null;
            }
            else if (conn != null && conn.isClosed()) {
                conn = null;
            }
        }
        catch (SQLException sqlError) {
            LOG.error(sqlError.getMessage(), sqlError);
            conn = null;
        }

        if (conn == null) {
            try {
                conn = makeConnection();
            }
            catch (SQLException sqlError) {
                LOG.error(sqlError.getMessage(), sqlError);
                conn = null;
            }
        }
    }

    protected void handleSQLConnectionException(SQLException sqlError) {
        if (this.conf.getReconnectOnConnectionFailure() && SQLUtil.isConnectionErrorException(sqlError)) {
            // reset the connection to null so the checkForReconnect() call can manage it
            this.conn = null;
        }
    }


    /**
     * Constructor, set all final parameters and initialize connection.
     *
     * @param interval
     * @param testState
     * @param workers
     * @param workloadConf
     */
    public DatabaseMonitor(
            int interval, BenchmarkState testState,
            List<? extends Worker<? extends BenchmarkModule>> workers, WorkloadConfiguration workloadConf) {
        super(interval, testState, workers);

        try {
            this.conf = workloadConf;
            this.conn = makeConnection();
        } catch (SQLException e) {
            this.conn = null;
            LOG.error("Could not initialize connection to create DatabaseMonitor.");
            LOG.error(e.getMessage());
        }

        FileUtil.makeDirIfNotExists(OUTPUT_DIR);
        // Write to file every ~10mins as back-up.
        fileTimeDiff = 600000 / this.intervalMonitor;

        // Init output proto builders.
        this.queryEventLogBuilder = QueryEventLog.newBuilder();
        this.perfEventLogBuilder = PerfEventLog.newBuilder();
        this.queryInfoLogBuilder = QueryInfoLog.newBuilder();

        LOG.info("Initialized DatabaseMonitor.");
    }

    /**
     * Util to write query event log to file.
     */
    protected void writeQueryEventLog() {
        String filePath = FileUtil.joinPath(
                OUTPUT_DIR, "query_event_log_" + fileCounter + ".proto");

        try {
            if (Files.deleteIfExists(Paths.get(filePath))) {
                LOG.warn("File at " + filePath + " deleted before writing query event log.");
            }

            FileOutputStream out = new FileOutputStream(filePath);
            queryEventLogBuilder.build().writeTo(out);
            out.close();
            LOG.info("Query event log written to " + filePath);
        } catch (IOException e) {
            LOG.error("Error when writing query event log to file.");
            LOG.error(e.getMessage());
        }
    }

    /**
     * Util to write performance event log to file.
     */
    protected void writePerfEventLog() {
        String filePath = FileUtil.joinPath(
                OUTPUT_DIR, "perf_event_log_" + fileCounter + ".proto");

        try {
            if (Files.deleteIfExists(Paths.get(filePath))) {
                LOG.warn("File at " + filePath + " deleted before writing perf event log.");
            }

            FileOutputStream out = new FileOutputStream(filePath);
            perfEventLogBuilder.build().writeTo(out);
            out.close();
            LOG.info("Perf event log written to " + filePath);
        } catch (IOException e) {
            LOG.error("Error when writing perf event log to file.");
            LOG.error(e.getMessage());
        }
    }

    /**
     * Util to write query info log to file.
     */
    protected void writeQueryInfoLog() {
        String filePath = FileUtil.joinPath(
                OUTPUT_DIR, "query_info_log_" + fileCounter + ".proto");

        try {
            if (Files.deleteIfExists(Paths.get(filePath))) {
                LOG.warn("File at " + filePath + " deleted before writing query info log.");
            }

            FileOutputStream out = new FileOutputStream(filePath);
            queryInfoLogBuilder.build().writeTo(out);
            out.close();
            LOG.info("Query info log written to " + filePath);
        } catch (IOException e) {
            LOG.error("Error when writing query info log to file.");
            LOG.error(e.getMessage());
        }
    }

    /**
     * DBMS-specific cache cleaning process.
     */
    protected abstract void cleanupCache();

    /**
     * Execute the extraction of desired query and performance metrics.
     */
    protected abstract void runExtraction();

    /**
     * Write proto builders to file.
     */
    protected abstract void writeLogs();

    /**
     * Consolidate protos that have been written to file and currently pending.
     * Write to a common output file.
     */
    protected abstract void finalizeLogs();

    /**
     * Consolidate protos that have been written to file and currently pending.
     * Write to a common output file.
     */
    protected abstract void writeToCSV();

    /**
     * Run monitor. Clean up cache first and do initial extraction, then sleep
     * as defined by the interval. Per periodic waking phase, extract metrics
     * and potentially write to file (currently every ~10mins by default). After
     * execution has finished, consolidate logs and clean up cache again.
     */
    @Override
    public void run() {
        LOG.info("Starting Monitor Interval [{}ms]", this.intervalMonitor);

        // Make sure we record one event during setup.
        if (this.conn != null) {
            cleanupCache();
            runExtraction();
        }
        // Periodically extract sys table stats.
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(this.intervalMonitor);
            } catch (InterruptedException ex) {
                // Restore interrupt flag.
                Thread.currentThread().interrupt();
            }
            if (this.conn != null) {
                runExtraction();
            }
            if (fileCounter % fileTimeDiff == 0) {
                writeLogs();
            }
            fileCounter++;
        }

        if (this.conn != null) {
            cleanupCache();
        }

        finalizeLogs();
    }

    /**
     * Called at the end of the test to do any clean up that may be required.
     */
    @Override
    public void tearDown() {
        if (this.conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOG.error("Connection couldn't be closed.", e);
            }
            this.conn = null;
        }
    }
}
