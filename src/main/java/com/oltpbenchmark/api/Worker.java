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

import com.oltpbenchmark.*;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Worker<T extends BenchmarkModule> implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    private static final Logger ABORT_LOG = LoggerFactory.getLogger("com.oltpbenchmark.api.ABORT_LOG");

    private WorkloadState state;
    private LatencyRecord latencies;
    private final Statement currStatement;

    // Interval requests used by the monitor
    private final AtomicInteger intervalRequests = new AtomicInteger(0);

    private final int id;
    private final T benchmarkModule;
    protected final WorkloadConfiguration configuration;
    protected final TransactionTypes transactionTypes;
    protected final Map<TransactionType, Procedure> procedures = new HashMap<>();
    protected final Map<String, Procedure> name_procedures = new HashMap<>();
    protected final Map<Class<? extends Procedure>, Procedure> class_procedures = new HashMap<>();

    private final Histogram<TransactionType> txnUnknown = new Histogram<>();
    private final Histogram<TransactionType> txnSuccess = new Histogram<>();
    private final Histogram<TransactionType> txnAbort = new Histogram<>();
    private final Histogram<TransactionType> txnRetry = new Histogram<>();
    private final Histogram<TransactionType> txnErrors = new Histogram<>();
    private final Histogram<TransactionType> txtRetryDifffernt = new Histogram<>();

    private boolean seenDone = false;

    public Worker(T benchmarkModule, int id) {
        this.id = id;
        this.benchmarkModule = benchmarkModule;
        this.configuration = this.benchmarkModule.getWorkloadConfiguration();
        this.state = this.configuration.getWorkloadState();
        this.currStatement = null;
        this.transactionTypes = this.configuration.getTransTypes();

        // Generate all the Procedures that we're going to need
        this.procedures.putAll(this.benchmarkModule.getProcedures());
        for (Entry<TransactionType, Procedure> e : this.procedures.entrySet()) {
            Procedure proc = e.getValue();
            this.name_procedures.put(e.getKey().getName(), proc);
            this.class_procedures.put(proc.getClass(), proc);
        }
    }

    /**
     * Get the BenchmarkModule managing this Worker
     */
    public final T getBenchmarkModule() {
        return (this.benchmarkModule);
    }

    /**
     * Get the unique thread id for this worker
     */
    public final int getId() {
        return this.id;
    }

    @Override
    public String toString() {
        return String.format("%s<%03d>", this.getClass().getSimpleName(), this.getId());
    }

    public final WorkloadConfiguration getWorkloadConfiguration() {
        return (this.benchmarkModule.getWorkloadConfiguration());
    }

    public final Random rng() {
        return (this.benchmarkModule.rng());
    }

    public final int getRequests() {
        return latencies.size();
    }

    public final int getAndResetIntervalRequests() {
        return intervalRequests.getAndSet(0);
    }

    public final Iterable<LatencyRecord.Sample> getLatencyRecords() {
        return latencies;
    }

    public final Procedure getProcedure(TransactionType type) {
        return (this.procedures.get(type));
    }

    @Deprecated
    public final Procedure getProcedure(String name) {
        return (this.name_procedures.get(name));
    }

    public final <P extends Procedure> P getProcedure(Class<P> procClass) {
        return (P) (this.class_procedures.get(procClass));
    }

    public final Histogram<TransactionType> getTransactionSuccessHistogram() {
        return (this.txnSuccess);
    }

    public final Histogram<TransactionType> getTransactionUnknownHistogram() {
        return (this.txnUnknown);
    }

    public final Histogram<TransactionType> getTransactionRetryHistogram() {
        return (this.txnRetry);
    }

    public final Histogram<TransactionType> getTransactionAbortHistogram() {
        return (this.txnAbort);
    }

    public final Histogram<TransactionType> getTransactionErrorHistogram() {
        return (this.txnErrors);
    }
    public final Histogram<TransactionType> getTransactionRetryDifferentHistogram() {
        return (this.txtRetryDifffernt);
    }

    /**
     * Stop executing the current statement.
     */
    synchronized public void cancelStatement() {
        try {
            if (this.currStatement != null) {
                this.currStatement.cancel();
            }
        } catch (SQLException e) {
            LOG.error("Failed to cancel statement: {}", e.getMessage());
        }
    }

    @Override
    public final void run() {
        Thread t = Thread.currentThread();
        SubmittedProcedure pieceOfWork;
        t.setName(this.toString());

        // In case of reuse reset the measurements
        latencies = new LatencyRecord(state.getTestStartNs());

        // Invoke the initialize callback
        try {
            this.initialize();
        } catch (Throwable ex) {
            throw new RuntimeException("Unexpected error when initializing " + this, ex);
        }

        // wait for start
        state.blockForStart();
        State preState, postState;
        Phase phase;

        TransactionType invalidTT = TransactionType.INVALID;

        work:
        while (true) {

            // PART 1: Init and check if done

            preState = state.getGlobalState();
            phase = this.state.getCurrentPhase();

            switch (preState) {
                case DONE:
                    if (!seenDone) {
                        // This is the first time we have observed that the
                        // test is done notify the global test state, then
                        // continue applying load
                        seenDone = true;
                        state.signalDone();
                        break work;
                    }
                    break;
                default:
                    // Do nothing
            }

            // PART 2: Wait for work

            // Sleep if there's nothing to do.
            state.stayAwake();
            phase = this.state.getCurrentPhase();
            if (phase == null) {
                continue work;
            }

            // Grab some work and update the state, in case it changed while we
            // waited.
            pieceOfWork = state.fetchWork();
            preState = state.getGlobalState();

            phase = this.state.getCurrentPhase();
            if (phase == null) {
                continue work;
            }

            switch (preState) {
                case DONE:
                case EXIT:
                case LATENCY_COMPLETE:
                    // Once a latency run is complete, we wait until the next
                    // phase or until DONE.
                    continue work;
                default:
                    // Do nothing
            }

            // PART 3: Execute work

            // TODO: Measuring latency when not rate limited is ... a little
            // weird because if you add more simultaneous clients, you will
            // increase latency (queue delay) but we do this anyway since it is
            // useful sometimes

            long start = pieceOfWork.getStartTime();

            TransactionType type = invalidTT;
            try {
                type = doWork(pieceOfWork);
            } catch (IndexOutOfBoundsException e) {
                if (phase.isThroughputRun()) {
                    LOG.error("Thread tried executing disabled phase!");
                    throw e;
                }
                if (phase.getId() == this.state.getCurrentPhase().getId()) {
                    switch (preState) {
                        case WARMUP:
                            // Don't quit yet: we haven't even begun!
                            phase.resetSerial();
                            break;
                        case COLD_QUERY:
                        case MEASURE:
                            // The serial phase is over. Finish the run early.
                            state.signalLatencyComplete();
                            LOG.info("[Serial] Serial execution of all" + " transactions complete.");
                            break;
                        default:
                            throw e;
                    }
                }
            }

            // PART 4: Record results

            long end = System.nanoTime();
            postState = state.getGlobalState();

            switch (postState) {
                case MEASURE:
                    // Non-serial measurement. Only measure if the state both
                    // before and after was MEASURE, and the phase hasn't
                    // changed, otherwise we're recording results for a query
                    // that either started during the warmup phase or ended
                    // after the timer went off.
                    if (preState == State.MEASURE && type != null && this.state.getCurrentPhase().getId() == phase.getId()) {
                        latencies.addLatency(type.getId(), start, end, this.id, phase.getId());
                        intervalRequests.incrementAndGet();
                    }
                    if (phase.isLatencyRun()) {
                        this.state.startColdQuery();
                    }
                    break;
                case COLD_QUERY:
                    // No recording for cold runs, but next time we will since
                    // it'll be a hot run.
                    if (preState == State.COLD_QUERY) {
                        this.state.startHotQuery();
                    }
                    break;
                default:
                    // Do nothing
            }

            state.finishedWork();
        }

        LOG.debug("worker calling teardown");

        tearDown(false);
    }

    /**
     * Called in a loop in the thread to exercise the system under test. Each
     * implementing worker should return the TransactionType handle that was
     * executed.
     *
     * @param pieceOfWork
     */
    protected final TransactionType doWork(SubmittedProcedure pieceOfWork) {

        final DatabaseType type = configuration.getDatabaseType();
        final TransactionType transactionType = transactionTypes.getType(pieceOfWork.getType());

        final int isolationMode = this.configuration.getIsolationMode();

        if (LOG.isDebugEnabled()) {
            LOG.debug("desired transaction isolation mode = {}", isolationMode);
        }
        try (Connection conn = benchmarkModule.getConnection()) {


            if (!conn.getAutoCommit()) {
                LOG.warn("autocommit is already false at beginning of work.  this is a problem");
            }

            conn.setAutoCommit(false);
            conn.setTransactionIsolation(isolationMode);

            int retryCount = 0;

            int maxRetryCount = configuration.getMaxRetries();

            while (retryCount < maxRetryCount && this.state.getGlobalState() != State.DONE) {

                TransactionStatus status = TransactionStatus.UNKNOWN;

                try {

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("%s %s attempting...", this, transactionType));
                    }

                    status = this.executeWork(conn, transactionType);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("%s %s completed with status [%s]...", this, transactionType, status.name()));
                    }

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("%s %s committing...", this, transactionType));
                    }

                    conn.commit();

                    break;

                } catch (UserAbortException ex) {
                    conn.rollback();

                    ABORT_LOG.debug(String.format("%s Aborted", transactionType), ex);

                    status = TransactionStatus.USER_ABORTED;

                    break;

                } catch (SQLException ex) {
                    conn.rollback();

                    if (isRetryable(ex)) {
                        LOG.warn(String.format("Retryable SQLException occurred during [%s]... current retry attempt [%d], max retry attempts [%d], sql state [%s], error code [%d].", transactionType, retryCount, maxRetryCount, ex.getSQLState(), ex.getErrorCode()), ex);

                        status = TransactionStatus.RETRY;

                        retryCount++;
                    } else {
                        LOG.warn(String.format("SQLException occurred during [%s] and will not be retried... sql state [%s], error code [%d].", transactionType, ex.getSQLState(), ex.getErrorCode()), ex);

                        status = TransactionStatus.ERROR;
                    }

                } finally {

                    switch (status) {

                        case UNKNOWN:
                            this.txnUnknown.put(transactionType);
                            break;
                        case SUCCESS:
                            this.txnSuccess.put(transactionType);
                            break;
                        case USER_ABORTED:
                            this.txnAbort.put(transactionType);
                            break;
                        case RETRY:
                            this.txnRetry.put(transactionType);
                            break;
                        case RETRY_DIFFERENT:
                            this.txtRetryDifffernt.put(transactionType);
                            break;
                        case ERROR:
                            this.txnErrors.put(transactionType);
                            break;
                    }

                }

            }

            if (conn.getAutoCommit()) {
                LOG.warn("autocommit is already true at end of work.  this is a problem");
            }

            conn.setAutoCommit(true);

        } catch (SQLException ex) {
            String msg = String.format("Unexpected SQLException in '%s' when executing '%s' on [%s]", this, transactionType, type.name());

            throw new RuntimeException(msg, ex);
        }

        return (transactionType);
    }

    private boolean isRetryable(SQLException ex) {

        String sqlState = ex.getSQLState();
        int errorCode = ex.getErrorCode();

        LOG.debug("sql state [{}] and error code [{}]", sqlState, errorCode);

        if (sqlState == null) {
            return true;
        }

        // ------------------
        // MYSQL: https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-error-sqlstates.html
        // ------------------
        if (errorCode == 1213 && sqlState.equals("40001")) {
            // MySQL ER_LOCK_DEADLOCK
            return true;
        } else if (errorCode == 1205 && sqlState.equals("41000")) {
            // MySQL ER_LOCK_WAIT_TIMEOUT
            return true;
        }

        // ------------------
        // POSTGRES: https://www.postgresql.org/docs/current/errcodes-appendix.html
        // ------------------
        if (errorCode == 0 && sqlState.equals("40001")) {
            // Postgres serialization_failure
            return true;
        }

        return false;
    }

    /**
     * Optional callback that can be used to initialize the Worker right before
     * the benchmark execution begins
     */
    protected void initialize() {
        // The default is to do nothing
    }

    /**
     * Invoke a single transaction for the given TransactionType
     *
     * @param conn
     * @param txnType
     * @return TODO
     * @throws UserAbortException TODO
     * @throws SQLException       TODO
     */
    protected abstract TransactionStatus executeWork(Connection conn, TransactionType txnType) throws UserAbortException, SQLException;

    /**
     * Called at the end of the test to do any clean up that may be required.
     *
     * @param error TODO
     */
    public void tearDown(boolean error) {

    }

    public void initializeState() {
        this.state = this.configuration.getWorkloadState();
    }
}
