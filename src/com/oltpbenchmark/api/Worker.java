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

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.oltpbenchmark.LatencyRecord;
import com.oltpbenchmark.Phase;
import com.oltpbenchmark.SubmittedProcedure;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.WorkloadState;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.catalog.Catalog;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.StringUtil;

public abstract class Worker<T extends BenchmarkModule> implements Runnable {
    private static final Logger LOG = Logger.getLogger(Worker.class);

    private WorkloadState wrkldState;
    private LatencyRecord latencies;
    private Statement currStatement;

    // Interval requests used by the monitor
    private AtomicInteger intervalRequests = new AtomicInteger(0);

    private final int id;
    private final T benchmarkModule;
    protected final Connection conn;
    protected final WorkloadConfiguration wrkld;
    protected final TransactionTypes transactionTypes;
    protected final Map<TransactionType, Procedure> procedures = new HashMap<TransactionType, Procedure>();
    protected final Map<String, Procedure> name_procedures = new HashMap<String, Procedure>();
    protected final Map<Class<? extends Procedure>, Procedure> class_procedures = new HashMap<Class<? extends Procedure>, Procedure>();

    private final Histogram<TransactionType> txnSuccess = new Histogram<TransactionType>();
    private final Histogram<TransactionType> txnAbort = new Histogram<TransactionType>();
    private final Histogram<TransactionType> txnRetry = new Histogram<TransactionType>();
    private final Histogram<TransactionType> txnErrors = new Histogram<TransactionType>();
    private final Map<TransactionType, Histogram<String>> txnAbortMessages = new HashMap<TransactionType, Histogram<String>>();

    private boolean seenDone = false;

    public Worker(T benchmarkModule, int id) {
        this.id = id;
        this.benchmarkModule = benchmarkModule;
        this.wrkld = this.benchmarkModule.getWorkloadConfiguration();
        this.wrkldState = this.wrkld.getWorkloadState();
        this.currStatement = null;
        this.transactionTypes = this.wrkld.getTransTypes();
        assert (this.transactionTypes != null) : "The TransactionTypes from the WorkloadConfiguration is null!";

        try {
            this.conn = this.benchmarkModule.makeConnection();
            this.conn.setAutoCommit(false);
            
            // 2018-01-11: Since we want to support NoSQL systems 
            // that do not support txns, we will not invoke certain JDBC functions
            // that may cause an error in them.
            if (this.wrkld.getDBType().shouldUseTransactions()) {
                this.conn.setTransactionIsolation(this.wrkld.getIsolationMode());
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to connect to database", ex);
        }

        // Generate all the Procedures that we're going to need
        this.procedures.putAll(this.benchmarkModule.getProcedures());
        assert (this.procedures.size() == this.transactionTypes.size()) : String.format("Failed to get all of the Procedures for %s [expected=%d, actual=%d]", this.benchmarkModule.getBenchmarkName(),
                this.transactionTypes.size(), this.procedures.size());
        for (Entry<TransactionType, Procedure> e : this.procedures.entrySet()) {
            Procedure proc = e.getValue();
            this.name_procedures.put(e.getKey().getName(), proc);
            this.class_procedures.put(proc.getClass(), proc);
            // e.getValue().generateAllPreparedStatements(this.conn);
        } // FOR
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
        return String.format("%s<%03d>",
                             this.getClass().getSimpleName(), this.getId());
    }
    
    /**
     * Get the the total number of workers in this benchmark invocation
     */
    public final int getNumWorkers() {
        return (this.benchmarkModule.getWorkloadConfiguration().getTerminals());
    }

    public final WorkloadConfiguration getWorkloadConfiguration() {
        return (this.benchmarkModule.getWorkloadConfiguration());
    }

    public final Catalog getCatalog() {
        return (this.benchmarkModule.getCatalog());
    }

    public final Random rng() {
        return (this.benchmarkModule.rng());
    }

    public final Connection getConnection() {
        return (this.conn);
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

    @SuppressWarnings("unchecked")
    public final <P extends Procedure> P getProcedure(Class<P> procClass) {
        return (P) (this.class_procedures.get(procClass));
    }

    public final Histogram<TransactionType> getTransactionSuccessHistogram() {
        return (this.txnSuccess);
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

    public final Map<TransactionType, Histogram<String>> getTransactionAbortMessageHistogram() {
        return (this.txnAbortMessages);
    }

    synchronized public void setCurrStatement(Statement s) {
        this.currStatement = s;
    }

    /**
     * Stop executing the current statement.
     */
    synchronized public void cancelStatement() {
        try {
            if (this.currStatement != null)
                this.currStatement.cancel();
        } catch (SQLException e) {
            LOG.error("Failed to cancel statement: " + e.getMessage());
        }
    }

    @Override
    public final void run() {
        Thread t = Thread.currentThread();
        SubmittedProcedure pieceOfWork;
        t.setName(this.toString());

        // In case of reuse reset the measurements
        latencies = new LatencyRecord(wrkldState.getTestStartNs());

        // Invoke the initialize callback
        try {
            this.initialize();
        } catch (Throwable ex) {
            throw new RuntimeException("Unexpected error when initializing " + this, ex);
        }

        // wait for start
        wrkldState.blockForStart();
        State preState, postState;
        Phase phase;

        TransactionType invalidTT = TransactionType.INVALID;
        assert (invalidTT != null);

        work: while (true) {

            // PART 1: Init and check if done

            preState = wrkldState.getGlobalState();
            phase = this.wrkldState.getCurrentPhase();

            switch (preState) {
                case DONE:
                    if (!seenDone) {
                        // This is the first time we have observed that the
                        // test is done notify the global test state, then
                        // continue applying load
                        seenDone = true;
                        wrkldState.signalDone();
                        break work;
                    }
                    break;
                default:
                    // Do nothing
            }

            // PART 2: Wait for work

            // Sleep if there's nothing to do.
            wrkldState.stayAwake();
            phase = this.wrkldState.getCurrentPhase();
            if (phase == null)
                continue work;

            // Grab some work and update the state, in case it changed while we
            // waited.
            pieceOfWork = wrkldState.fetchWork();
            preState = wrkldState.getGlobalState();

            phase = this.wrkldState.getCurrentPhase();
            if (phase == null)
                continue work;

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
                type = doWork(preState == State.MEASURE, pieceOfWork);
            } catch (IndexOutOfBoundsException e) {
                if (phase.isThroughputRun()) {
                    LOG.error("Thread tried executing disabled phase!");
                    throw e;
                }
                if (phase.id == this.wrkldState.getCurrentPhase().id) {
                    switch (preState) {
                        case WARMUP:
                            // Don't quit yet: we haven't even begun!
                            phase.resetSerial();
                            break;
                        case COLD_QUERY:
                        case MEASURE:
                            // The serial phase is over. Finish the run early.
                            wrkldState.signalLatencyComplete();
                            LOG.info("[Serial] Serial execution of all" + " transactions complete.");
                            break;
                        default:
                            throw e;
                    }
                }
            }

            // PART 4: Record results

            long end = System.nanoTime();
            postState = wrkldState.getGlobalState();

            switch (postState) {
                case MEASURE:
                    // Non-serial measurement. Only measure if the state both
                    // before and after was MEASURE, and the phase hasn't
                    // changed, otherwise we're recording results for a query
                    // that either started during the warmup phase or ended
                    // after the timer went off.
                    if (preState == State.MEASURE && type != null && this.wrkldState.getCurrentPhase().id == phase.id) {
                        latencies.addLatency(type.getId(), start, end, this.id, phase.id);
                        intervalRequests.incrementAndGet();
                    }
                    if (phase.isLatencyRun())
                        this.wrkldState.startColdQuery();
                    break;
                case COLD_QUERY:
                    // No recording for cold runs, but next time we will since
                    // it'll be a hot run.
                    if (preState == State.COLD_QUERY)
                        this.wrkldState.startHotQuery();
                    break;
                default:
                    // Do nothing
            }

            wrkldState.finishedWork();
        }

        tearDown(false);
    }

    /**
     * Called in a loop in the thread to exercise the system under test. Each
     * implementing worker should return the TransactionType handle that was
     * executed.
     * 
     * @param llr
     */
    protected final TransactionType doWork(boolean measure, SubmittedProcedure pieceOfWork) {
        TransactionType next = null;
        TransactionStatus status = TransactionStatus.RETRY;
        Savepoint savepoint = null;
        final DatabaseType dbType = wrkld.getDBType();
        final boolean recordAbortMessages = wrkld.getRecordAbortMessages();

        try {
            while (status == TransactionStatus.RETRY && this.wrkldState.getGlobalState() != State.DONE) {
                if (next == null) {
                    next = transactionTypes.getType(pieceOfWork.getType());
                }
                assert (next.isSupplemental() == false) : "Trying to select a supplemental transaction " + next;

                try {
                    // For Postgres, we have to create a savepoint in order
                    // to rollback a user aborted transaction
                    // if (dbType == DatabaseType.POSTGRES) {
                    // savepoint = this.conn.setSavepoint();
                    // // if (LOG.isDebugEnabled())
                    // LOG.info("Created SavePoint: " + savepoint);
                    // }

                    status = TransactionStatus.UNKNOWN;
                    status = this.executeWork(next);

                // User Abort Handling
                // These are not errors
                } catch (UserAbortException ex) {
                    if (LOG.isDebugEnabled())
                        LOG.trace(next + " Aborted", ex);

                    /* PAVLO */
                    if (recordAbortMessages) {
                        Histogram<String> error_h = this.txnAbortMessages.get(next);
                        if (error_h == null) {
                            error_h = new Histogram<String>();
                            this.txnAbortMessages.put(next, error_h);
                        }
                        error_h.put(StringUtil.abbrv(ex.getMessage(), 20));
                    }

                    if (savepoint != null) {
                        this.conn.rollback(savepoint);
                    } else {
                        this.conn.rollback();
                    }
                    
                    status = TransactionStatus.USER_ABORTED;
                    break;

                // Database System Specific Exception Handling
                } catch (SQLException ex) {
                    // TODO: Handle acceptable error codes for every DBMS
                     if (LOG.isDebugEnabled())
                        LOG.warn(String.format("%s thrown when executing '%s' on '%s' " +
                                               "[Message='%s', ErrorCode='%d', SQLState='%s']", 
                                               ex.getClass().getSimpleName(), next, this.toString(),
                                               ex.getMessage(), ex.getErrorCode(), ex.getSQLState()), ex);

                    this.txnErrors.put(next);

		    if (this.wrkld.getDBType().shouldUseTransactions()) {
			if (savepoint != null) {
			    this.conn.rollback(savepoint);
			} else {
			    this.conn.rollback();
			}
		    }

                    if (ex.getSQLState() == null) {
                        continue;
                    // ------------------
                    // MYSQL
                    // ------------------
                    } else if (ex.getErrorCode() == 1213 && ex.getSQLState().equals("40001")) {
                        // MySQLTransactionRollbackException
                        continue;
                    } else if (ex.getErrorCode() == 1205 && ex.getSQLState().equals("41000")) {
                        // MySQL Lock timeout
                        continue;
                        
                    // ------------------
                    // SQL SERVER
                    // ------------------
                    } else if (ex.getErrorCode() == 1205 && ex.getSQLState().equals("40001")) {
                        // SQLServerException Deadlock
                        continue;
                    
                    // ------------------
                    // POSTGRES
                    // ------------------
                    } else if (ex.getErrorCode() == 0 && ex.getSQLState() != null && ex.getSQLState().equals("40001")) {
                        // Postgres serialization
                        continue;
                    } else if (ex.getErrorCode() == 0 && ex.getSQLState() != null && ex.getSQLState().equals("53200")) {
                        // Postgres OOM error
                        throw ex;
                        
                    // ------------------
                    // ORACLE
                    // ------------------
                    } else if (ex.getErrorCode() == 8177 && ex.getSQLState().equals("72000")) {
                        // ORA-08177: Oracle Serialization
                        continue;
                        
                    // ------------------
                    // DB2
                    // ------------------
                    } else if (ex.getErrorCode() == -911 && ex.getSQLState().equals("40001")) {
                        // DB2Exception Deadlock
                        continue;
                    } else if ((ex.getErrorCode() == 0 && ex.getSQLState().equals("57014")) ||
                               (ex.getErrorCode() == -952 && ex.getSQLState().equals("57014"))) {
                        // Query cancelled by benchmark because we changed
                        // state. That's fine! We expected/caused this.
                        status = TransactionStatus.RETRY_DIFFERENT;
                        continue;
                    } else if (ex.getErrorCode() == 0 && ex.getSQLState().equals("02000")) {
                        // No results returned. That's okay, we can proceed to
                        // a different query. But we should send out a warning,
                        // too, since this is unusual.
                        status = TransactionStatus.RETRY_DIFFERENT;
                        continue;

                    // ------------------
                    // UNKNOWN!
                    // ------------------
                    } else {
                        // UNKNOWN: In this case .. Retry as well!
                        LOG.warn("The DBMS rejected the transaction without an error code", ex);
                        continue;
                        // FIXME Disable this for now
                        // throw ex;
                    }
                // Assertion Error
                } catch (Error ex) {
                    LOG.error("Fatal error when invoking " + next, ex);
                    throw ex;
                 // Random Error
                } catch (Exception ex) {
                    LOG.error("Fatal error when invoking " + next, ex);
                    throw new RuntimeException(ex);
                    
                } finally {
                     if (LOG.isDebugEnabled())
                        LOG.debug(String.format("%s %s Result: %s", this, next, status));
                    
                    switch (status) {
                        case SUCCESS:
                            this.txnSuccess.put(next);
                            break;
                        case RETRY_DIFFERENT:
                            this.txnRetry.put(next);
                            return null;
                        case USER_ABORTED:
                            this.txnAbort.put(next);
                            break;
                        case RETRY:
                            continue;
                        default:
                            assert (false) : String.format("Unexpected status '%s' for %s", status, next);
                    } // SWITCH
                }

            } // WHILE
        } catch (SQLException ex) {
            String msg = String.format("Unexpected fatal, error in '%s' when executing '%s' [%s]",
                                       this, next, dbType);
            // FIXME: PAVLO 2016-12-29
            // Right now our DBMS throws an exception when the txn gets aborted
            // due to a conflict, so for now we have to not kill ourselves.
            // This *does not* incorrectly inflate our performance numbers.
            // It's more of a workaround for now until I can figure out how to do
            // this correctly in JDBC.
            if (dbType == DatabaseType.PELOTON) {
                msg += "\nBut we are not stopping because " + dbType + " cannot handle this correctly";
                LOG.warn(msg);
            } else {
                throw new RuntimeException(msg, ex);
            }
        }

        return (next);
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
     * @param txnType
     * @return TODO
     * @throws UserAbortException
     *             TODO
     * @throws SQLException
     *             TODO
     */
    protected abstract TransactionStatus executeWork(TransactionType txnType) throws UserAbortException, SQLException;

    /**
     * Called at the end of the test to do any clean up that may be required.
     * 
     * @param error
     *            TODO
     */
    public void tearDown(boolean error) {
        try {
            conn.close();
        } catch (SQLException e) {
            LOG.warn("No connection to close");
        }
    }

    public void initializeState() {
        assert (this.wrkldState == null);
        this.wrkldState = this.wrkld.getWorkloadState();
    }
}
