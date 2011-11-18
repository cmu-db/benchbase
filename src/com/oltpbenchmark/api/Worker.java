package com.oltpbenchmark.api;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.oltpbenchmark.BenchmarkState;
import com.oltpbenchmark.LatencyRecord;
import com.oltpbenchmark.Phase;
import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.types.State;

public abstract class Worker implements Runnable {
    private static final Logger LOG = Logger.getLogger(Worker.class);
            
	private BenchmarkState testState;
	private LatencyRecord latencies;
	
	protected final int id;
	protected final BenchmarkModule benchmarkModule;
	protected final Connection conn;
	protected final WorkLoadConfiguration wrkld;
	protected final TransactionTypes transactionTypes;
	protected final Map<TransactionType, Procedure> procedures = new HashMap<TransactionType, Procedure>();
	protected final Map<String, Procedure> name_procedures = new HashMap<String, Procedure>();
	protected final Map<Class<? extends Procedure>, Procedure> class_procedures = new HashMap<Class<? extends Procedure>, Procedure>();
	
	public Worker(int id, BenchmarkModule benchmarkModule) {
		this.id = id;
		this.benchmarkModule = benchmarkModule;
		this.wrkld = this.benchmarkModule.getWorkloadConfiguration();
		this.transactionTypes = this.wrkld.getTransTypes();
		
		try {
		    this.conn = this.benchmarkModule.getConnection();
		    this.conn.setAutoCommit(false);
		} catch (SQLException ex) {
		    throw new RuntimeException("Failed to connect to database", ex);
		}
		
		// Generate all the Procedures that we're going to need
		this.procedures.putAll(this.benchmarkModule.getProcedures());
        for (Entry<TransactionType, Procedure> e : this.procedures.entrySet()) {
            Procedure proc = e.getValue();
            this.name_procedures.put(e.getKey().getName(), proc);
            this.class_procedures.put(proc.getClass(), proc);
            e.getValue().generateAllPreparedStatements(this.conn);
        } // FOR
	}
	
	/**
	 * Unique thread id for this worker
	 * @return
	 */
	public int getId() {
		return this.id;
	}
	
	public final Procedure getProcedure(TransactionType type) {
        return (this.procedures.get(type));
    }
    public final Procedure getProcedure(String name) {
        return (this.name_procedures.get(name));
    }
    @SuppressWarnings("unchecked")
    public final <T extends Procedure> T getProcedure(Class<T> procClass) {
        return (T)(this.class_procedures.get(procClass));
    }

	@Override
	public final void run() {
		// TODO: Make this an interface; move code to class to prevent reuse
		// In case of reuse reset the measurements
		latencies = new LatencyRecord(testState.getTestStartNs());
		boolean isRateLimited = testState.isRateLimited();

		// wait for start
		testState.blockForStart();

		// System.out.println(this + " start");
		boolean seenDone = false;
		State state = testState.getState();
		
		TransactionType invalidTT = wrkld.getTransTypes().getType("INVALID");
		assert(invalidTT != null);
		
		while (state != State.EXIT) {
			if (state == State.DONE && !seenDone) {
				// This is the first time we have observed that the test is
				// done
				// notify the global test state, then continue applying load
				seenDone = true;
				testState.signalDone();
			}
			Phase phase = null;
			// apply load
			if (isRateLimited) {
				// re-reads the state because it could have changed if we
				// blocked
				state = testState.fetchWork();
				phase = testState.fetchWorkType();
			}

			boolean measure = state == State.MEASURE;

			// TODO: Measuring latency when not rate limited is ... a little
			// weird because
			// if you add more simultaneous clients, you will increase
			// latency (queue delay)
			// but we do this anyway since it is useful sometimes
			long start = 0;
			if (measure) {
				start = System.nanoTime();
			}

			TransactionType type = invalidTT;
			if (phase != null) type = doWork(measure, phase);
			assert(type != null);
			
			if (measure) {
				long end = System.nanoTime();
				latencies.addLatency(type.getId(), start, end);
			}
			state = testState.getState();
		}

		tearDown();
		testState = null;
	}

	public int getRequests() {
		return latencies.size();
	}

	public Iterable<LatencyRecord.Sample> getLatencyRecords() {
		return latencies;
	}

	/**
	 * Called in a loop in the thread to exercise the system under test.
	 * Each implementing worker should return the TransactionType handle that
	 * was executed.
	 * 
	 * @param llr
	 */
	protected abstract TransactionType doWork(boolean measure, Phase phase);

	/**
	 * Called at the end of the test to do any clean up that may be
	 * required.
	 */
	protected void tearDown() {
	}

	public void setBenchmark(BenchmarkState testState) {
		assert this.testState == null;
		this.testState = testState;
	}
}