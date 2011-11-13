package com.oltpbenchmark.api;

import java.sql.Connection;

import com.oltpbenchmark.BenchmarkState;
import com.oltpbenchmark.LatencyRecord;
import com.oltpbenchmark.Phase;
import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.ThreadBench.State;

public abstract class Worker implements Runnable {
	private BenchmarkState testState;
	private LatencyRecord latencies;
	
	private final int id;
	protected final Connection conn;
	protected final WorkLoadConfiguration wrkld;
	protected final TransactionTypes transTypes;
	
	public Worker(int id, Connection conn, WorkLoadConfiguration wrkld) {
		this.id = id;
		this.conn = conn;
		this.wrkld = wrkld;
		this.transTypes = this.wrkld.getTransTypes();
	}
	
	/**
	 * Unique thread id for this worker
	 * @return
	 */
	public int getId() {
		return this.id;
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
		
		TransactionType invalidTT = transTypes.getType("INVALID");
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