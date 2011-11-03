package com.oltpbenchmark;

import com.oltpbenchmark.LatencyRecord.Sample;
import com.oltpbenchmark.ThreadBench.BenchmarkState;
import com.oltpbenchmark.ThreadBench.State;
import com.oltpbenchmark.WorkLoadConfiguration.Phase;
import com.oltpbenchmark.tpcc.jTPCCConfig;

public abstract class Worker implements Runnable {
	private BenchmarkState testState;
	private LatencyRecord latencies;

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

			jTPCCConfig.TransactionType type = doWork(measure, phase);
			if (measure) {
				long end = System.nanoTime();
				latencies.addLatency(type.ordinal(), start, end);
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
	 * 
	 * @param llr
	 */
	protected abstract jTPCCConfig.TransactionType doWork(boolean measure,
			Phase phase);

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