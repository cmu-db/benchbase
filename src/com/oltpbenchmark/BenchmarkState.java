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

package com.oltpbenchmark;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.oltpbenchmark.types.State;

public final class BenchmarkState {
    
    private static final Logger LOG = Logger.getLogger(BenchmarkState.class);

	private volatile State state = State.WARMUP;

	// Assigned a value when starting the test. Used for offsets in the
	// latency record.
	private final long testStartNs;

	public long getTestStartNs() {
		return testStartNs;
	}

	private final CountDownLatch startBarrier;
	private AtomicInteger notDoneCount;

	// Protected by this

	/**
	 * 
	 * @param numThreads
	 *            number of threads involved in the test: including the
	 *            master thread.
	 * @param rateLimited
	 * @param queueLimit
	 */
	public BenchmarkState(int numThreads) {
		startBarrier = new CountDownLatch(numThreads);
		notDoneCount = new AtomicInteger(numThreads);
	
		assert numThreads > 0;

		testStartNs = System.nanoTime();
	}

	public State getState() {
	    synchronized (this) {
	        return state;
        }
	}

	/**
	 * Wait for all threads to call this. Returns once all the threads have
	 * entered.
	 */
	public void blockForStart() {
		assert state == State.WARMUP;
		assert startBarrier.getCount() > 0;
		startBarrier.countDown();
		try {
			startBarrier.await();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public void startMeasure() {
		assert state == State.WARMUP;

		state = State.MEASURE;
	}

    public void startColdQuery() {
        assert state == State.MEASURE;
        state = State.COLD_QUERY;
    }

    public void startHotQuery() {
        assert state == State.COLD_QUERY;
        state = State.MEASURE;
    }

    public void signalLatencyComplete() {
        assert state == State.MEASURE;
        state = State.LATENCY_COMPLETE;
    }

    public void ackLatencyComplete() {
        assert state == State.LATENCY_COMPLETE;
        state = State.MEASURE;
    }

	public void startCoolDown() {
		assert state == State.MEASURE;
		state = State.DONE;

		// The master thread must also signal that it is done
		signalDone();
	}

	/** Notify that this thread has entered the done state. */
	public int signalDone() {
		assert state == State.DONE;
		int current = notDoneCount.decrementAndGet();
		assert current >= 0;
		if (LOG.isDebugEnabled())
		    LOG.debug(String.format("%d workers are not done. Waiting until they finish", current));
		if (current == 0) {
            // We are the last thread to notice that we are done: wake any
            // blocked workers
		    this.state = State.EXIT;
		}
		return current;
	}

}