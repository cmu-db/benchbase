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

import com.oltpbenchmark.types.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public final class BenchmarkState {

    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkState.class);

    private final long testStartNs;
    private final CountDownLatch startBarrier;
    private final AtomicInteger notDoneCount;
    private volatile State state = State.WARMUP;

    /**
     * @param numThreads number of threads involved in the test: including the
     *                   master thread.
     */
    public BenchmarkState(int numThreads) {
        startBarrier = new CountDownLatch(numThreads);
        notDoneCount = new AtomicInteger(numThreads);


        testStartNs = System.nanoTime();
    }

    // Protected by this

    public long getTestStartNs() {
        return testStartNs;
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


        startBarrier.countDown();
        try {
            startBarrier.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void startMeasure() {
        state = State.MEASURE;
    }

    public void startColdQuery() {
        state = State.COLD_QUERY;
    }

    public void startHotQuery() {
        state = State.MEASURE;
    }

    public void signalLatencyComplete() {
        state = State.LATENCY_COMPLETE;
    }

    public void ackLatencyComplete() {
        state = State.MEASURE;
    }

    public void signalError() {
        // A thread died, decrement the count and set error state
        notDoneCount.decrementAndGet();
        state = State.ERROR;
    }

    public void startCoolDown() {
        state = State.DONE;

        // The master thread must also signal that it is done
        signalDone();
    }

    /**
     * Notify that this thread has entered the done state.
     */
    public int signalDone() {

        int current = notDoneCount.decrementAndGet();

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("%d workers are not done. Waiting until they finish", current));
        }
        if (current == 0) {
            // We are the last thread to notice that we are done: wake any
            // blocked workers
            this.state = State.EXIT;
        }
        return current;
    }

}