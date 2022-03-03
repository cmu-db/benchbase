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

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Efficiently stores a record of (start time, latency) pairs.
 */
public class LatencyRecord implements Iterable<LatencyRecord.Sample> {
    /**
     * Allocate space for 500k samples at a time
     */
    static final int ALLOC_SIZE = 500000;

    /**
     * Contains (start time, latency, transactionType, workerid, phaseid) pentiplets
     * in microsecond form. The start times are "compressed" by encoding them as
     * increments, starting from startNs. A 32-bit integer provides sufficient resolution
     * for an interval of 2146 seconds, or 35 minutes.
     */
    private final ArrayList<Sample[]> values = new ArrayList<>();
    private int nextIndex;

    private final long startNanosecond;
    private long lastNanosecond;

    public LatencyRecord(long startNanosecond) {
        this.startNanosecond = startNanosecond;
        this.lastNanosecond = startNanosecond;
        allocateChunk();

    }

    public void addLatency(int transType, long startNanosecond, long endNanosecond, int workerId, int phaseId) {


        if (nextIndex == ALLOC_SIZE) {
            allocateChunk();
        }
        Sample[] chunk = values.get(values.size() - 1);

        long startOffsetNanosecond = (startNanosecond - lastNanosecond + 500);

        int latencyMicroseconds = (int) ((endNanosecond - startNanosecond + 500) / 1000);


        chunk[nextIndex] = new Sample(transType, startOffsetNanosecond, latencyMicroseconds, workerId, phaseId);
        ++nextIndex;

        lastNanosecond += startOffsetNanosecond;
    }

    private void allocateChunk() {
        values.add(new Sample[ALLOC_SIZE]);
        nextIndex = 0;
    }

    /**
     * Returns the number of recorded samples.
     */
    public int size() {
        // Samples stored in full chunks
        int samples = (values.size() - 1) * ALLOC_SIZE;

        // Samples stored in the last not full chunk
        samples += nextIndex;
        return samples;
    }

    /**
     * Stores the start time and latency for a single sample. Immutable.
     */
    public static final class Sample implements Comparable<Sample> {
        private final int transactionType;
        private long startNanosecond;
        private final int latencyMicrosecond;
        private final int workerId;
        private final int phaseId;

        public Sample(int transactionType, long startNanosecond, int latencyMicrosecond, int workerId, int phaseId) {
            this.transactionType = transactionType;
            this.startNanosecond = startNanosecond;
            this.latencyMicrosecond = latencyMicrosecond;
            this.workerId = workerId;
            this.phaseId = phaseId;
        }

        public int getTransactionType() {
            return transactionType;
        }

        public long getStartNanosecond() {
            return startNanosecond;
        }

        public int getLatencyMicrosecond() {
            return latencyMicrosecond;
        }

        public int getWorkerId() {
            return workerId;
        }

        public int getPhaseId() {
            return phaseId;
        }

        @Override
        public int compareTo(Sample other) {
            long diff = this.startNanosecond - other.startNanosecond;

            // explicit comparison to avoid long to int overflow
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            } else {

                return 0;
            }
        }
    }

    private final class LatencyRecordIterator implements Iterator<Sample> {
        private int chunkIndex = 0;
        private int subIndex = 0;
        private long lastIteratorNanosecond = startNanosecond;

        @Override
        public boolean hasNext() {
            if (chunkIndex < values.size() - 1) {
                return true;
            }
            return subIndex < nextIndex;
        }

        @Override
        public Sample next() {
            Sample[] chunk = values.get(chunkIndex);
            Sample s = chunk[subIndex];

            // Iterate in chunk, and wrap to next one
            ++subIndex;

            if (subIndex == ALLOC_SIZE) {
                chunkIndex += 1;
                subIndex = 0;
            }

            // Previously, s.startNs was just an offset from the previous
            // value.  Now we make it an absolute.
            s.startNanosecond += lastIteratorNanosecond;
            lastIteratorNanosecond = s.startNanosecond;

            return s;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove is not supported");
        }
    }

    public Iterator<Sample> iterator() {
        return new LatencyRecordIterator();
    }
}
