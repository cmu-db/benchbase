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

import com.oltpbenchmark.LatencyRecord.Sample;
import com.oltpbenchmark.ThreadBench.TimeBucketIterable;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.StringUtil;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Results {
    private final long nanoseconds;
    private final int measuredRequests;
    private final DistributionStatistics distributionStatistics;
    private final List<LatencyRecord.Sample> latencySamples;
    private final Histogram<TransactionType> unknown = new Histogram<>(false);
    private final Histogram<TransactionType> success = new Histogram<>(true);
    private final Histogram<TransactionType> abort = new Histogram<>(false);
    private final Histogram<TransactionType> retry = new Histogram<>(false);
    private final Histogram<TransactionType> error = new Histogram<>(false);
    private final Histogram<TransactionType> retryDifferent = new Histogram<>(false);
    private final Map<TransactionType, Histogram<String>> abortMessages = new HashMap<>();

    public Results(long nanoseconds, int measuredRequests, DistributionStatistics distributionStatistics, final List<LatencyRecord.Sample> latencySamples) {
        this.nanoseconds = nanoseconds;
        this.measuredRequests = measuredRequests;
        this.distributionStatistics = distributionStatistics;

        if (distributionStatistics == null) {

            this.latencySamples = null;
        } else {
            // defensive copy
            this.latencySamples = List.copyOf(latencySamples);

        }
    }

    public DistributionStatistics getDistributionStatistics() {
        return distributionStatistics;
    }

    public Histogram<TransactionType> getSuccess() {
        return success;
    }

    public Histogram<TransactionType> getUnknown() {
        return unknown;
    }

    public Histogram<TransactionType> getAbort() {
        return abort;
    }

    public Histogram<TransactionType> getRetry() {
        return retry;
    }

    public Histogram<TransactionType> getError() {
        return error;
    }

    public Histogram<TransactionType> getRetryDifferent() {
        return retryDifferent;
    }

    public Map<TransactionType, Histogram<String>> getAbortMessages() {
        return abortMessages;
    }

    public double requestsPerSecond() {
        return (double) measuredRequests / (double) nanoseconds * 1e9;
    }

    @Override
    public String toString() {
        return "Results(nanoSeconds=" + nanoseconds + ", measuredRequests=" + measuredRequests + ") = " + requestsPerSecond() + " requests/sec";
    }

    public void writeCSV(int windowSizeSeconds, PrintStream out) {
        writeCSV(windowSizeSeconds, out, TransactionType.INVALID);
    }

    public void writeCSV(int windowSizeSeconds, PrintStream out, TransactionType txType) {
        out.println("time(sec), throughput(req/sec), avg_lat(ms), min_lat(ms), 25th_lat(ms), median_lat(ms), 75th_lat(ms), 90th_lat(ms), 95th_lat(ms), 99th_lat(ms), max_lat(ms), tp (req/s) scaled");
        int i = 0;
        for (DistributionStatistics s : new TimeBucketIterable(latencySamples, windowSizeSeconds, txType)) {
            final double MILLISECONDS_FACTOR = 1e3;
            out.printf("%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n", i * windowSizeSeconds, (double) s.getCount() / windowSizeSeconds, s.getAverage() / MILLISECONDS_FACTOR,
                    s.getMinimum() / MILLISECONDS_FACTOR, s.get25thPercentile() / MILLISECONDS_FACTOR, s.getMedian() / MILLISECONDS_FACTOR, s.get75thPercentile() / MILLISECONDS_FACTOR,
                    s.get90thPercentile() / MILLISECONDS_FACTOR, s.get95thPercentile() / MILLISECONDS_FACTOR, s.get99thPercentile() / MILLISECONDS_FACTOR, s.getMaximum() / MILLISECONDS_FACTOR,
                    MILLISECONDS_FACTOR / s.getAverage());
            i += 1;
        }
    }

    public void writeCSV2(PrintStream out) {
        writeCSV2(1, out, TransactionType.INVALID);
    }

    public void writeCSV2(int windowSizeSeconds, PrintStream out, TransactionType txType) {
        String[] header = {
                "Time (seconds)",
                "Requests",
                "Throughput (requests/second)",
                "Minimum Latency (microseconds)",
                "25th Percentile Latency (microseconds)",
                "Median Latency (microseconds)",
                "Average Latency (microseconds)",
                "75th Percentile Latency (microseconds)",
                "90th Percentile Latency (microseconds)",
                "95th Percentile Latency (microseconds)",
                "99th Percentile Latency (microseconds)",
                "Maximum Latency (microseconds)"
        };
        out.println(StringUtil.join(",", header));
        int i = 0;
        for (DistributionStatistics s : new TimeBucketIterable(latencySamples, windowSizeSeconds, txType)) {
            out.printf("%d,%d,%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
                    i * windowSizeSeconds,
                    s.getCount(),
                    (double) s.getCount() / windowSizeSeconds,
                    (int) s.getMinimum(),
                    (int) s.get25thPercentile(),
                    (int) s.getMedian(),
                    (int) s.getAverage(),
                    (int) s.get75thPercentile(),
                    (int) s.get90thPercentile(),
                    (int) s.get95thPercentile(),
                    (int) s.get99thPercentile(),
                    (int) s.getMaximum());
            i += 1;
        }
    }

    public void writeAllCSVAbsoluteTiming(List<TransactionType> activeTXTypes, PrintStream out) {

        // This is needed because nanTime does not guarantee offset... we
        // ground it (and round it) to ms from 1970-01-01 like currentTime
        double x = ((double) System.nanoTime() / (double) 1000000000);
        double y = ((double) System.currentTimeMillis() / (double) 1000);
        double offset = x - y;

        // long startNs = latencySamples.get(0).startNs;
        String[] header = {
                "Transaction Type Index",
                "Transaction Name",
                "Start Time (microseconds)",
                "Latency (microseconds)",
                "Worker Id (start number)",
                "Phase Id (index in config file)"
        };
        out.println(StringUtil.join(",", header));
        for (Sample s : latencySamples) {
            double startUs = ((double) s.startNs / (double) 1000000000);
            String[] row = {
                    Integer.toString(s.tranType),
                    // Important!
                    // The TxnType offsets start at 1!
                    activeTXTypes.get(s.tranType - 1).getName(),
                    String.format("%10.6f", startUs - offset),
                    Integer.toString(s.latencyUs),
                    Integer.toString(s.workerId),
                    Integer.toString(s.phaseId),
            };
            out.println(StringUtil.join(",", row));
        }
    }

}