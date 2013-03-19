/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:    Carlo Curino <carlo.curino@gmail.com>
 *              Evan Jones <ej@evanjones.ca>
 *              DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 *              Andy Pavlo <pavlo@cs.brown.edu>
 *              CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *                  Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.oltpbenchmark.LatencyRecord.Sample;
import com.oltpbenchmark.ThreadBench.TimeBucketIterable;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.util.Histogram;

public final class Results {
    public final long nanoSeconds;
    public final int measuredRequests;
    public final DistributionStatistics latencyDistribution;
    final Histogram<TransactionType> txnSuccess = new Histogram<TransactionType>(true);
    final Histogram<TransactionType> txnAbort = new Histogram<TransactionType>(true);
    final Histogram<TransactionType> txnRetry = new Histogram<TransactionType>(true);
    final Histogram<TransactionType> txnErrors = new Histogram<TransactionType>(true);
    final Map<TransactionType, Histogram<String>> txnAbortMessages = new HashMap<TransactionType, Histogram<String>>();
    
    public final List<LatencyRecord.Sample> latencySamples;

    public Results(long nanoSeconds, int measuredRequests, DistributionStatistics latencyDistribution, final List<LatencyRecord.Sample> latencySamples) {
        this.nanoSeconds = nanoSeconds;
        this.measuredRequests = measuredRequests;
        this.latencyDistribution = latencyDistribution;

        if (latencyDistribution == null) {
            assert latencySamples == null;
            this.latencySamples = null;
        } else {
            // defensive copy
            this.latencySamples = Collections.unmodifiableList(new ArrayList<LatencyRecord.Sample>(latencySamples));
            assert !this.latencySamples.isEmpty();
        }
    }

    /**
     * Get a histogram of how often each transaction was executed
     */
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

    public double getRequestsPerSecond() {
        return (double) measuredRequests / (double) nanoSeconds * 1e9;
    }

    @Override
    public String toString() {
        return "Results(nanoSeconds=" + nanoSeconds + ", measuredRequests=" + measuredRequests + ") = " + getRequestsPerSecond() + " requests/sec";
    }

    public void writeCSV(int windowSizeSeconds, PrintStream out) {
        out.println("time(sec), throughput(req/sec), avg_lat(ms), min_lat(ms), 25th_lat(ms), median_lat(ms), 75th_lat(ms), 90th_lat(ms), 95th_lat(ms), 99th_lat(ms), max_lat(ms)");
        int i = 0;
        for (DistributionStatistics s : new TimeBucketIterable(latencySamples, windowSizeSeconds)) {
            final double MILLISECONDS_FACTOR = 1e3;
            out.printf("%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n", i * windowSizeSeconds, (double) s.getCount() / windowSizeSeconds, s.getAverage() / MILLISECONDS_FACTOR,
                    s.getMinimum() / MILLISECONDS_FACTOR, s.get25thPercentile() / MILLISECONDS_FACTOR, s.getMedian() / MILLISECONDS_FACTOR, s.get75thPercentile() / MILLISECONDS_FACTOR,
                    s.get90thPercentile() / MILLISECONDS_FACTOR, s.get95thPercentile() / MILLISECONDS_FACTOR, s.get99thPercentile() / MILLISECONDS_FACTOR, s.getMaximum() / MILLISECONDS_FACTOR);
            i += 1;
        }
    }

    public void writeAllCSV(PrintStream out) {
        long startNs = latencySamples.get(0).startNs;
        out.println("transaction type (index in config file), start time (microseconds),latency (microseconds)");
        for (Sample s : latencySamples) {
            long startUs = (s.startNs - startNs + 500) / 1000;
            out.println(s.tranType + "," + startUs + "," + s.latencyUs);
        }
    }

    public void writeAllCSVAbsoluteTiming(PrintStream out) {

        // This is needed because nanTime does not guarantee offset... we
        // ground it (and round it) to ms from 1970-01-01 like currentTime
        double x = ((double) System.nanoTime() / (double) 1000000000);
        double y = ((double) System.currentTimeMillis() / (double) 1000);
        double offset = x - y;

        // long startNs = latencySamples.get(0).startNs;
        out.println("transaction type (index in config file), start time (microseconds),latency (microseconds),worker id(start number), phase id(index in config file)");
        for (Sample s : latencySamples) {
            double startUs = ((double) s.startNs / (double) 1000000000);
            out.println(s.tranType + "," + String.format("%10.6f", startUs - offset) + "," + s.latencyUs + "," + s.workerId + "," + s.phaseId);
        }
    }

}