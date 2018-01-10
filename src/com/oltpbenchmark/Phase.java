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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.oltpbenchmark.util.StringUtil;

public class Phase {
    public enum Arrival {
        REGULAR,POISSON,
    }

    private final Random gen = new Random();
    public final String benchmarkName;
    public final int id;
    public final int time;
    public final int warmupTime;
    public final int rate;
    public final Arrival arrival;


    private final boolean rateLimited;
    private final boolean disabled;
    private final boolean serial;
    private final boolean timed;
    private final List<Double> weights;
    private final int num_weights;
    private int activeTerminals;
    private int nextSerial;
    

    Phase(String benchmarkName, int id, int t, int wt, int r, List<String> o, boolean rateLimited, boolean disabled, boolean serial, boolean timed, int activeTerminals, Arrival a) {
        ArrayList<Double> w = new ArrayList<Double>();
        for (String s : o)
            w.add(Double.parseDouble(s));

        this.benchmarkName = benchmarkName;
        this.id = id;
        this.time = t;
        this.warmupTime = wt;
        this.rate = r;
        this.weights = Collections.unmodifiableList(w);
        this.num_weights = this.weights.size();
        this.rateLimited = rateLimited;
        this.disabled = disabled;
        this.serial = serial;
        this.timed = timed;
        this.nextSerial = 1;
        this.activeTerminals = activeTerminals;
        this.arrival=a;
    }
    
    public boolean isRateLimited() {
        return rateLimited;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public boolean isSerial() {
        return serial;
    }

    public boolean isTimed() {
        return timed;
    }

    public boolean isLatencyRun() {
        return !timed && serial;
    }

    public boolean isThroughputRun() {
        return !isLatencyRun();
    }

    public void resetSerial() {
        this.nextSerial = 1;
    }

    public int getActiveTerminals() {
        return activeTerminals;
    }

    public int getWeightCount() {
        return (this.num_weights);
    }
    public List<Double> getWeights() {
        return (this.weights);
    }
    
    /**
     * Computes the sum of weights. Usually needs to add up to 100%
     * 
     * @return The total weight
     */
    public double totalWeight() {
        double total = 0.0;
        for (Double d : weights)
            total += d;
        return total;
    }
    
    /**
     * This simply computes the next transaction by randomly selecting one based
     * on the weights of this phase.
     * 
     * @return
     */
    public int chooseTransaction() {
        return chooseTransaction(false);
    }
    public int chooseTransaction(boolean isColdQuery) {
        if (isDisabled())
            return -1;

        if (isSerial()) {
            int ret;
            synchronized(this) {
                ret = this.nextSerial;

                // Serial runs should not execute queries with non-positive
                // weights.
                while (ret <= this.num_weights && weights.get(ret - 1).doubleValue() <= 0.0)
                    ret = ++this.nextSerial;

                // If it's a cold execution, then we don't want to advance yet,
                // since the hot run needs to execute the same query.
                if (!isColdQuery) {
                    // For timed, serial executions, we're doing a QPS (query
                    // throughput) run, so we loop through the list multiple
                    // times. Note that we do the modulus before the increment
                    // so that we end up in the range [1,num_weights]
                    if (isTimed()) {
                        assert this.isThroughputRun();
                        this.nextSerial %= this.num_weights;
                    }

                    ++this.nextSerial;
                }
            }
            return ret;
        }
        else {
            int randomPercentage = gen.nextInt((int)totalWeight()) + 1;
        double weight = 0.0;
        for (int i = 0; i < this.num_weights; i++) {
            weight += weights.get(i).doubleValue();
            if (randomPercentage <= weight) {
                return i + 1;
            }
        } // FOR
        }

        return -1;
    }
    
    /**
     * Returns a string for logging purposes when entering the phase
     */
    public String currentPhaseString() {
        List<String> inner = new ArrayList<String>();
        inner.add("[Workload=" + benchmarkName.toUpperCase() + "]");
        if (isDisabled()){
            inner.add("[Disabled=true]");
        } else {
            if (isLatencyRun()) {
                inner.add("[Serial=true]");
                inner.add("[Time=n/a]");
            }
            else {
                inner.add("[Serial="+ isSerial() + "]");
                inner.add("[Time=" + time + "]");
            }
            inner.add("[WarmupTime=" + warmupTime + "]");
            inner.add("[Rate=" + (isRateLimited() ? rate : "unlimited") + "]");
            inner.add("[Arrival=" + arrival + "]");
            inner.add("[Ratios=" + getWeights() + "]");
            inner.add("[ActiveWorkers=" + getActiveTerminals() + "]");
        }
        
        return StringUtil.bold("PHASE START") + " :: " + StringUtil.join(" ", inner);
    }

}