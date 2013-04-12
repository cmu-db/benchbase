/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import com.oltpbenchmark.LatencyRecord.Sample;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.QueueLimitException;
import com.oltpbenchmark.util.StringUtil;

public class ThreadBench implements Thread.UncaughtExceptionHandler {
    private static final Logger LOG = Logger.getLogger(ThreadBench.class);

    
    private static BenchmarkState testState;
    private final List<? extends Worker> workers;
    private final ArrayList<Thread> workerThreads;
    // private File profileFile;
    private List<WorkloadConfiguration> workConfs;
    private List<WorkloadState> workStates;
    ArrayList<LatencyRecord.Sample> samples = new ArrayList<LatencyRecord.Sample>();

    private ThreadBench(List<? extends Worker> workers, List<WorkloadConfiguration> workConfs) {
        this(workers, null, workConfs);
    }

    public ThreadBench(List<? extends Worker> workers, File profileFile, List<WorkloadConfiguration> workConfs) {
        this.workers = workers;
        this.workConfs = workConfs;
        this.workerThreads = new ArrayList<Thread>(workers.size());
    }

    public static final class TimeBucketIterable implements Iterable<DistributionStatistics> {
        private final Iterable<Sample> samples;
        private final int windowSizeSeconds;

        public TimeBucketIterable(Iterable<Sample> samples, int windowSizeSeconds) {
            this.samples = samples;
            this.windowSizeSeconds = windowSizeSeconds;
        }

        @Override
        public Iterator<DistributionStatistics> iterator() {
            return new TimeBucketIterator(samples.iterator(), windowSizeSeconds);
        }
    }

    public static final class TimeBucketIterator implements Iterator<DistributionStatistics> {
        private final Iterator<Sample> samples;
        private final int windowSizeSeconds;

        private Sample sample;
        private long nextStartNs;

        private DistributionStatistics next;

        public TimeBucketIterator(Iterator<LatencyRecord.Sample> samples, int windowSizeSeconds) {
            this.samples = samples;
            this.windowSizeSeconds = windowSizeSeconds;

            if (samples.hasNext()) {
                sample = samples.next();
                // TODO: To be totally correct, we would want this to be the
                // timestamp of the start
                // of the measurement interval. In most cases this won't matter.
                nextStartNs = sample.startNs;
                calculateNext();
            }
        }

        private void calculateNext() {
            assert next == null;
            assert sample != null;
            assert sample.startNs >= nextStartNs;

            // Collect all samples in the time window
            ArrayList<Integer> latencies = new ArrayList<Integer>();
            long endNs = nextStartNs + windowSizeSeconds * 1000000000L;
            while (sample != null && sample.startNs < endNs) {
                latencies.add(sample.latencyUs);

                if (samples.hasNext()) {
                    sample = samples.next();
                } else {
                    sample = null;
                }
            }

            // Set up the next time window
            assert sample == null || endNs <= sample.startNs;
            nextStartNs = endNs;

            int[] l = new int[latencies.size()];
            for (int i = 0; i < l.length; ++i) {
                l[i] = latencies.get(i);
            }

            next = DistributionStatistics.computeStatistics(l);
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public DistributionStatistics next() {
            if (next == null)
                throw new NoSuchElementException();
            DistributionStatistics out = next;
            next = null;
            if (sample != null) {
                calculateNext();
            }
            return out;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("unsupported");
        }
    }

    private void createWorkerThreads() {
        
        for (Worker worker : workers) {
            worker.initializeState();
            Thread thread = new Thread(worker);
            thread.setUncaughtExceptionHandler(this);
            thread.start();
            this.workerThreads.add(thread);
        }
        return;
    }

    private int finalizeWorkers(ArrayList<Thread> workerThreads) throws InterruptedException {
        assert testState.getState() == State.DONE || testState.getState() == State.EXIT;
        int requests = 0;
        
        new WatchDogThread().start();
        
        for (int i = 0; i < workerThreads.size(); ++i) {

            // FIXME not sure this is the best solution... ensure we don't hang
            // forever, however we might ignore
            // problems
            workerThreads.get(i).join(60000); // wait for 60second for threads
                                              // to terminate... hands otherwise

            /*
             * // CARLO: Maybe we might want to do this to kill threads that are
             * hanging... if (workerThreads.get(i).isAlive()) {
             * workerThreads.get(i).kill(); try { workerThreads.get(i).join(); }
             * catch (InterruptedException e) { } }
             */

            requests += workers.get(i).getRequests();
            workers.get(i).tearDown(false);
        }
        testState = null;
        return requests;
    }
    
    private class WatchDogThread extends Thread {
        {
            this.setDaemon(true);
        }
        @Override
        public void run() {
            Map<String, Object> m = new ListOrderedMap<String, Object>();
            LOG.info("Starting WatchDogThread");
            while (true) {
                try {
                    Thread.sleep(20000);
                } catch (InterruptedException ex) {
                    return;
                }
                if (testState == null) return;
                m.clear();
                for (Thread t : workerThreads) {
                    m.put(t.getName(), t.isAlive());
                }
                LOG.info("Worker Thread Status:\n" + StringUtil.formatMaps(m));
            } // WHILE
        }
    } // CLASS

    /*
     * public static Results runRateLimitedBenchmark(List<Worker> workers, File
     * profileFile) throws QueueLimitException, IOException { ThreadBench bench
     * = new ThreadBench(workers, profileFile); return
     * bench.runRateLimitedFromFile(); }
     */

    public static Results runRateLimitedBenchmark(List<Worker> workers, List<WorkloadConfiguration> workConfs) throws QueueLimitException, IOException {
        ThreadBench bench = new ThreadBench(workers, workConfs);
        return bench.runRateLimitedMultiPhase();
    }

    public Results runRateLimitedMultiPhase() throws QueueLimitException, IOException {
        assert testState == null;
        testState = new BenchmarkState(workers.size() + 1);
        workStates = new ArrayList<WorkloadState>();
        
        for (WorkloadConfiguration workState : this.workConfs) {
            workStates.add(workState.initializeState(testState));
        }
        
        this.createWorkerThreads();
        testState.blockForStart();

        // long measureStart = start;
        
        long start = System.nanoTime();
        long measureEnd = -1;
        //used to determine the longest sleep interval
        int lowestRate = Integer.MAX_VALUE;
        
        Phase phase = null;
        
        
        for (WorkloadState workState : this.workStates) {
        	workState.switchToNextPhase();
        	phase = workState.getCurrentPhase();
        	LOG.info(phase.currentPhaseString());
        	if (phase.rate < lowestRate) {
        	    lowestRate = phase.rate;
        	}
        }

        long intervalNs = getInterval(lowestRate,phase.arrival);

        long nextInterval = start + intervalNs;
        int nextToAdd = 1;
        int rateFactor;

        boolean resetQueues = true;

        long delta = phase.time * 1000000000L;
        boolean lastEntry = false;

        while (true) {
            // posting new work... and reseting the queue in case we have new
            // portion of the workload...
            
            for (WorkloadState workState : this.workStates) {
                if (workState.getCurrentPhase() != null) {
                    rateFactor = workState.getCurrentPhase().rate / lowestRate;
                } else {
                    rateFactor = 1;
                }
                    workState.addToQueue(nextToAdd * rateFactor, resetQueues);
            }
            resetQueues = false;

            // Wait until the interval expires, which may be "don't wait"
            long now = System.nanoTime();
            long diff = nextInterval - now;
            while (diff > 0) { // this can wake early: sleep multiple times to
                               // avoid that
                long ms = diff / 1000000;
                diff = diff % 1000000;
                try {
                    Thread.sleep(ms, (int) diff);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                now = System.nanoTime();
                diff = nextInterval - now;
            }
            assert diff <= 0;

            if (start + delta < System.nanoTime() && !lastEntry) {
                // enters here after each phase of the test
                // reset the queues so that the new phase is not affected by the
                // queue of the previous one
                resetQueues = true;
                
                // Fetch a new Phase
                synchronized (testState) {
                    for (WorkloadState workState : workStates) {
                    	workState.switchToNextPhase();
                    	lowestRate = Integer.MAX_VALUE;
                    	phase = workState.getCurrentPhase();
                    	if (phase == null) {
                    	    // Last phase
                    	    lastEntry = true;
                    	    break;
                    	} else {
                    	    LOG.info(phase.currentPhaseString());
                            if (phase.rate < lowestRate) {
                                lowestRate = phase.rate;
                            }
                    	}
                    }
                    if (phase != null) {
                        // update frequency in which we check according to wakeup
                        // speed
                        // intervalNs = (long) (1000000000. / (double) lowestRate + 0.5);
                        delta += phase.time * 1000000000L;
                    }
                }
            }

            // Compute the next interval 
            // and how many messages to deliver
            if(phase != null)
            {
                intervalNs=0;
                nextToAdd = 0;
                do
                {
                    intervalNs += getInterval(lowestRate,phase.arrival);
                    nextToAdd ++;
                } while ( (-diff) > intervalNs && !lastEntry);
                nextInterval += intervalNs;
            }

            // Update the test state appropriately
            State state = testState.getState();
            if (state == State.WARMUP && now >= start) {
                testState.startMeasure();
                start = now;
                // measureEnd = measureStart + measureSeconds * 1000000000L;
            } else if (state == State.MEASURE && lastEntry && now >= start + delta) {
                testState.startCoolDown();
                LOG.info("[Terminate] Waiting for all terminals to finish ..");
                measureEnd = now;
            } else if (state == State.EXIT) {
                // All threads have noticed the done, meaning all measured
                // requests have definitely finished.
                // Time to quit.
                break;
            }
        }

        try {
            int requests = finalizeWorkers(this.workerThreads);

            // Combine all the latencies together in the most disgusting way
            // possible: sorting!
            for (Worker w : workers) {
                for (LatencyRecord.Sample sample : w.getLatencyRecords()) {
                    samples.add(sample);
                }
            }
            Collections.sort(samples);

            // Compute stats on all the latencies
            int[] latencies = new int[samples.size()];
            for (int i = 0; i < samples.size(); ++i) {
                latencies[i] = samples.get(i).latencyUs;
            }
            DistributionStatistics stats = DistributionStatistics.computeStatistics(latencies);

            Results results = new Results(measureEnd - start, requests, stats, samples);

            // Compute transaction histogram
            Set<TransactionType> txnTypes = new HashSet<TransactionType>();
            for (WorkloadConfiguration workConf : workConfs) {
            	txnTypes.addAll(workConf.getTransTypes());
            }
            txnTypes.remove(TransactionType.INVALID);

            results.txnSuccess.putAll(txnTypes, 0);
            results.txnRetry.putAll(txnTypes, 0);
            results.txnAbort.putAll(txnTypes, 0);
            results.txnErrors.putAll(txnTypes, 0);
            

            for (Worker w : workers) {
                results.txnSuccess.putHistogram(w.getTransactionSuccessHistogram());
                results.txnRetry.putHistogram(w.getTransactionRetryHistogram());
                results.txnAbort.putHistogram(w.getTransactionAbortHistogram());
                results.txnErrors.putHistogram(w.getTransactionErrorHistogram());
                
                for (Entry<TransactionType, Histogram<String>> e : w.getTransactionAbortMessageHistogram().entrySet()) {
                    Histogram<String> h = results.txnAbortMessages.get(e.getKey());
                    if (h == null) {
                        h = new Histogram<String>(true);
                        results.txnAbortMessages.put(e.getKey(), h);
                    }
                    h.putHistogram(e.getValue());
                } // FOR
            } // FOR

            return (results);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private long getInterval(int lowestRate, Phase.Arrival arrival) {
        // TODO Auto-generated method stub
        if(arrival==Phase.Arrival.POISSON)
            return (long) ((-Math.log(1-Math.random())/lowestRate) * 1000000000.);
        else 
            return (long) (1000000000. / (double) lowestRate + 0.5);
    }

//    public Results runPoissonMultiPhase() throws QueueLimitException, IOException {
//        assert testState == null;
//        testState = new BenchmarkState(workers.size() + 1);
//        workStates = new ArrayList<WorkloadState>();
//        
//        for (WorkloadConfiguration workState : this.workConfs) {
//            workStates.add(workState.initializeState(testState));
//        }
//        
//        this.createWorkerThreads();
//        testState.blockForStart();
//
//        long start = System.nanoTime();
//        long measureEnd = -1;
//        
//        //used to determine the longest sleep interval
//        int lowestRate = Integer.MAX_VALUE;
//        
//        Phase phase = null;
//        
//        
//        for (WorkloadState workState : this.workStates) {
//            workState.switchToNextPhase();
//            phase = workState.getCurrentPhase();
//            LOG.info(phase.currentPhaseString());
//            if (phase.rate < lowestRate) {
//                lowestRate = phase.rate;
//            }
//        }
//        
//        long intervalNs = getInterval(lowestRate);
//        
//        long nextInterval = start + intervalNs;
//        int nextToAdd = 1;
//        int rateFactor;
//
//        boolean resetQueues = true;
//
//        long delta = phase.time * 1000000000L;
//        boolean lastEntry = false;
//        int submitted=0;
//        while (true) {
////            System.out.println(intervalNs);
//            // posting new work... and reseting the queue in case we have new
//            // portion of the workload...
//            submitted+=nextToAdd;
//            for (WorkloadState workState : this.workStates) {
//                if (workState.getCurrentPhase() != null) {
//                    rateFactor = workState.getCurrentPhase().rate / lowestRate;
//                } else {
//                    rateFactor = 1;
//                }
//                    workState.addToQueue(nextToAdd * rateFactor, resetQueues);
//            }
//            resetQueues = false;
//
//
//            // Wait until the interval expires, which may be "don't wait"
//            long now = System.nanoTime();
//            long diff = nextInterval - now;
//            while (diff > 0) { // this can wake early: sleep multiple times to
//                               // avoid that
//                long ms = diff / 1000000;
//                diff = diff % 1000000;
//                try {
//                    Thread.sleep(ms, (int) diff);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//                now = System.nanoTime();
//                diff = nextInterval - now;
//            }
//            assert diff <= 0;
//        
//            // End of Phase
//            if (start + delta < System.nanoTime() && !lastEntry) {
//                // enters here after each phase of the test
//                // reset the queues so that the new phase is not affected by the
//                // queue of the previous one
//                resetQueues = true;
//                
//                // Fetch a new Phase
//                synchronized (testState) {
//                    for (WorkloadState workState : workStates) {
//                        workState.switchToNextPhase();
//                        lowestRate = Integer.MAX_VALUE;
//                        phase = workState.getCurrentPhase();
//                        if (phase == null) {
//                            // Last phase
//                            lastEntry = true;
//                            break;
//                        } else {
//                            LOG.info(phase.currentPhaseString());
//                            if (phase.rate < lowestRate) {
//                                lowestRate = phase.rate;
//                            }
//                        }
//                    }
//                    if (phase != null) {
//                        // update frequency in which we check according to wakeup
//                        // speed
//                        // intervalNs = (long) (1000000000. / (double) lowestRate + 0.5);
//                        delta += phase.time * 1000000000L;
//                    }
//                }
//            }
//            
//            // Compute the next interval and how many messages to deliver
//            if(phase != null)
//            {
//                intervalNs=0;
//                nextToAdd = 0;
//                do
//                {
//                    intervalNs += getInterval(lowestRate);;
//                    nextToAdd ++;
//                } while ( (-diff) > intervalNs && !lastEntry);
//                nextInterval += intervalNs;
//                assert nextToAdd > 0;
//            }
//            // Update the test state appropriately
//            State state = testState.getState();
//            if (state == State.WARMUP && now >= start) {
//                testState.startMeasure();
//                start = now;
//                // measureEnd = measureStart + measureSeconds * 1000000000L;
//            } else if (state == State.MEASURE && lastEntry && now >= start + delta) {
//                System.out.println("### ToTal: "+ submitted);
//                testState.startCoolDown();
//                LOG.info("[Terminate] Waiting for all terminals to finish ..");
//                measureEnd = now;
//            } else if (state == State.EXIT) {
//                // All threads have noticed the done, meaning all measured
//                // requests have definitely finished.
//                // Time to quit.
//                break;
//            }
//        }
//        
//        try {
//            int requests = finalizeWorkers(this.workerThreads);
//
//            // Combine all the latencies together in the most disgusting way
//            // possible: sorting!
//            for (Worker w : workers) {
//                for (LatencyRecord.Sample sample : w.getLatencyRecords()) {
//                    samples.add(sample);
//                }
//            }
//            Collections.sort(samples);
//
//            // Compute stats on all the latencies
//            int[] latencies = new int[samples.size()];
//            for (int i = 0; i < samples.size(); ++i) {
//                latencies[i] = samples.get(i).latencyUs;
//            }
//            DistributionStatistics stats = DistributionStatistics.computeStatistics(latencies);
//
//            Results results = new Results(measureEnd - start, requests, stats, samples);
//
//            // Compute transaction histogram
//            Set<TransactionType> txnTypes = new HashSet<TransactionType>();
//            for (WorkloadConfiguration workConf : workConfs) {
//                txnTypes.addAll(workConf.getTransTypes());
//            }
//            txnTypes.remove(TransactionType.INVALID);
//
//            results.txnSuccess.putAll(txnTypes, 0);
//            results.txnRetry.putAll(txnTypes, 0);
//            results.txnAbort.putAll(txnTypes, 0);
//            results.txnErrors.putAll(txnTypes, 0);
//            
//
//            for (Worker w : workers) {
//                results.txnSuccess.putHistogram(w.getTransactionSuccessHistogram());
//                results.txnRetry.putHistogram(w.getTransactionRetryHistogram());
//                results.txnAbort.putHistogram(w.getTransactionAbortHistogram());
//                results.txnErrors.putHistogram(w.getTransactionErrorHistogram());
//                
//                for (Entry<TransactionType, Histogram<String>> e : w.getTransactionAbortMessageHistogram().entrySet()) {
//                    Histogram<String> h = results.txnAbortMessages.get(e.getKey());
//                    if (h == null) {
//                        h = new Histogram<String>(true);
//                        results.txnAbortMessages.put(e.getKey(), h);
//                    }
//                    h.putHistogram(e.getValue());
//                } // FOR
//            } // FOR
//
//            return (results);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        // Something bad happened! Tell all of our workers that the party is over!
//        synchronized (this) {
//            if (this.calledTearDown == false) {
//                for (Worker w : this.workers) {
//                    w.tearDown(true);
//                }
//            }
//            this.calledTearDown = true;
//        } // SYNCH
//            
        
        // HERE WE HANDLE THE CASE IN WHICH ONE OF OUR WOKERTHREADS DIED
        e.printStackTrace();
        System.exit(-1);

        /*
         * Alternatively, we could keep an HashMap<Thread,Worker> storing the
         * runnable for each thread, so that we can get the latency numbers from
         * a thread that died, and either continue or at least report current
         * status. (Remember to remove this thread from the list of threads to
         * wait for)
         */

    }

}
