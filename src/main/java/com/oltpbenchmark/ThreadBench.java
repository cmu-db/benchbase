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
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.util.StringUtil;
import java.util.*;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadBench implements Thread.UncaughtExceptionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ThreadBench.class);

  private final BenchmarkState testState;
  private final List<? extends Worker<? extends BenchmarkModule>> workers;
  private final ArrayList<Thread> workerThreads;
  private final List<WorkloadConfiguration> workConfs;
  private final ArrayList<LatencyRecord.Sample> samples = new ArrayList<>();
  private final int intervalMonitor;

  private ThreadBench(
      List<? extends Worker<? extends BenchmarkModule>> workers,
      List<WorkloadConfiguration> workConfs,
      int intervalMonitoring) {
    this.workers = workers;
    this.workConfs = workConfs;
    this.workerThreads = new ArrayList<>(workers.size());
    this.intervalMonitor = intervalMonitoring;
    this.testState = new BenchmarkState(workers.size() + 1);
  }

  public static Results runRateLimitedBenchmark(
      List<Worker<? extends BenchmarkModule>> workers,
      List<WorkloadConfiguration> workConfs,
      int intervalMonitoring) {
    ThreadBench bench = new ThreadBench(workers, workConfs, intervalMonitoring);
    return bench.runRateLimitedMultiPhase();
  }

  private void createWorkerThreads() {

    for (Worker<?> worker : workers) {
      worker.initializeState();
      Thread thread = new Thread(worker);
      thread.setUncaughtExceptionHandler(this);
      thread.start();
      this.workerThreads.add(thread);
    }
  }

  private void interruptWorkers() {
    for (Worker<?> worker : workers) {
      worker.cancelStatement();
    }
  }

  private int finalizeWorkers(ArrayList<Thread> workerThreads) throws InterruptedException {

    int requests = 0;

    new WatchDogThread().start();

    for (int i = 0; i < workerThreads.size(); ++i) {

      // FIXME not sure this is the best solution... ensure we don't hang
      // forever, however we might ignore problems
      workerThreads.get(i).join(60000); // wait for 60second for threads
      // to terminate... hands otherwise

      /*
       * // CARLO: Maybe we might want to do this to kill threads that are
       * hanging... if (workerThreads.get(i).isAlive()) {
       * workerThreads.get(i).kill(); try { workerThreads.get(i).join(); }
       * catch (InterruptedException e) { } }
       */

      requests += workers.get(i).getRequests();

      LOG.debug("threadbench calling teardown");

      workers.get(i).tearDown();
    }

    return requests;
  }

  private Results runRateLimitedMultiPhase() {
    boolean errorsThrown = false;
    List<WorkloadState> workStates = new ArrayList<>();

    for (WorkloadConfiguration workState : this.workConfs) {
      workState.initializeState(testState);
      workStates.add(workState.getWorkloadState());
    }

    this.createWorkerThreads();

    // long measureStart = start;

    long startTs = System.currentTimeMillis();
    long start = System.nanoTime();
    long warmupStart = System.nanoTime();
    long warmup = warmupStart;
    long measureEnd = -1;
    // used to determine the longest sleep interval
    double lowestRate = Double.MAX_VALUE;

    Phase phase = null;

    for (WorkloadState workState : workStates) {
      workState.switchToNextPhase();
      phase = workState.getCurrentPhase();
      LOG.info(phase.currentPhaseString());
      if (phase.getRate() < lowestRate) {
        lowestRate = phase.getRate();
      }
    }

    // Change testState to cold query if execution is serial, since we don't
    // have a warm-up phase for serial execution but execute a cold and a
    // measured query in sequence.
    if (phase != null && phase.isLatencyRun()) {
      synchronized (testState) {
        testState.startColdQuery();
      }
    }

    long intervalNs = getInterval(lowestRate, phase.getArrival());

    long nextInterval = start + intervalNs;
    int nextToAdd = 1;
    int rateFactor;

    boolean resetQueues = true;

    long delta = phase.getTime() * 1000000000L;
    boolean lastEntry = false;

    // Initialize the Monitor
    if (this.intervalMonitor > 0) {
      new MonitorThread(this.intervalMonitor).start();
    }

    // Allow workers to start work.
    testState.blockForStart();

    // Main Loop
    while (true) {
      // posting new work... and resetting the queue in case we have new
      // portion of the workload...

      for (WorkloadState workState : workStates) {
        if (workState.getCurrentPhase() != null) {
          rateFactor = (int) (workState.getCurrentPhase().getRate() / lowestRate);
        } else {
          rateFactor = 1;
        }
        workState.addToQueue(nextToAdd * rateFactor, resetQueues);
      }
      resetQueues = false;

      // Wait until the interval expires, which may be "don't wait"
      long now = System.nanoTime();
      if (phase != null) {
        warmup = warmupStart + phase.getWarmupTime() * 1000000000L;
      }
      long diff = nextInterval - now;
      while (diff > 0) { // this can wake early: sleep multiple times to avoid that
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

      boolean phaseComplete = false;
      if (phase != null) {
        if (phase.isLatencyRun())
        // Latency runs (serial run through each query) have their own
        // state to mark completion
        {
          phaseComplete = testState.getState() == State.LATENCY_COMPLETE;
        } else {
          phaseComplete = testState.getState() == State.MEASURE && (start + delta <= now);
        }
      }

      // Go to next phase if this one is complete or enter if error was thrown
      boolean errorThrown = testState.getState() == State.ERROR;
      errorsThrown = errorsThrown || errorThrown;
      if ((phaseComplete || errorThrown) && !lastEntry) {
        // enters here after each phase of the test
        // reset the queues so that the new phase is not affected by the
        // queue of the previous one
        resetQueues = true;

        // Fetch a new Phase
        synchronized (testState) {
          if (phase.isLatencyRun()) {
            testState.ackLatencyComplete();
          }
          for (WorkloadState workState : workStates) {
            synchronized (workState) {
              workState.switchToNextPhase();
              lowestRate = Integer.MAX_VALUE;
              phase = workState.getCurrentPhase();
              interruptWorkers();
              if (phase == null && !lastEntry) {
                // Last phase
                lastEntry = true;
                testState.startCoolDown();
                measureEnd = now;
                LOG.info(
                    "{} :: Waiting for all terminals to finish ..", StringUtil.bold("TERMINATE"));
              } else if (phase != null) {
                // Reset serial execution parameters.
                if (phase.isLatencyRun()) {
                  phase.resetSerial();
                  testState.startColdQuery();
                }
                LOG.info(phase.currentPhaseString());
                if (phase.getRate() < lowestRate) {
                  lowestRate = phase.getRate();
                }
              }
            }
          }
          if (phase != null) {
            // update frequency in which we check according to
            // wakeup
            // speed
            // intervalNs = (long) (1000000000. / (double)
            // lowestRate + 0.5);
            delta += phase.getTime() * 1000000000L;
          }
        }
      }

      // Compute the next interval
      // and how many messages to deliver
      if (phase != null) {
        intervalNs = 0;
        nextToAdd = 0;
        do {
          intervalNs += getInterval(lowestRate, phase.getArrival());
          nextToAdd++;
        } while ((-diff) > intervalNs && !lastEntry);
        nextInterval += intervalNs;
      }

      // Update the test state appropriately
      State state = testState.getState();
      if (state == State.WARMUP && now >= warmup) {
        synchronized (testState) {
          if (phase != null && phase.isLatencyRun()) {
            testState.startColdQuery();
          } else {
            testState.startMeasure();
          }
          interruptWorkers();
        }
        start = now;
        LOG.info("{} :: Warmup complete, starting measurements.", StringUtil.bold("MEASURE"));
        // measureEnd = measureStart + measureSeconds * 1000000000L;

        // For serial executions, we want to do every query exactly
        // once, so we need to restart in case some of the queries
        // began during the warmup phase.
        // If we're not doing serial executions, this function has no
        // effect and is thus safe to call regardless.
        phase.resetSerial();
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
      for (Worker<?> w : workers) {
        for (LatencyRecord.Sample sample : w.getLatencyRecords()) {
          samples.add(sample);
        }
      }
      Collections.sort(samples);

      // Compute stats on all the latencies
      int[] latencies = new int[samples.size()];
      for (int i = 0; i < samples.size(); ++i) {
        latencies[i] = samples.get(i).getLatencyMicrosecond();
      }
      DistributionStatistics stats = DistributionStatistics.computeStatistics(latencies);

      Results results =
          new Results(
              // If any errors were thrown during the execution, proprogate that fact to the
              // final Results state so we can exit non-zero *after* we output the results.
              errorsThrown ? State.ERROR : testState.getState(),
              startTs,
              measureEnd - start,
              requests,
              stats,
              samples);

      // Compute transaction histogram
      Set<TransactionType> txnTypes = new HashSet<>();
      for (WorkloadConfiguration workConf : workConfs) {
        txnTypes.addAll(workConf.getTransTypes());
      }
      txnTypes.remove(TransactionType.INVALID);

      results.getUnknown().putAll(txnTypes, 0);
      results.getSuccess().putAll(txnTypes, 0);
      results.getRetry().putAll(txnTypes, 0);
      results.getAbort().putAll(txnTypes, 0);
      results.getError().putAll(txnTypes, 0);
      results.getRetryDifferent().putAll(txnTypes, 0);

      for (Worker<?> w : workers) {
        results.getUnknown().putHistogram(w.getTransactionUnknownHistogram());
        results.getSuccess().putHistogram(w.getTransactionSuccessHistogram());
        results.getRetry().putHistogram(w.getTransactionRetryHistogram());
        results.getAbort().putHistogram(w.getTransactionAbortHistogram());
        results.getError().putHistogram(w.getTransactionErrorHistogram());
        results.getRetryDifferent().putHistogram(w.getTransactionRetryDifferentHistogram());
      }

      return (results);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private long getInterval(double lowestRate, Phase.Arrival arrival) {
    // TODO Auto-generated method stub
    if (arrival == Phase.Arrival.POISSON) {
      return (long) ((-Math.log(1 - Math.random()) / lowestRate) * 1000000000.);
    } else {
      return (long) (1000000000. / lowestRate + 0.5);
    }
  }

  @Override
  public void uncaughtException(Thread t, Throwable e) {
    // Here we handle the case in which one of our worker threads died
    LOG.error(e.getMessage(), e);
    // We do not continue with the experiment. Instead, bypass rest of
    // phases that were left in the test and signal error state.
    // The rest of the workflow to finish the experiment remains the same,
    // and partial metrics will be reported (i.e., until failure happened).
    synchronized (testState) {
      for (WorkloadConfiguration workConf : this.workConfs) {
        synchronized (workConf.getWorkloadState()) {
          WorkloadState workState = workConf.getWorkloadState();
          Phase phase = workState.getCurrentPhase();
          while (phase != null) {
            workState.switchToNextPhase();
            phase = workState.getCurrentPhase();
          }
        }
      }
      testState.signalError();
    }
  }

  public static final class TimeBucketIterable implements Iterable<DistributionStatistics> {
    private final Iterable<Sample> samples;
    private final int windowSizeSeconds;
    private final TransactionType transactionType;

    /**
     * @param samples
     * @param windowSizeSeconds
     * @param transactionType Allows to filter transactions by type
     */
    public TimeBucketIterable(
        Iterable<Sample> samples, int windowSizeSeconds, TransactionType transactionType) {
      this.samples = samples;
      this.windowSizeSeconds = windowSizeSeconds;
      this.transactionType = transactionType;
    }

    @Override
    public Iterator<DistributionStatistics> iterator() {
      return new TimeBucketIterator(samples.iterator(), windowSizeSeconds, transactionType);
    }
  }

  private static final class TimeBucketIterator implements Iterator<DistributionStatistics> {
    private final Iterator<Sample> samples;
    private final int windowSizeSeconds;
    private final TransactionType txType;

    private Sample sample;
    private long nextStartNanosecond;

    private DistributionStatistics next;

    /**
     * @param samples
     * @param windowSizeSeconds
     * @param txType Allows to filter transactions by type
     */
    public TimeBucketIterator(
        Iterator<LatencyRecord.Sample> samples, int windowSizeSeconds, TransactionType txType) {
      this.samples = samples;
      this.windowSizeSeconds = windowSizeSeconds;
      this.txType = txType;

      if (samples.hasNext()) {
        sample = samples.next();
        // TODO: To be totally correct, we would want this to be the
        // timestamp of the start
        // of the measurement interval. In most cases this won't matter.
        nextStartNanosecond = sample.getStartNanosecond();
        calculateNext();
      }
    }

    private void calculateNext() {

      // Collect all samples in the time window
      ArrayList<Integer> latencies = new ArrayList<>();
      long endNanoseconds = nextStartNanosecond + (windowSizeSeconds * 1000000000L);
      while (sample != null && sample.getStartNanosecond() < endNanoseconds) {

        // Check if a TX Type filter is set, in the default case,
        // INVALID TXType means all should be reported, if a filter is
        // set, only this specific transaction
        if (txType.equals(TransactionType.INVALID)
            || txType.getId() == sample.getTransactionType()) {
          latencies.add(sample.getLatencyMicrosecond());
        }

        if (samples.hasNext()) {
          sample = samples.next();
        } else {
          sample = null;
        }
      }

      // Set up the next time window

      nextStartNanosecond = endNanoseconds;

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
      if (next == null) {
        throw new NoSuchElementException();
      }
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

  private class WatchDogThread extends Thread {
    {
      this.setDaemon(true);
    }

    @Override
    public void run() {
      Map<String, Object> m = new ListOrderedMap<>();
      LOG.info("Starting WatchDogThread");
      while (true) {
        try {
          Thread.sleep(20000);
        } catch (InterruptedException ex) {
          return;
        }

        m.clear();
        for (Thread t : workerThreads) {
          m.put(t.getName(), t.isAlive());
        }
        LOG.info("Worker Thread Status:\n{}", StringUtil.formatMaps(m));
      }
    }
  }

  private class MonitorThread extends Thread {
    private final int intervalMonitor;

    {
      this.setDaemon(true);
    }

    /**
     * @param interval How long to wait between polling in milliseconds
     */
    MonitorThread(int interval) {
      this.intervalMonitor = interval;
    }

    @Override
    public void run() {
      LOG.info("Starting MonitorThread Interval [{}ms]", this.intervalMonitor);
      while (true) {
        try {
          Thread.sleep(this.intervalMonitor);
        } catch (InterruptedException ex) {
          return;
        }

        // Compute the last throughput
        long measuredRequests = 0;
        synchronized (testState) {
          for (Worker<?> w : workers) {
            measuredRequests += w.getAndResetIntervalRequests();
          }
        }
        double seconds = this.intervalMonitor / 1000d;
        double tps = (double) measuredRequests / seconds;
        LOG.info("Throughput: {} txn/sec", tps);
      }
    }
  }
}
