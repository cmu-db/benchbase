package com.oltpbenchmark.api.collectors.monitoring;

import com.oltpbenchmark.BenchmarkState;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.util.MonitorInfo;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic monitoring class that reports the throughput of the executing workers while the benchmark
 * is being executed.
 */
public class Monitor extends Thread {
  protected static final Logger LOG = LoggerFactory.getLogger(DatabaseMonitor.class);

  protected final MonitorInfo monitorInfo;
  protected final BenchmarkState testState;
  protected final List<? extends Worker<? extends BenchmarkModule>> workers;

  {
    this.setDaemon(true);
  }

  /**
   * @param interval How long to wait between polling in milliseconds
   */
  Monitor(
      MonitorInfo monitorInfo,
      BenchmarkState testState,
      List<? extends Worker<? extends BenchmarkModule>> workers) {
    this.monitorInfo = monitorInfo;
    this.testState = testState;
    this.workers = workers;
  }

  @Override
  public void run() {
    int interval = this.monitorInfo.getMonitoringInterval();

    LOG.info("Starting MonitorThread Interval [{}ms]", interval);
    while (!Thread.currentThread().isInterrupted()) {
      // Compute the last throughput
      long measuredRequests = 0;
      synchronized (this.testState) {
        for (Worker<?> w : this.workers) {
          measuredRequests += w.getAndResetIntervalRequests();
        }
      }
      double seconds = interval / 1000d;
      double tps = (double) measuredRequests / seconds;
      LOG.info("Throughput: {} txn/sec", tps);

      try {
        Thread.sleep(interval);
      } catch (InterruptedException ex) {
        // Restore interrupt flag.
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Called at the end of the test to do any clean up that may be required. */
  public void tearDown() {
    // nothing to do here
  }
}
