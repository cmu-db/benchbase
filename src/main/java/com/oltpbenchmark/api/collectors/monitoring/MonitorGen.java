package com.oltpbenchmark.api.collectors.monitoring;

import com.oltpbenchmark.BenchmarkState;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.util.MonitorInfo;
import java.util.List;

/**
 * Monitor generator that picks the appropriate monitoring implemnetation based on the database
 * type.
 */
public class MonitorGen {
  public static Monitor getMonitor(
      MonitorInfo monitorInfo,
      BenchmarkState testState,
      List<? extends Worker<? extends BenchmarkModule>> workers,
      WorkloadConfiguration conf) {
    switch (monitorInfo.getMonitoringType()) {
      case ADVANCED:
        {
          switch (conf.getDatabaseType()) {
            case SQLSERVER:
              return new SQLServerMonitor(monitorInfo, testState, workers, conf);
            case POSTGRES:
              return new PostgreSQLMonitor(monitorInfo, testState, workers, conf);
            default:
              return new Monitor(monitorInfo, testState, workers);
          }
        }
      default:
        return new Monitor(monitorInfo, testState, workers);
    }
  }
}
