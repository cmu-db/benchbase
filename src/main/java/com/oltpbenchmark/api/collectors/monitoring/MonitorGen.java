package com.oltpbenchmark.api.collectors.monitoring;

import java.util.List;

import com.oltpbenchmark.BenchmarkState;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Worker;

/**
 * Monitor generator that picks the appropriate monitoring implemnetation based
 * on the database type.
 */
public class MonitorGen {
    public static Monitor getMonitor(int interval, BenchmarkState testState,
            List<? extends Worker<? extends BenchmarkModule>> workers, WorkloadConfiguration conf) {
        switch (conf.getDatabaseType()) {
            case SQLSERVER:
                return new SQLServerMonitor(interval, testState, workers, conf);
            case POSTGRES:
                return new PostgreSQLMonitor(interval, testState, workers, conf);
            default:
                return new Monitor(interval, testState, workers);
        }
    }
}