package com.oltpbenchmark.benchmarks.replay;

import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.replay.procedures.GenericQuery;

public class ReplayBenchmark extends BenchmarkModule {
    public ReplayBenchmark(WorkloadConfiguration workConf) {
        super(workConf);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return (GenericQuery.class.getPackage());
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        return new ArrayList<Worker<? extends BenchmarkModule>>();
    }

    @Override
    protected Loader<ReplayBenchmark> makeLoaderImpl() {
        throw new UnsupportedOperationException("Replay benchmarks do not support loading.");
    }
}
