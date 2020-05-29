package com.oltpbenchmark.benchmarks.tpcds;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcds.procedures.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TPCDSBenchmark extends BenchmarkModule {
    private static final Logger LOG = LoggerFactory.getLogger(TPCDSBenchmark.class);

    public TPCDSBenchmark(WorkloadConfiguration workConf) {
        super(workConf, true);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return (Test.class.getPackage());
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() throws IOException {
        LOG.debug(String.format("Initializing %d %s", this.workConf.getTerminals(), TPCDSWorker.class.getSimpleName()));

        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        try {
            for (int i = 0; i < this.workConf.getTerminals(); ++i) {
                TPCDSWorker worker = new TPCDSWorker(this, i);
                workers.add(worker);
            } // FOR
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return workers;
    }

    @Override
    protected Loader<TPCDSBenchmark> makeLoaderImpl() throws SQLException {
        return new TPCDSLoader(this);
    }
}
