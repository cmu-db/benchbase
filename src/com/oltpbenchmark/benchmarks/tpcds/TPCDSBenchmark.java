package com.oltpbenchmark.benchmarks.tpcds;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcds.procedures.Test;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TPCDSBenchmark extends BenchmarkModule {
    private static final Logger LOG = Logger.getLogger(TPCDSBenchmark.class);

    public TPCDSBenchmark(WorkloadConfiguration workConf) {
        super("tpcds", workConf, true);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return (Test.class.getPackage());
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException {
        LOG.debug(String.format("Initializing %d %s", this.workConf.getTerminals(), TPCDSWorker.class.getSimpleName()));

        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();
        try {
            for (int i = 0; i < this.workConf.getTerminals(); ++i) {
                TPCDSWorker worker = new TPCDSWorker(this, i);
                workers.add(worker);
            } // FOR
        } catch (Exception e) {
            e.printStackTrace();
        }
        return workers;
    }

    @Override
    protected Loader<TPCDSBenchmark> makeLoaderImpl() throws SQLException {
        return new TPCDSLoader(this);
    }
}
