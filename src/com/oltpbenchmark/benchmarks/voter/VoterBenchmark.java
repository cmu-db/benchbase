package com.oltpbenchmark.benchmarks.voter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.voter.procedures.Vote;

public class VoterBenchmark extends BenchmarkModule {

    public final int numContestants;
    
    public VoterBenchmark(WorkloadConfiguration workConf) {
        super("voter", workConf, true);
        numContestants = VoterUtil.getScaledNumContestants(workConf.getScaleFactor());
    }

    @Override
    protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
        List<Worker> workers = new ArrayList<Worker>();
        for (int i = 0; i < workConf.getTerminals(); ++i) {
            workers.add(new VoterWorker(this, i));
        }
        return workers;
    }

    @Override
    protected Loader makeLoaderImpl(Connection conn) throws SQLException {
        return new VoterLoader(this, conn);
    }

    @Override
    protected Package getProcedurePackageImpl() {
       return Vote.class.getPackage();
    }

}
