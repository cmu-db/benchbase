package com.oltpbenchmark.benchmarks.jpab;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.jpab.procedures.Persist;
import com.oltpbenchmark.benchmarks.jpab.tests.Test;

public class JPABBenchmark extends BenchmarkModule {
    private static final Logger LOG = Logger.getLogger(JPABBenchmark.class);

    private EntityManagerFactory emf;

    private JPABConfiguration jpabConf;

    public JPABBenchmark(WorkloadConfiguration workConf) {
        super("jpab", workConf, false);
        this.jpabConf = new JPABConfiguration(workConf);
    }

    @Override
    protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
        ArrayList<Worker> workers = new ArrayList<Worker>();
        emf = Persistence.createEntityManagerFactory(jpabConf.getPersistanceUnit());
        Test test = null;
        try {
            test = (Test)Class.forName("com.oltpbenchmark.benchmarks.jpab.tests."+this.jpabConf.getTestName()).newInstance();
            int totalObjectCount= (int) this.workConf.getScaleFactor();
            test.setBatchSize(1);
            test.setEntityCount(totalObjectCount);
            test.setEntityCount(totalObjectCount);
            test.buildInventory(totalObjectCount * 13 / 10);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        for (int i = 0; i < workConf.getTerminals(); ++i) {
            JPABWorker worker = new JPABWorker(i, this , test);
            worker.em = emf.createEntityManager();
            workers.add(worker);
        }

        return workers;
    }

    @Override
    protected Loader makeLoaderImpl(Connection conn) throws SQLException {
        return new JPABLoader(this, conn, jpabConf.getPersistanceUnit());
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return Persist.class.getPackage();
    }

}
