/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.jpab;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.jpab.procedures.Persist;
import com.oltpbenchmark.benchmarks.jpab.tests.Test;

public class JPABBenchmark extends BenchmarkModule {

    private EntityManagerFactory emf;

    private JPABConfiguration jpabConf;

    public JPABBenchmark(WorkloadConfiguration workConf) {
        super("jpab", workConf, false);
        this.jpabConf = new JPABConfiguration(workConf);
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();
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
            JPABWorker worker = new JPABWorker(this, i , test);
            worker.em = emf.createEntityManager();
            workers.add(worker);
        }

        return workers;
    }

    @Override
    protected Loader<JPABBenchmark> makeLoaderImpl() throws SQLException {
        return new JPABLoader(this, jpabConf.getPersistanceUnit());
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return Persist.class.getPackage();
    }

}
