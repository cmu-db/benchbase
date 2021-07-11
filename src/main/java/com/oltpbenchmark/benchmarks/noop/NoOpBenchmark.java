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

package com.oltpbenchmark.benchmarks.noop;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.noop.procedures.NoOp;

import java.util.ArrayList;
import java.util.List;

/**
 * The NoOp Benchmark doesn't have any tables or execute any queries.
 * It's just how fast the DBMS can process NoOps
 *
 * @author pavlo
 * @author eric-haibin-lin
 */
public class NoOpBenchmark extends BenchmarkModule {

    public NoOpBenchmark(WorkloadConfiguration workConf) {
        super(workConf);
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        for (int i = 0; i < workConf.getTerminals(); ++i) {
            workers.add(new NoOpWorker(this, i));
        }
        return workers;
    }

    @Override
    protected Loader<NoOpBenchmark> makeLoaderImpl() {
        return new NoOpLoader(this);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return NoOp.class.getPackage();
    }

}
